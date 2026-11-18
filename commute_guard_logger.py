import csv
import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.transit import gtfs_realtime_pb2


def load_env(path: Path = Path(".env")):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


load_env()
NTA_API_KEY = os.environ.get("NTA_API_KEY", "")
VEHICLE_POSITIONS_URL = "https://api.nationaltransport.ie/gtfsr/v2/Vehicles"
TRIP_UPDATES_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
IRISH_RAIL_URL = (
    "http://api.irishrail.ie/realtime/realtime.asmx/"
    "getStationDataByNameXML?StationDesc=Maynooth"
)

LOOKUP_FILE = Path("data/maynooth_lookup.json")
EVENTS_CSV = Path("data/baseline_events.csv")
TRANSFERS_CSV = Path("data/transfer_alerts.csv")

STATION_BUS_STOP_IDS = {"8260DB003917", "8260DB007779", "8260B1354601"}
WALK_BUFFER_MINS = 3
MIN_SAFE_TRANSFER_MINS = 5
MAX_VALID_DELAY_MINS = 45
POLL_INTERVAL_SECONDS = 60
TZ = ZoneInfo("Europe/Dublin")


def build_http_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=2,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


HTTP = build_http_session()


def fetch_gtfs_rt(url: str) -> gtfs_realtime_pb2.FeedMessage:
    headers = {"x-api-key": NTA_API_KEY, "Cache-Control": "no-cache"}
    resp = HTTP.get(url, headers=headers, timeout=(10, 25))
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


def parse_gtfs_time(service_date: datetime, time_str: str) -> datetime | None:
    if not time_str:
        return None
    parts = time_str.strip().split(":")
    if len(parts) != 3:
        return None
    hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
    base = datetime(service_date.year, service_date.month, service_date.day, tzinfo=TZ)
    return base + timedelta(hours=hours, minutes=minutes, seconds=seconds)


def get_active_service_ids(lookup: dict, service_date: datetime) -> set[str]:
    date_str = service_date.strftime("%Y%m%d")
    weekday_idx = service_date.weekday()
    active = set()

    for svid, rule in lookup.get("calendar", {}).items():
        if rule["start_date"] <= date_str <= rule["end_date"]:
            if rule["days"][weekday_idx] == 1:
                active.add(svid)

    for svid, exceptions in lookup.get("calendar_dates", {}).items():
        for exc in exceptions:
            if exc["date"] == date_str:
                if exc["exception_type"] == "1":
                    active.add(svid)
                elif exc["exception_type"] == "2":
                    active.discard(svid)
    return active


def load_existing_keys() -> tuple[set, set]:
    seen_events = set()
    if EVENTS_CSV.exists():
        with EVENTS_CSV.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                seen_events.add((row["service_date"], row["trip_id"], row["alert_type"]))

    seen_transfers = set()
    if TRANSFERS_CSV.exists():
        with TRANSFERS_CSV.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                seen_transfers.add((row["logged_at"][:10], row["bus_trip_id"], row["train_code"]))
    return seen_events, seen_transfers


def fetch_irish_rail_departures(now: datetime) -> list[dict]:
    resp = HTTP.get(IRISH_RAIL_URL, timeout=(10, 20))
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    ns = {"ir": "http://api.irishrail.ie/realtime/"}
    departures = []

    for tr in root.findall("ir:objStationData", ns):
        loc_type = tr.findtext("ir:LocationType", default="", namespaces=ns).strip()
        exp_depart = tr.findtext("ir:Expdepart", default="00:00", namespaces=ns).strip()
        if loc_type == "D" or exp_depart == "00:00":
            continue

        dest = tr.findtext("ir:Destination", default="Unknown", namespaces=ns).strip()
        train_code = tr.findtext("ir:Traincode", default="", namespaces=ns).strip()
        late_mins = int(tr.findtext("ir:Late", default="0", namespaces=ns) or 0)
        due_in = int(tr.findtext("ir:Duein", default="0", namespaces=ns) or 0)

        hh, mm = map(int, exp_depart.split(":"))
        dep_dt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if dep_dt < now - timedelta(hours=2):
            dep_dt += timedelta(days=1)

        departures.append({
            "train_code": train_code,
            "destination": dest,
            "exp_depart_str": exp_depart,
            "exp_depart_dt": dep_dt,
            "due_in": due_in,
            "late_mins": late_mins,
        })
    return departures


def append_csv(path: Path, fieldnames: list[str], row: dict):
    file_exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def run_cycle(lookup: dict, seen_events: set, seen_transfers: set):
    now = datetime.now(TZ)
    today_services = get_active_service_ids(lookup, now)
    yesterday = now - timedelta(days=1)
    yesterday_services = get_active_service_ids(lookup, yesterday)

    vp_feed = fetch_gtfs_rt(VEHICLE_POSITIONS_URL)
    live_gps_trips = {}
    live_gps_fallback = {}
    for entity in vp_feed.entity:
        if not entity.HasField("vehicle"):
            continue
        t = entity.vehicle.trip
        vid = entity.vehicle.vehicle.id
        if t.trip_id:
            live_gps_trips[t.trip_id] = vid
        if t.route_id and t.start_time:
            live_gps_fallback[(t.route_id, t.start_time)] = vid

    tu_feed = fetch_gtfs_rt(TRIP_UPDATES_URL)
    trip_updates = {}
    trip_updates_fallback = {}
    for entity in tu_feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        t = tu.trip
        rel = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
            t.schedule_relationship
        )
        default_delay = 0.0
        stop_delays = {}
        for stu in tu.stop_time_update:
            d_sec = None
            if stu.HasField("arrival") and stu.arrival.HasField("delay"):
                d_sec = stu.arrival.delay
            elif stu.HasField("departure") and stu.departure.HasField("delay"):
                d_sec = stu.departure.delay
            if d_sec is not None:
                d_min = round(d_sec / 60, 1)
                default_delay = d_min
                if stu.stop_id:
                    stop_delays[stu.stop_id] = d_min

        payload = {
            "status": rel,
            "default_delay": default_delay,
            "stop_delays": stop_delays,
        }
        if t.trip_id:
            trip_updates[t.trip_id] = payload
        if t.route_id and t.start_time:
            trip_updates_fallback[(t.route_id, t.start_time)] = payload

    active_window_trips = {}
    for item in lookup["maynooth_stop_times"]:
        svid = item["service_id"]
        if svid in today_services:
            s_date = now
        elif now.hour < 4 and svid in yesterday_services:
            s_date = yesterday
        else:
            continue

        origin_dt = parse_gtfs_time(s_date, item["origin_departure_time"])
        arr_dt = parse_gtfs_time(s_date, item["arrival_time"])
        if not origin_dt or not arr_dt:
            continue

        tid = item["trip_id"]
        if tid not in active_window_trips or arr_dt < active_window_trips[tid]["arr_dt"]:
            active_window_trips[tid] = {
                **item,
                "service_date": s_date.strftime("%Y-%m-%d"),
                "origin_dt": origin_dt,
                "arr_dt": arr_dt,
            }

    ghost_count = 0
    delay_count = 0
    station_arrivals = []

    for tid, info in active_window_trips.items():
        mins_since_origin = (now - info["origin_dt"]).total_seconds() / 60.0
        mins_until_stop = (info["arr_dt"] - now).total_seconds() / 60.0
        fb_key = (info["route_id"], info["origin_departure_time"])

        tu_info = trip_updates.get(tid) or trip_updates_fallback.get(fb_key)
        has_gps = (tid in live_gps_trips) or (fb_key in live_gps_fallback)
        if tu_info:
            delay_mins = tu_info["stop_delays"].get(info["stop_id"], tu_info["default_delay"])
        else:
            delay_mins = 0.0

        pred_arr_dt = info["arr_dt"] + timedelta(minutes=delay_mins)
        is_zombie_delay = delay_mins > MAX_VALID_DELAY_MINS

        if (
            not is_zombie_delay
            and info["stop_id"] in STATION_BUS_STOP_IDS
            and info.get("stop_role") != "ORIGIN"
            and -2 <= (pred_arr_dt - now).total_seconds() / 60.0 <= 45
        ):
            station_arrivals.append({**info, "delay_mins": delay_mins, "pred_arr_dt": pred_arr_dt})

        if mins_since_origin < 10 or mins_until_stop < -10:
            continue

        alert_type = None
        if tu_info and tu_info["status"] == "CANCELED":
            alert_type = "OFFICIAL_CANCELED"
        elif is_zombie_delay:
            alert_type = "STALLED_GHOST_RISK"
        elif not has_gps and tu_info is None:
            alert_type = "SILENT_GHOST_RISK"
        elif delay_mins >= 10:
            alert_type = "HEAVY_DELAY"

        if alert_type:
            if alert_type in ("OFFICIAL_CANCELED", "SILENT_GHOST_RISK", "STALLED_GHOST_RISK"):
                ghost_count += 1
            else:
                delay_count += 1

            event_key = (info["service_date"], tid, alert_type)
            if event_key not in seen_events:
                seen_events.add(event_key)
                append_csv(
                    EVENTS_CSV,
                    [
                        "logged_at", "service_date", "route", "operator", "headsign",
                        "trip_id", "alert_type", "origin_dep", "sched_maynooth_arr",
                        "delay_mins", "has_gps", "stop_name", "stop_role"
                    ],
                    {
                        "logged_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                        "service_date": info["service_date"],
                        "route": info["route_short_name"],
                        "operator": info.get("agency_name", ""),
                        "headsign": info["headsign"],
                        "trip_id": tid,
                        "alert_type": alert_type,
                        "origin_dep": info["origin_departure_time"],
                        "sched_maynooth_arr": info["arrival_time"],
                        "delay_mins": delay_mins,
                        "has_gps": has_gps,
                        "stop_name": info["stop_name"],
                        "stop_role": info.get("stop_role", ""),
                    },
                )

    trains = fetch_irish_rail_departures(now)
    transfer_risks = 0
    for bus in station_arrivals:
        if bus["delay_mins"] < 2:
            continue
        for tr in trains:
            sched_buffer = (tr["exp_depart_dt"] - bus["arr_dt"]).total_seconds() / 60.0 - WALK_BUFFER_MINS
            live_buffer = (tr["exp_depart_dt"] - bus["pred_arr_dt"]).total_seconds() / 60.0 - WALK_BUFFER_MINS

            if sched_buffer >= 3 and -5 <= live_buffer < MIN_SAFE_TRANSFER_MINS:
                transfer_risks += 1
                t_key = (bus["service_date"], bus["trip_id"], tr["train_code"])
                if t_key not in seen_transfers:
                    seen_transfers.add(t_key)
                    append_csv(
                        TRANSFERS_CSV,
                        [
                            "logged_at", "route", "bus_trip_id", "bus_sched_arr",
                            "bus_delay_mins", "train_dest", "train_code",
                            "train_exp_dep", "live_buffer_mins"
                        ],
                        {
                            "logged_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                            "route": bus["route_short_name"],
                            "bus_trip_id": bus["trip_id"],
                            "bus_sched_arr": bus["arrival_time"],
                            "bus_delay_mins": bus["delay_mins"],
                            "train_dest": tr["destination"],
                            "train_code": tr["train_code"],
                            "train_exp_dep": tr["exp_depart_str"],
                            "live_buffer_mins": round(live_buffer, 1),
                        },
                    )

    print(
        f"[{now.strftime('%H:%M:%S')}] Active GPS Buses: {len(live_gps_trips):>3} | "
        f"Ghost/Cancel Flags: {ghost_count:>2} | Heavy Delays (10m+): {delay_count:>2} | "
        f"Train Transfer Risks: {transfer_risks:>2}"
    )


def main():
    if not NTA_API_KEY:
        raise RuntimeError("Missing NTA_API_KEY in .env file.")

    with LOOKUP_FILE.open("r", encoding="utf-8") as f:
        lookup = json.load(f)

    print(f"Loaded Maynooth lookup (GTFS valid through {lookup.get('feed_end_date', 'unknown')}).")
    print("Starting Commute Reliability & Connection Guard Logger (Ctrl+C to stop)...")
    print(f"Logging events to {EVENTS_CSV} and {TRANSFERS_CSV}\n")
    seen_events, seen_transfers = load_existing_keys()

    try:
        while True:
            try:
                run_cycle(lookup, seen_events, seen_transfers)
            except Exception as exc:
                print(f"[{datetime.now(TZ).strftime('%H:%M:%S')}] Warning: {exc}")
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nLogger stopped cleanly. All captured events are saved in data/.")


if __name__ == "__main__":
    main()
