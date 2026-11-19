import gc
﻿import csv
import json
import os
import threading
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, render_template_string, request, send_file
from google.transit import gtfs_realtime_pb2
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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
GIST_ID = os.environ.get("GIST_ID", "").strip()
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()

VEHICLE_POSITIONS_URL = "https://api.nationaltransport.ie/gtfsr/v2/Vehicles"
TRIP_UPDATES_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
IRISH_RAIL_URL = (
    "http://api.irishrail.ie/realtime/realtime.asmx/"
    "getStationDataByNameXML?StationDesc=Maynooth"
)

LOOKUP_FILE = Path("data/maynooth_lookup.json")
EVENTS_CSV = Path("data/baseline_events.csv")
TRANSFERS_CSV = Path("data/transfer_alerts.csv")
CROWD_CSV = Path("data/crowd_reports.csv")

STATION_BUS_STOP_IDS = {"8260DB003917", "8260DB007779", "8260B1354601"}
CAMPUS_STOP_IDS = {"8260B1031601", "8260B1034301", "8260PB10049"}
WALK_BUFFER_MINS = 3
MIN_SAFE_TRANSFER_MINS = 5
MAX_VALID_DELAY_MINS = 45
CACHE_TTL_SECONDS = 45
TZ = ZoneInfo("Europe/Dublin")

app = Flask(__name__)

with LOOKUP_FILE.open("r", encoding="utf-8") as f:
    LOOKUP = json.load(f)


def build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=1,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


HTTP = build_http_session()
CACHE_LOCK = threading.Lock()
CSV_LOCK = threading.Lock()
CACHE = {"timestamp": 0.0, "payload": None, "cooldown_until": 0.0}
LAST_GIST_SYNC = 0.0

CROWD_TALLY: dict[str, dict[str, int]] = {}
SEEN_EVENTS: set[tuple[str, str, str]] = set()
SEEN_TRANSFERS: set[tuple[str, str, str]] = set()


def sync_from_gist():
    if not (GIST_ID and GITHUB_TOKEN):
        return
    try:
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
        }
        resp = HTTP.get(f"https://api.github.com/gists/{GIST_ID}", headers=headers, timeout=12)
        if not resp.ok:
            return
        files = resp.json().get("files", {})
        for fname in ("baseline_events.csv", "transfer_alerts.csv", "crowd_reports.csv"):
            remote_content = files.get(fname, {}).get("content", "")
            if not remote_content.strip():
                continue
            local_path = Path("data") / fname
            local_lines = len(local_path.read_text(encoding="utf-8").splitlines()) if local_path.exists() else 0
            remote_lines = len(remote_content.splitlines())
            if remote_lines > local_lines:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.write_text(remote_content, encoding="utf-8")
                print(f"Restored {fname} from Gist ({remote_lines} lines).")
    except Exception as exc:
        print(f"Gist restore note: {exc}")


def sync_to_gist(force: bool = False):
    global LAST_GIST_SYNC
    if not (GIST_ID and GITHUB_TOKEN):
        return
    now_ts = time.time()
    if not force and (now_ts - LAST_GIST_SYNC < 120):
        return
    try:
        payload_files = {}
        for fname in ("baseline_events.csv", "transfer_alerts.csv", "crowd_reports.csv"):
            fpath = Path("data") / fname
            if fpath.exists() and fpath.stat().st_size > 0:
                payload_files[fname] = {"content": fpath.read_text(encoding="utf-8")}
        if not payload_files:
            return
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
        }
        resp = HTTP.patch(
            f"https://api.github.com/gists/{GIST_ID}",
            headers=headers,
            json={"files": payload_files},
            timeout=12,
        )
        if resp.ok:
            LAST_GIST_SYNC = now_ts
    except Exception as exc:
        print(f"Gist backup note: {exc}")


def load_initial_state():
    if CROWD_CSV.exists():
        with CROWD_CSV.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                key = f"{row.get('service_date', '')}|{row.get('trip_id', '')}"
                CROWD_TALLY.setdefault(key, {"ARRIVED": 0, "GHOST": 0})
                verdict = row.get("verdict", "")
                if verdict in CROWD_TALLY[key]:
                    CROWD_TALLY[key][verdict] += 1

    if EVENTS_CSV.exists():
        with EVENTS_CSV.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                SEEN_EVENTS.add((
                    row.get("service_date", ""),
                    row.get("trip_id", ""),
                    row.get("alert_type", ""),
                ))

    if TRANSFERS_CSV.exists():
        with TRANSFERS_CSV.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                SEEN_TRANSFERS.add((
                    row.get("logged_at", "")[:10],
                    row.get("bus_trip_id", ""),
                    row.get("train_code", ""),
                ))


sync_from_gist()
load_initial_state()


def append_csv_safe(path: Path, default_fieldnames: list[str], row: dict):
    with CSV_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = path.exists() and path.stat().st_size > 0
        if file_exists:
            with path.open("r", encoding="utf-8", newline="") as f:
                existing_headers = next(csv.reader(f), None)
            fieldnames = existing_headers if existing_headers else default_fieldnames
        else:
            fieldnames = default_fieldnames

        filtered_row = {k: row.get(k, "") for k in fieldnames}
        with path.open("a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(filtered_row)


def compute_phase2_analytics() -> dict:
    route_stats = defaultdict(lambda: {"ghosts": 0, "canceled": 0, "delays": 0, "total": 0})
    totals = {
        "total_events": 0,
        "silent_ghosts": 0,
        "official_canceled": 0,
        "stalled_ghosts": 0,
        "heavy_delays": 0,
        "transfer_risks": 0,
        "crowd_total": 0,
        "crowd_confirmed_ghosts": 0,
        "crowd_gps_faults": 0,
        "crowd_verified_arrived": 0,
    }

    if EVENTS_CSV.exists():
        with EVENTS_CSV.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                rt = row.get("route", "Unknown")
                at = row.get("alert_type", "")
                totals["total_events"] += 1
                route_stats[rt]["total"] += 1

                if at == "SILENT_GHOST_RISK":
                    totals["silent_ghosts"] += 1
                    route_stats[rt]["ghosts"] += 1
                elif at == "OFFICIAL_CANCELED":
                    totals["official_canceled"] += 1
                    route_stats[rt]["canceled"] += 1
                elif at == "STALLED_GHOST_RISK":
                    totals["stalled_ghosts"] += 1
                    route_stats[rt]["ghosts"] += 1
                elif at == "HEAVY_DELAY":
                    totals["heavy_delays"] += 1
                    route_stats[rt]["delays"] += 1

    if TRANSFERS_CSV.exists():
        with TRANSFERS_CSV.open("r", encoding="utf-8", newline="") as f:
            for _ in csv.DictReader(f):
                totals["transfer_risks"] += 1

    if CROWD_CSV.exists():
        with CROWD_CSV.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                verdict = row.get("verdict", "")
                api_st = row.get("api_status", "")
                totals["crowd_total"] += 1
                if verdict == "GHOST":
                    totals["crowd_confirmed_ghosts"] += 1
                elif verdict == "ARRIVED":
                    if api_st in ("SILENT_GHOST_RISK", "SCHEDULED", "STALLED_GHOST"):
                        totals["crowd_gps_faults"] += 1
                    else:
                        totals["crowd_verified_arrived"] += 1

    ranked_routes = sorted(
        [
            {
                "route": rt,
                "ghosts": st["ghosts"] + st["canceled"],
                "delays": st["delays"],
                "total": st["total"],
            }
            for rt, st in route_stats.items()
        ],
        key=lambda x: x["total"],
        reverse=True,
    )

    return {
        "totals": totals,
        "routes": ranked_routes,
        "route_map": dict(route_stats),
    }


def parse_gtfs_time(service_date: datetime, time_str: str) -> datetime | None:
    if not time_str:
        return None
    parts = time_str.strip().split(":")
    if len(parts) != 3:
        return None
    hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
    base = datetime(service_date.year, service_date.month, service_date.day, tzinfo=TZ)
    return base + timedelta(hours=hours, minutes=minutes, seconds=seconds)


def get_active_service_ids(service_date: datetime) -> set[str]:
    date_str = service_date.strftime("%Y%m%d")
    weekday_idx = service_date.weekday()
    active = set()

    for svid, rule in LOOKUP.get("calendar", {}).items():
        if rule["start_date"] <= date_str <= rule["end_date"]:
            if rule["days"][weekday_idx] == 1:
                active.add(svid)

    for svid, exceptions in LOOKUP.get("calendar_dates", {}).items():
        for exc in exceptions:
            if exc["date"] == date_str:
                if exc["exception_type"] == "1":
                    active.add(svid)
                elif exc["exception_type"] == "2":
                    active.discard(svid)
    return active


def fetch_gtfs_rt(url: str) -> gtfs_realtime_pb2.FeedMessage:
    headers = {"x-api-key": NTA_API_KEY, "Cache-Control": "no-cache"}
    resp = HTTP.get(url, headers=headers, timeout=(8, 20))
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


def fetch_trains(now: datetime) -> list[dict]:
    resp = HTTP.get(IRISH_RAIL_URL, timeout=(8, 15))
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    ns = {"ir": "http://api.irishrail.ie/realtime/"}
    trains = []

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

        trains.append({
            "train_code": train_code,
            "destination": dest,
            "exp_depart": exp_depart,
            "exp_depart_dt": dep_dt,
            "due_in": due_in,
            "late_mins": late_mins,
        })

    trains.sort(key=lambda x: x["due_in"])
    return trains


def get_live_snapshot(force_refresh: bool = False) -> dict:
    with CACHE_LOCK:
        now_ts = time.time()
        if not force_refresh and CACHE["payload"] and (now_ts - CACHE["timestamp"] < CACHE_TTL_SECONDS):
            return CACHE["payload"]

        if now_ts < CACHE["cooldown_until"]:
            wait_secs = int(CACHE["cooldown_until"] - now_ts)
            if CACHE["payload"]:
                stale = dict(CACHE["payload"])
                stale["warning"] = f"NTA rate-limit cooldown ({wait_secs}s left) — showing cached snapshot"
                return stale
            return {
                "updated_at": datetime.now(TZ).strftime("%H:%M:%S"),
                "warning": f"NTA API cooling down after rate limit — retrying in {wait_secs}s",
                "trains": [],
                "buses": [],
                "analytics": compute_phase2_analytics(),
                "stats": {"active_gps_national": 0, "ghost_or_cancel": 0, "transfer_risks": 0},
            }

        now = datetime.now(TZ)
        try:
            vp_feed = fetch_gtfs_rt(VEHICLE_POSITIONS_URL)
            tu_feed = fetch_gtfs_rt(TRIP_UPDATES_URL)
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else 0
            retry_after = 65
            if status_code == 429 and exc.response is not None:
                raw_ra = exc.response.headers.get("Retry-After", "")
                if raw_ra.isdigit():
                    retry_after = max(30, int(raw_ra) + 5)
            CACHE["cooldown_until"] = time.time() + retry_after
            if CACHE["payload"]:
                stale = dict(CACHE["payload"])
                stale["warning"] = f"NTA rate limit (HTTP {status_code}) — paused for {retry_after}s"
                return stale
            return {
                "updated_at": now.strftime("%H:%M:%S"),
                "warning": f"NTA rate limit (HTTP {status_code}) — cooling down for {retry_after}s",
                "trains": [],
                "buses": [],
                "analytics": compute_phase2_analytics(),
                "stats": {"active_gps_national": 0, "ghost_or_cancel": 0, "transfer_risks": 0},
            }

        today_services = get_active_service_ids(now)
        yesterday = now - timedelta(days=1)
        yesterday_services = get_active_service_ids(yesterday)

        live_gps_trips = set()
        live_gps_fallback = set()
        for entity in vp_feed.entity:
            if not entity.HasField("vehicle"):
                continue
            t = entity.vehicle.trip
            if t.trip_id:
                live_gps_trips.add(t.trip_id)
            if t.route_id and t.start_time:
                live_gps_fallback.add((t.route_id, t.start_time))

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

            payload = {"status": rel, "default_delay": default_delay, "stop_delays": stop_delays}
            if t.trip_id:
                trip_updates[t.trip_id] = payload
            if t.route_id and t.start_time:
                trip_updates_fallback[(t.route_id, t.start_time)] = payload

        try:
            trains = fetch_trains(now)
        except Exception:
            trains = []

        # 1. Background Phase 2 Logging
        active_window_trips = {}
        for item in LOOKUP["maynooth_stop_times"]:
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

        for tid, info in active_window_trips.items():
            mins_since_origin = (now - info["origin_dt"]).total_seconds() / 60.0
            mins_until_stop = (info["arr_dt"] - now).total_seconds() / 60.0
            fb_key = (info["route_id"], info["origin_departure_time"])

            tu_info = trip_updates.get(tid) or trip_updates_fallback.get(fb_key)
            has_gps = (tid in live_gps_trips) or (fb_key in live_gps_fallback)
            delay_mins = (
                tu_info["stop_delays"].get(info["stop_id"], tu_info["default_delay"])
                if tu_info
                else 0.0
            )
            pred_arr_dt = info["arr_dt"] + timedelta(minutes=delay_mins)
            is_zombie = delay_mins > MAX_VALID_DELAY_MINS

            if (
                not is_zombie
                and delay_mins >= 2
                and info["stop_id"] in STATION_BUS_STOP_IDS
                and info.get("stop_role") != "ORIGIN"
                and -2 <= (pred_arr_dt - now).total_seconds() / 60.0 <= 45
            ):
                for tr in trains:
                    sched_buf = (tr["exp_depart_dt"] - info["arr_dt"]).total_seconds() / 60.0 - WALK_BUFFER_MINS
                    live_buf = (tr["exp_depart_dt"] - pred_arr_dt).total_seconds() / 60.0 - WALK_BUFFER_MINS
                    if sched_buf >= 3 and -5 <= live_buf < MIN_SAFE_TRANSFER_MINS:
                        t_key = (info["service_date"], tid, tr["train_code"])
                        if t_key not in SEEN_TRANSFERS:
                            SEEN_TRANSFERS.add(t_key)
                            append_csv_safe(
                                TRANSFERS_CSV,
                                [
                                    "logged_at", "route", "bus_trip_id", "bus_sched_arr",
                                    "bus_delay_mins", "train_dest", "train_code",
                                    "train_exp_dep", "live_buffer_mins"
                                ],
                                {
                                    "logged_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                                    "route": info["route_short_name"],
                                    "bus_trip_id": tid,
                                    "bus_sched_arr": info["arrival_time"],
                                    "bus_delay_mins": delay_mins,
                                    "train_dest": tr["destination"],
                                    "train_code": tr["train_code"],
                                    "train_exp_dep": tr["exp_depart"],
                                    "live_buffer_mins": round(live_buf, 1),
                                },
                            )

            if mins_since_origin < 10 or mins_until_stop < -10:
                continue

            alert_type = None
            if tu_info and tu_info["status"] == "CANCELED":
                alert_type = "OFFICIAL_CANCELED"
            elif is_zombie:
                alert_type = "STALLED_GHOST_RISK"
            elif not has_gps and tu_info is None:
                alert_type = "SILENT_GHOST_RISK"
            elif delay_mins >= 10:
                alert_type = "HEAVY_DELAY"

            if alert_type:
                e_key = (info["service_date"], tid, alert_type)
                if e_key not in SEEN_EVENTS:
                    SEEN_EVENTS.add(e_key)
                    append_csv_safe(
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

        # 2. Build Phase 3 UI Cards
        analytics = compute_phase2_analytics()
        route_map = analytics["route_map"]

        upcoming_buses = []
        seen_trip_stop = set()

        for item in LOOKUP["maynooth_stop_times"]:
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
            fb_key = (item["route_id"], item["origin_departure_time"])
            tu_info = trip_updates.get(tid) or trip_updates_fallback.get(fb_key)
            has_gps = (tid in live_gps_trips) or (fb_key in live_gps_fallback)

            delay_mins = (
                tu_info["stop_delays"].get(item["stop_id"], tu_info["default_delay"])
                if tu_info
                else 0.0
            )
            is_zombie = delay_mins > MAX_VALID_DELAY_MINS
            pred_arr_dt = arr_dt if is_zombie else arr_dt + timedelta(minutes=delay_mins)
            mins_away = round((pred_arr_dt - now).total_seconds() / 60.0)

            if mins_away < -4 or mins_away > 75:
                continue

            dedup_key = (tid, item["stop_id"])
            if dedup_key in seen_trip_stop:
                continue
            seen_trip_stop.add(dedup_key)

            mins_since_origin = (now - origin_dt).total_seconds() / 60.0
            if tu_info and tu_info["status"] == "CANCELED":
                status = "CANCELED"
            elif is_zombie:
                status = "STALLED_GHOST"
            elif not has_gps and tu_info is None and mins_since_origin >= 10:
                status = "SILENT_GHOST_RISK"
            elif delay_mins >= 8:
                status = "HEAVY_DELAY"
            elif has_gps:
                status = "GPS_LIVE"
            else:
                status = "SCHEDULED"

            transfer_alert = None
            if (
                item["stop_id"] in STATION_BUS_STOP_IDS
                and item.get("stop_role") != "ORIGIN"
                and status not in ("CANCELED", "STALLED_GHOST")
            ):
                for idx, tr in enumerate(trains):
                    sched_buf = (tr["exp_depart_dt"] - arr_dt).total_seconds() / 60.0 - WALK_BUFFER_MINS
                    live_buf = (tr["exp_depart_dt"] - pred_arr_dt).total_seconds() / 60.0 - WALK_BUFFER_MINS
                    if sched_buf >= 3 and -4 <= live_buf < MIN_SAFE_TRANSFER_MINS:
                        next_tr = next(
                            (t2 for t2 in trains[idx + 1 :] if t2["exp_depart_dt"] > tr["exp_depart_dt"]),
                            None,
                        )
                        transfer_alert = {
                            "train_dest": tr["destination"],
                            "train_dep": tr["exp_depart"],
                            "buffer_mins": round(live_buf, 1),
                            "backup_train": (
                                f"Next train to {next_tr['destination']} departs at {next_tr['exp_depart']}"
                                if next_tr
                                else None
                            ),
                        }
                        break

            stop_zone = (
                "station"
                if item["stop_id"] in STATION_BUS_STOP_IDS
                else ("campus" if item["stop_id"] in CAMPUS_STOP_IDS else "town")
            )

            s_date_str = s_date.strftime("%Y-%m-%d")
            tally = CROWD_TALLY.get(f"{s_date_str}|{tid}", {"ARRIVED": 0, "GHOST": 0})
            rt = item["route_short_name"]
            r_hist = route_map.get(rt, {"ghosts": 0, "canceled": 0, "delays": 0})

            upcoming_buses.append({
                "trip_id": tid,
                "service_date": s_date_str,
                "route": rt,
                "headsign": item["headsign"],
                "stop_id": item["stop_id"],
                "stop_name": item["stop_name"],
                "stop_zone": stop_zone,
                "stop_role": item.get("stop_role", ""),
                "sched_time": item["arrival_time"][:5],
                "pred_time": pred_arr_dt.strftime("%H:%M"),
                "mins_away": mins_away,
                "delay_mins": delay_mins,
                "has_gps": has_gps,
                "status": status,
                "transfer_alert": transfer_alert,
                "crowd": dict(tally),
                "history": {
                    "ghosts": r_hist["ghosts"] + r_hist["canceled"],
                    "delays": r_hist["delays"],
                },
                "backup_bus": None,
            })

        upcoming_buses.sort(key=lambda x: x["mins_away"])

        for i, bus in enumerate(upcoming_buses):
            if bus["status"] in ("CANCELED", "SILENT_GHOST_RISK", "STALLED_GHOST"):
                backup = next(
                    (
                        b2
                        for b2 in upcoming_buses[i + 1 :]
                        if b2["route"] == bus["route"]
                        and b2["stop_id"] == bus["stop_id"]
                        and b2["status"] not in ("CANCELED", "SILENT_GHOST_RISK", "STALLED_GHOST")
                    ),
                    None,
                )
                if backup:
                    bus["backup_bus"] = f"Next {backup['route']} arrives at {backup['pred_time']} (in {backup['mins_away']} min)"

        serializable_trains = [
            {k: v for k, v in tr.items() if k != "exp_depart_dt"} for tr in trains[:6]
        ]

        result = {
            "updated_at": now.strftime("%H:%M:%S"),
            "warning": None,
            "trains": serializable_trains,
            "buses": upcoming_buses[:40],
            "analytics": analytics,
            "stats": {
                "active_gps_national": len(live_gps_trips),
                "ghost_or_cancel": sum(
                    1 for b in upcoming_buses if b["status"] in ("CANCELED", "SILENT_GHOST_RISK", "STALLED_GHOST")
                ),
                "transfer_risks": sum(1 for b in upcoming_buses if b["transfer_alert"]),
            },
        }
        CACHE["timestamp"] = time.time()
        CACHE["payload"] = result
        del vp_feed, tu_feed
        gc.collect()
        return result


def background_logger_worker():
    time.sleep(8)
    while True:
        try:
            get_live_snapshot(force_refresh=True)
        except Exception as exc:
            print(f"[{datetime.now(TZ).strftime('%H:%M:%S')}] Background poll note: {exc}")
        sync_to_gist(force=False)
        time.sleep(90)


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Maynooth Connection Guard</title>
  <style>
    :root {
      --bg: #0f172a; --card: #1e293b; --border: #334155;
      --text: #f8fafc; --muted: #94a3b8;
      --green: #22c55e; --amber: #f59e0b; --red: #ef4444; --blue: #38bdf8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: system-ui, -apple-system, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 14px; max-width: 680px; margin: 0 auto; }
    header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
    h1 { font-size: 1.2rem; font-weight: 700; }
    .sub { font-size: 0.78rem; color: var(--muted); }
    .warn-banner {
      background: rgba(245,158,11,0.18); border: 1px solid rgba(245,158,11,0.45);
      color: #fde68a; padding: 8px 12px; border-radius: 8px; font-size: 0.8rem; margin-bottom: 12px; display: none;
    }
    .pill-bar { display: flex; gap: 8px; margin-bottom: 14px; overflow-x: auto; padding-bottom: 2px; }
    .pill {
      background: var(--card); border: 1px solid var(--border); color: var(--text);
      padding: 6px 12px; border-radius: 999px; font-size: 0.82rem; cursor: pointer; white-space: nowrap;
    }
    .pill.active { background: var(--blue); color: #0f172a; font-weight: 700; border-color: var(--blue); }
    .section-title { font-size: 0.82rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); margin: 14px 0 8px; }
    .train-strip { display: grid; grid-template-columns: repeat(auto-fill, minmax(195px, 1fr)); gap: 8px; margin-bottom: 14px; }
    .train-card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 10px; }
    .train-dest { font-weight: 700; font-size: 0.92rem; }
    .train-meta { font-size: 0.8rem; color: var(--muted); margin-top: 4px; }
    .bus-card {
      background: var(--card); border: 1px solid var(--border); border-radius: 12px;
      padding: 12px; margin-bottom: 10px;
    }
    .bus-top { display: flex; justify-content: space-between; align-items: center; gap: 8px; }
    .route-badge {
      background: #0284c7; color: white; font-weight: 800; font-size: 0.95rem;
      padding: 4px 10px; border-radius: 8px; min-width: 52px; text-align: center;
    }
    .bus-info { flex: 1; }
    .bus-head { font-weight: 600; font-size: 0.95rem; }
    .bus-stop { font-size: 0.78rem; color: var(--muted); margin-top: 2px; }
    .role-note { color: #fbbf24; font-weight: 600; }
    .eta { text-align: right; }
    .eta-mins { font-size: 1.05rem; font-weight: 800; }
    .eta-clock { font-size: 0.75rem; color: var(--muted); }
    .tags { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; align-items: center; }
    .tag { font-size: 0.72rem; font-weight: 700; padding: 3px 8px; border-radius: 6px; }
    .tag-live { background: rgba(34,197,94,0.18); color: var(--green); }
    .tag-sched { background: rgba(148,163,184,0.18); color: var(--muted); }
    .tag-delay { background: rgba(245,158,11,0.2); color: var(--amber); }
    .tag-ghost { background: rgba(239,68,68,0.22); color: var(--red); }
    .tag-hist { background: rgba(56,189,248,0.14); color: #7dd3fc; font-weight: 600; }
    .transfer-box {
      margin-top: 8px; padding: 8px 10px; border-radius: 8px; font-size: 0.8rem;
      background: rgba(239,68,68,0.15); border: 1px solid rgba(239,68,68,0.45); color: #fecaca;
    }
    .backup-box {
      margin-top: 8px; padding: 7px 10px; border-radius: 8px; font-size: 0.78rem;
      background: rgba(56,189,248,0.14); border: 1px solid rgba(56,189,248,0.35); color: #bae6fd;
    }
    .verify-row { display: flex; gap: 8px; margin-top: 10px; }
    .btn {
      flex: 1; padding: 8px; border-radius: 8px; border: 1px solid var(--border);
      background: #0f172a; color: var(--text); font-size: 0.8rem; font-weight: 600; cursor: pointer;
    }
    .btn:disabled { opacity: 0.4; cursor: not-allowed; }
    .btn-voted { border-color: var(--blue); color: var(--blue); }
    .btn-ok:not(:disabled):hover { border-color: var(--green); color: var(--green); }
    .btn-bad:not(:disabled):hover { border-color: var(--red); color: var(--red); }
    .kpi-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; margin-bottom: 14px; }
    .kpi-card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 12px; }
    .kpi-val { font-size: 1.4rem; font-weight: 800; color: var(--blue); }
    .kpi-lbl { font-size: 0.76rem; color: var(--muted); margin-top: 2px; }
    .chart-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 12px; margin-bottom: 12px; }
    .bar-row { margin-bottom: 10px; }
    .bar-head { display: flex; justify-content: space-between; font-size: 0.8rem; margin-bottom: 4px; }
    .bar-track { background: #0f172a; height: 10px; border-radius: 999px; overflow: hidden; display: flex; }
    .bar-ghost { background: var(--red); height: 100%; }
    .bar-delay { background: var(--amber); height: 100%; }
    .toast {
      position: fixed; bottom: 16px; left: 50%; transform: translateX(-50%);
      background: #22c55e; color: #052e16; font-weight: 700; padding: 8px 16px;
      border-radius: 999px; font-size: 0.82rem; display: none; z-index: 50;
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Maynooth Connection Guard</h1>
      <div class="sub" id="statusLine">Loading live NTA & Irish Rail feeds...</div>
    </div>
    <button class="pill" onclick="loadData()">Refresh</button>
  </header>

  <div class="warn-banner" id="warnBanner"></div>

  <div class="pill-bar">
    <button class="pill active" onclick="setTab('all', this)">All Maynooth Stops</button>
    <button class="pill" onclick="setTab('station', this)">Maynooth Station</button>
    <button class="pill" onclick="setTab('campus', this)">MU Campus</button>
    <button class="pill" onclick="setTab('analytics', this)">📊 Reliability Data</button>
  </div>

  <div id="liveView">
    <div class="section-title">Next Trains Departing Maynooth Station</div>
    <div class="train-strip" id="trainsBox"></div>

    <div class="section-title">Live Buses & Crowd Ground-Truth Check</div>
    <div id="busesBox"></div>
  </div>

  <div id="analyticsView" style="display:none;"></div>

  <div class="toast" id="toast">Report logged!</div>

  <script>
    let currentTab = 'all';
    let latestData = null;
    const VOTED_KEY = 'mcg_voted_trips';

    function getVoted() {
      try { return JSON.parse(localStorage.getItem(VOTED_KEY) || '{}'); } catch (e) { return {}; }
    }

    function setTab(tab, el) {
      currentTab = tab;
      document.querySelectorAll('.pill-bar .pill').forEach(p => p.classList.remove('active'));
      el.classList.add('active');
      render();
    }

    function showToast(msg) {
      const t = document.getElementById('toast');
      t.textContent = msg;
      t.style.display = 'block';
      setTimeout(() => { t.style.display = 'none'; }, 2400);
    }

    async function sendVerify(tripId, serviceDate, route, stopName, status, verdict) {
      const vKey = `${serviceDate}|${tripId}`;
      const voted = getVoted();
      if (voted[vKey]) {
        showToast('Already verified this bus on this device');
        return;
      }
      try {
        const res = await fetch('/api/verify', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            trip_id: tripId,
            service_date: serviceDate,
            route: route,
            stop_name: stopName,
            api_status: status,
            verdict: verdict
          })
        });
        if (res.ok) {
          voted[vKey] = verdict;
          try { localStorage.setItem(VOTED_KEY, JSON.stringify(voted)); } catch (e) {}
          const out = await res.json();
          if (latestData && latestData.buses) {
            latestData.buses.forEach(b => {
              if (b.trip_id === tripId && b.service_date === serviceDate) {
                b.crowd = out.tally;
              }
            });
            render();
          }
          showToast(verdict === 'ARRIVED' ? 'Logged: Bus Arrived' : 'Logged: Ghost Bus');
        }
      } catch (e) {
        showToast('Network error saving report');
      }
    }

    function statusBadge(b) {
      if (b.crowd.GHOST > 0 && ['SILENT_GHOST_RISK', 'CANCELED', 'STALLED_GHOST', 'SCHEDULED'].includes(b.status)) {
        return `<span class="tag tag-ghost">👻 CROWD CONFIRMED GHOST (${b.crowd.GHOST})</span>`;
      }
      if (b.crowd.ARRIVED > 0 && !b.has_gps) {
        return `<span class="tag tag-delay">📡 UNTRACKED BUS • GPS FAULT (${b.crowd.ARRIVED})</span>`;
      }
      if (b.crowd.ARRIVED > 0 && b.has_gps) {
        return `<span class="tag tag-live">✅ CROWD VERIFIED ARRIVED (${b.crowd.ARRIVED})</span>`;
      }
      if (b.status === 'CANCELED') return '<span class="tag tag-ghost">OFFICIAL CANCELED</span>';
      if (b.status === 'SILENT_GHOST_RISK') return '<span class="tag tag-ghost">SILENT GHOST RISK (NO GPS)</span>';
      if (b.status === 'STALLED_GHOST') return '<span class="tag tag-ghost">STALLED / PHANTOM ETA</span>';
      if (b.status === 'HEAVY_DELAY') return `<span class="tag tag-delay">+${b.delay_mins}m LATE (GPS)</span>`;
      if (b.status === 'GPS_LIVE') {
        const d = b.delay_mins > 0 ? ` (+${b.delay_mins}m)` : ' (On time)';
        return `<span class="tag tag-live">GPS LIVE${d}</span>`;
      }
      return '<span class="tag tag-sched">SCHEDULED (NO PING YET)</span>';
    }

    function formatRole(role) {
      if (role === 'TERMINUS') return '<span class="role-note">• Arriving Only (Terminates Here)</span>';
      if (role === 'ORIGIN') return '• Starts Here';
      return '• Through Stop';
    }

    function renderAnalytics() {
      const a = latestData.analytics;
      const t = a.totals;
      const maxRouteTotal = a.routes.length ? Math.max(...a.routes.map(r => r.total)) : 1;

      const barsHtml = a.routes.map(r => {
        const gPct = Math.round((r.ghosts / maxRouteTotal) * 100);
        const dPct = Math.round((r.delays / maxRouteTotal) * 100);
        return `
          <div class="bar-row">
            <div class="bar-head">
              <strong>Route ${r.route}</strong>
              <span class="sub">${r.ghosts} ghost/cancel • ${r.delays} heavy delays</span>
            </div>
            <div class="bar-track">
              <div class="bar-ghost" style="width:${gPct}%"></div>
              <div class="bar-delay" style="width:${dPct}%"></div>
            </div>
          </div>
        `;
      }).join('') || '<div class="sub">No baseline events logged yet.</div>';

      document.getElementById('analyticsView').innerHTML = `
        <div class="section-title">Phase 2 Baseline Telemetry Summary</div>
        <div class="kpi-grid">
          <div class="kpi-card">
            <div class="kpi-val" style="color:var(--red)">${t.silent_ghosts}</div>
            <div class="kpi-lbl">Unannounced Silent Ghosts (vs ${t.official_canceled} Official Canceled)</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-val" style="color:var(--amber)">${t.heavy_delays}</div>
            <div class="kpi-lbl">Heavy Delays (10+ mins)</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-val">${t.transfer_risks}</div>
            <div class="kpi-lbl">Tight / Missed Train Transfers</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-val" style="color:var(--green)">${t.crowd_total}</div>
            <div class="kpi-lbl">Crowd Checks (${t.crowd_confirmed_ghosts} Ghosts • ${t.crowd_gps_faults} GPS Faults)</div>
          </div>
        </div>

        <div class="section-title">Disruptions by Route (Red = Ghost/Cancel, Amber = 10m+ Delay)</div>
        <div class="chart-card">${barsHtml}</div>
      `;
    }

    function render() {
      if (!latestData) return;
      document.getElementById('statusLine').textContent =
        `Updated ${latestData.updated_at} • ${latestData.stats.ghost_or_cancel} active ghost/cancel • ${latestData.stats.transfer_risks} train risks`;

      const wb = document.getElementById('warnBanner');
      if (latestData.warning) {
        wb.textContent = latestData.warning;
        wb.style.display = 'block';
      } else {
        wb.style.display = 'none';
      }

      if (currentTab === 'analytics') {
        document.getElementById('liveView').style.display = 'none';
        document.getElementById('analyticsView').style.display = 'block';
        renderAnalytics();
        return;
      }

      document.getElementById('liveView').style.display = 'block';
      document.getElementById('analyticsView').style.display = 'none';

      const trainsBox = document.getElementById('trainsBox');
      trainsBox.innerHTML = latestData.trains.map(tr => `
        <div class="train-card">
          <div class="train-dest">To ${tr.destination}</div>
          <div class="train-meta">Dep ${tr.exp_depart} • Due in <strong>${tr.due_in}m</strong> ${tr.late_mins > 0 ? `(+${tr.late_mins}m late)` : ''}</div>
        </div>
      `).join('') || '<div class="sub">No departing trains in window.</div>';

      const voted = getVoted();
      const buses = latestData.buses.filter(b => currentTab === 'all' || b.stop_zone === currentTab);
      const busesBox = document.getElementById('busesBox');

      busesBox.innerHTML = buses.map(b => {
        const isCanceled = b.status === 'CANCELED';
        const etaHtml = isCanceled
          ? `<div class="eta-mins" style="color:var(--red)">Canceled</div><div class="eta-clock" style="text-decoration:line-through">Sched ${b.sched_time}</div>`
          : `<div class="eta-mins">${b.mins_away <= 0 ? 'Due' : b.mins_away + ' min'}</div><div class="eta-clock">Pred ${b.pred_time} (Sched ${b.sched_time})</div>`;

        const histPill = (b.history && (b.history.ghosts > 0 || b.history.delays > 0))
          ? `<span class="tag tag-hist">📊 Logged: ${b.history.ghosts} ghost • ${b.history.delays} delay</span>`
          : '';

        const transferHtml = b.transfer_alert
          ? `<div class="transfer-box">⚠️ <strong>Tight Train Transfer:</strong> ${b.transfer_alert.buffer_mins} min margin for ${b.transfer_alert.train_dep} train to ${b.transfer_alert.train_dest}${b.transfer_alert.backup_train ? `<br>🚆 <em>${b.transfer_alert.backup_train}</em>` : ''}</div>`
          : '';

        const backupHtml = b.backup_bus
          ? `<div class="backup-box">💡 <strong>Backup Option:</strong> ${b.backup_bus}</div>`
          : '';

        const vKey = `${b.service_date}|${b.trip_id}`;
        const userVote = voted[vKey];
        const arrivedDisabled = (isCanceled || userVote) ? 'disabled' : '';
        const ghostDisabled = userVote ? 'disabled' : '';

        return `
          <div class="bus-card">
            <div class="bus-top">
              <div class="route-badge">${b.route}</div>
              <div class="bus-info">
                <div class="bus-head">${b.headsign || 'Maynooth Service'}</div>
                <div class="bus-stop">${b.stop_name} ${formatRole(b.stop_role)}</div>
              </div>
              <div class="eta">${etaHtml}</div>
            </div>
            <div class="tags">${statusBadge(b)}${histPill}</div>
            ${transferHtml}
            ${backupHtml}
            <div class="verify-row">
              <button class="btn btn-ok ${userVote === 'ARRIVED' ? 'btn-voted' : ''}" ${arrivedDisabled}
                onclick="sendVerify('${b.trip_id}','${b.service_date}','${b.route}','${b.stop_name.replace(/'/g, "")}','${b.status}','ARRIVED')">
                ✅ ${userVote === 'ARRIVED' ? 'Voted Arrived' : 'Bus Arrived'} (${b.crowd.ARRIVED})
              </button>
              <button class="btn btn-bad ${userVote === 'GHOST' ? 'btn-voted' : ''}" ${ghostDisabled}
                onclick="sendVerify('${b.trip_id}','${b.service_date}','${b.route}','${b.stop_name.replace(/'/g, "")}','${b.status}','GHOST')">
                👻 ${userVote === 'GHOST' ? 'Voted Ghost' : 'Ghost Bus'} (${b.crowd.GHOST})
              </button>
            </div>
          </div>
        `;
      }).join('') || '<div class="sub">No buses scheduled in the next 75 minutes for this filter.</div>';
    }

    async function loadData() {
      try {
        const res = await fetch('/api/live');
        latestData = await res.json();
        render();
      } catch (e) {
        document.getElementById('statusLine').textContent = 'Connection error — retrying...';
      }
    }

    loadData();
    setInterval(loadData, 45000);
  </script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/api/live")
def api_live():
    return jsonify(get_live_snapshot(force_refresh=False))


@app.route("/api/verify", methods=["POST"])
def api_verify():
    data = request.get_json(force=True)
    verdict = data.get("verdict", "").upper()
    if verdict not in ("ARRIVED", "GHOST"):
        return jsonify({"error": "Invalid verdict"}), 400

    now = datetime.now(TZ)
    trip_id = str(data.get("trip_id", ""))
    service_date = str(data.get("service_date", now.strftime("%Y-%m-%d")))
    key = f"{service_date}|{trip_id}"

    CROWD_TALLY.setdefault(key, {"ARRIVED": 0, "GHOST": 0})
    CROWD_TALLY[key][verdict] += 1

    append_csv_safe(
        CROWD_CSV,
        ["logged_at", "service_date", "route", "trip_id", "stop_name", "api_status", "verdict"],
        {
            "logged_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "service_date": service_date,
            "route": data.get("route", ""),
            "trip_id": trip_id,
            "stop_name": data.get("stop_name", ""),
            "api_status": data.get("api_status", ""),
            "verdict": verdict,
        },
    )

    with CACHE_LOCK:
        if CACHE["payload"] and "buses" in CACHE["payload"]:
            for bus in CACHE["payload"]["buses"]:
                if bus["trip_id"] == trip_id and bus["service_date"] == service_date:
                    bus["crowd"] = dict(CROWD_TALLY[key])
            CACHE["payload"]["analytics"] = compute_phase2_analytics()

    return jsonify({"ok": True, "tally": CROWD_TALLY[key]})


@app.route("/api/health")
def api_health():
    return jsonify({"ok": True, "time": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")})


@app.route("/api/export/<filename>")
def api_export(filename: str):
    allowed = {"baseline_events.csv", "transfer_alerts.csv", "crowd_reports.csv"}
    if filename not in allowed:
        return jsonify({"error": "File not allowed"}), 404
    fpath = Path("data") / filename
    if not fpath.exists():
        return jsonify({"error": "No data recorded yet"}), 404
    return send_file(fpath, mimetype="text/csv", as_attachment=True, download_name=filename)


_WORKER_STARTED = False


def start_background_worker():
    global _WORKER_STARTED
    if _WORKER_STARTED:
        return
    _WORKER_STARTED = True
    t = threading.Thread(target=background_logger_worker, daemon=True)
    t.start()


start_background_worker()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
