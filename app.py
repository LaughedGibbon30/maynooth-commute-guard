import gc
import csv
import json
import math
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
from build_lookup import check_lookup_expiry, refresh_lookup_if_needed
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
SUMMARY_CSV = Path("data/daily_summary.csv")

STATION_BUS_STOP_IDS = {"8260DB003917", "8260DB007779", "8260B1354601"}
CAMPUS_STOP_IDS = {"8260B1031601", "8260B1034301", "8260PB10049"}
WALK_BUFFER_MINS = 3
MIN_SAFE_TRANSFER_MINS = 5
MAX_VALID_DELAY_MINS = 45
STALE_GPS_THRESHOLD_SECONDS = 300
CACHE_TTL_SECONDS = 45
TZ = ZoneInfo("Europe/Dublin")

app = Flask(__name__)

LOOKUP_LOCK = threading.Lock()
LAST_GTFS_CHECK = 0.0

try:
    refresh_lookup_if_needed(force=False)
except Exception as exc:
    print(f"Startup GTFS refresh note: {exc}")

with LOOKUP_FILE.open("r", encoding="utf-8") as f:
    LOOKUP = json.load(f)


def maybe_refresh_gtfs(force: bool = False) -> dict:
    global LOOKUP, LAST_GTFS_CHECK
    now_ts = time.time()
    if not force and (now_ts - LAST_GTFS_CHECK < 21600):
        expired, reason = check_lookup_expiry()
        return {"refreshed": False, "reason": reason}
    LAST_GTFS_CHECK = now_ts
    res = refresh_lookup_if_needed(force=force)
    if res.get("refreshed") and LOOKUP_FILE.exists():
        with LOOKUP_FILE.open("r", encoding="utf-8") as f:
            fresh = json.load(f)
        with LOOKUP_LOCK:
            LOOKUP = fresh
        print(f"Hot-reloaded LOOKUP ({res.get('stop_times_count')} rows, end={res.get('feed_info', {}).get('feed_end_date')}).")
    return res


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

BOOT_TIME = datetime.now(TZ)
CROWD_TALLY: dict[str, dict[str, int]] = {}
SEEN_EVENTS: set[tuple[str, str, str]] = set()
SEEN_TRANSFERS: set[tuple[str, str, str]] = set()
SEEN_EVALUATED_TRIPS: set[tuple[str, str]] = set()
SEEN_GHOST_TRIPS: set[tuple[str, str]] = set()
SEEN_DELAY_TRIPS: set[tuple[str, str]] = set()
SUMMARY_STATE: dict[tuple[str, str, str], dict] = {}


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
        for fname in ("baseline_events.csv", "transfer_alerts.csv", "crowd_reports.csv", "daily_summary.csv"):
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
        for fname in ("baseline_events.csv", "transfer_alerts.csv", "crowd_reports.csv", "daily_summary.csv"):
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

    for s_date, tid, at in SEEN_EVENTS:
        if at in ("SILENT_GHOST_RISK", "OFFICIAL_CANCELED", "STALLED_GHOST_RISK", "STALE_GPS_ZOMBIE"):
            SEEN_GHOST_TRIPS.add((s_date, tid))
        elif at in ("HEAVY_DELAY", "KINEMATIC_DELAY_HIDDEN"):
            SEEN_DELAY_TRIPS.add((s_date, tid))

    if SUMMARY_CSV.exists():
        with SUMMARY_CSV.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                s_key = (
                    row.get("service_date", ""),
                    row.get("route", "Unknown"),
                    row.get("operator", "") or "Unlabeled",
                )
                SUMMARY_STATE[s_key] = {
                    "gtfs_rt_expected": str(row.get("gtfs_rt_expected", "True")).lower() == "true",
                    "total_evaluated": int(row.get("total_evaluated", 0) or 0),
                    "ghost_or_cancel": int(row.get("ghost_or_cancel", 0) or 0),
                    "heavy_delays": int(row.get("heavy_delays", 0) or 0),
                    "restored_on_boot": True,
                }


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


def flush_daily_summary(now: datetime):
    if not SUMMARY_STATE:
        return
    fieldnames = [
        "service_date", "route", "operator", "gtfs_rt_expected",
        "total_evaluated", "ghost_or_cancel", "heavy_delays",
        "clean_trips", "ghost_rate_pct", "otp_pct", "updated_at"
    ]
    rows = []
    for (s_date, rt, op), st in sorted(SUMMARY_STATE.items()):
        total_eval = max(
            int(st.get("total_evaluated", 0)),
            int(st.get("ghost_or_cancel", 0)) + int(st.get("heavy_delays", 0)),
        )
        ghosts = int(st.get("ghost_or_cancel", 0))
        delays = int(st.get("heavy_delays", 0))
        clean = max(0, total_eval - ghosts - delays)
        ghost_pct = round((ghosts / total_eval) * 100, 1) if total_eval > 0 else 0.0
        otp_pct = round((clean / total_eval) * 100, 1) if total_eval > 0 else 0.0
        rows.append({
            "service_date": s_date,
            "route": rt,
            "operator": op,
            "gtfs_rt_expected": st.get("gtfs_rt_expected", True),
            "total_evaluated": total_eval,
            "ghost_or_cancel": ghosts,
            "heavy_delays": delays,
            "clean_trips": clean,
            "ghost_rate_pct": ghost_pct,
            "otp_pct": otp_pct,
            "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        })

    with CSV_LOCK:
        SUMMARY_CSV.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = SUMMARY_CSV.with_suffix(".csv.tmp")
        with tmp_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, SUMMARY_CSV)


def compute_phase2_analytics() -> dict:
    route_stats = defaultdict(lambda: {"ghosts": 0, "canceled": 0, "delays": 0, "total": 0})
    operator_stats = defaultdict(lambda: {"ghosts": 0, "canceled": 0, "delays": 0, "total": 0})
    totals = {
        "total_events": 0,
        "silent_ghosts": 0,
        "official_canceled": 0,
        "stalled_ghosts": 0,
        "late_avl_recoveries": 0,
        "heavy_delays": 0,
        "kinematic_delays": 0,
        "transfer_risks": 0,
        "crowd_total": 0,
        "crowd_confirmed_ghosts": 0,
        "crowd_gps_faults": 0,
        "crowd_verified_arrived": 0,
        "total_evaluated": 0,
        "clean_trips": 0,
        "ghost_rate_pct": 0.0,
        "otp_pct": 0.0,
    }

    if EVENTS_CSV.exists():
        with EVENTS_CSV.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                rt = row.get("route", "Unknown")
                op = (row.get("operator") or "").strip() or "Unlabeled (Legacy)"
                at = row.get("alert_type", "")
                totals["total_events"] += 1
                route_stats[rt]["total"] += 1
                operator_stats[op]["total"] += 1

                if at == "SILENT_GHOST_RISK":
                    totals["silent_ghosts"] += 1
                    route_stats[rt]["ghosts"] += 1
                    operator_stats[op]["ghosts"] += 1
                elif at == "OFFICIAL_CANCELED":
                    totals["official_canceled"] += 1
                    route_stats[rt]["canceled"] += 1
                    operator_stats[op]["canceled"] += 1
                elif at in ("STALLED_GHOST_RISK", "STALE_GPS_ZOMBIE"):
                    totals["stalled_ghosts"] += 1
                    route_stats[rt]["ghosts"] += 1
                    operator_stats[op]["ghosts"] += 1
                elif at == "LATE_AVL_RECOVERY":
                    totals["late_avl_recoveries"] += 1
                elif at in ("HEAVY_DELAY", "KINEMATIC_DELAY_HIDDEN"):
                    totals["heavy_delays"] += 1
                    if at == "KINEMATIC_DELAY_HIDDEN":
                        totals["kinematic_delays"] += 1
                    route_stats[rt]["delays"] += 1
                    operator_stats[op]["delays"] += 1

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

    route_eval_map = defaultdict(int)
    route_clean_map = defaultdict(int)
    for (_, rt, _), st in SUMMARY_STATE.items():
        ev = max(
            int(st.get("total_evaluated", 0)),
            int(st.get("ghost_or_cancel", 0)) + int(st.get("heavy_delays", 0)),
        )
        gh = int(st.get("ghost_or_cancel", 0))
        dl = int(st.get("heavy_delays", 0))
        cl = max(0, ev - gh - dl)
        totals["total_evaluated"] += ev
        totals["clean_trips"] += cl
        route_eval_map[rt] += ev
        route_clean_map[rt] += cl

    if totals["total_evaluated"] > 0:
        total_gh = totals["silent_ghosts"] + totals["official_canceled"] + totals["stalled_ghosts"]
        totals["ghost_rate_pct"] = round((total_gh / totals["total_evaluated"]) * 100, 1)
        totals["otp_pct"] = round((totals["clean_trips"] / totals["total_evaluated"]) * 100, 1)

    ranked_routes = sorted(
        [
            {
                "route": rt,
                "ghosts": st["ghosts"] + st["canceled"],
                "delays": st["delays"],
                "total": st["total"],
                "evaluated": max(route_eval_map[rt], st["total"]),
                "ghost_rate_pct": round(((st["ghosts"] + st["canceled"]) / max(route_eval_map[rt], st["total"])) * 100, 1)
                if max(route_eval_map[rt], st["total"]) > 0
                else 0.0,
                "otp_pct": round((route_clean_map[rt] / max(route_eval_map[rt], st["total"])) * 100, 1)
                if max(route_eval_map[rt], st["total"]) > 0
                else 0.0,
            }
            for rt, st in route_stats.items()
        ],
        key=lambda x: x["total"],
        reverse=True,
    )

    ranked_operators = sorted(
        [
            {
                "operator": op,
                "ghosts": st["ghosts"] + st["canceled"],
                "delays": st["delays"],
                "total": st["total"],
            }
            for op, st in operator_stats.items()
        ],
        key=lambda x: x["total"],
        reverse=True,
    )

    return {
        "totals": totals,
        "routes": ranked_routes,
        "operators": ranked_operators,
        "route_map": dict(route_stats),
    }


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def compute_kinematic_delay(
    info: dict,
    tid: str,
    fb_key: tuple[str, str],
    now: datetime,
    live_gps_coords: dict[str, tuple[float, float]],
    live_gps_coords_fallback: dict[tuple[str, str], tuple[float, float]],
) -> tuple[float | None, float | None, float]:
    bus_coords = live_gps_coords.get(tid) or live_gps_coords_fallback.get(fb_key)
    stop_meta = LOOKUP.get("stops", {}).get(info["stop_id"], {})
    s_lat = stop_meta.get("stop_lat")
    s_lon = stop_meta.get("stop_lon")
    if not bus_coords or s_lat is None or s_lon is None:
        return None, None, 0.0

    dist_km = haversine_km(bus_coords[0], bus_coords[1], float(s_lat), float(s_lon))
    gps_eta_mins = 0.0 if dist_km < 0.15 else max(1.0, dist_km * 3.0)
    sched_mins_left = (info["arr_dt"] - now).total_seconds() / 60.0

    if dist_km < 0.8 or not (-5.0 <= sched_mins_left <= 45.0):
        return round(dist_km, 2), round(gps_eta_mins, 1), 0.0

    hidden_delay = max(0.0, round(gps_eta_mins - sched_mins_left, 1))
    return round(dist_km, 2), round(gps_eta_mins, 1), hidden_delay


def resolve_stop_delay(tu_info: dict | None, stop_id: str, stop_sequence: int | None = None) -> float:
    if not tu_info:
        return 0.0
    stop_delays = tu_info.get("stop_delays", {})
    if stop_id and stop_id in stop_delays:
        return stop_delays[stop_id]
    seq_delays = tu_info.get("seq_delays", [])
    if stop_sequence is not None and seq_delays:
        best_preceding = None
        for seq, d_min in seq_delays:
            if seq == stop_sequence:
                return d_min
            if seq < stop_sequence:
                best_preceding = d_min
            else:
                break
        if best_preceding is not None:
            return best_preceding
    first_delay = tu_info.get("first_delay")
    return first_delay if first_delay is not None else 0.0


def build_bus_diagnostic(b: dict) -> str:
    status = b.get("status", "SCHEDULED")
    mins_away = max(0, int(b.get("mins_away", 0) or 0))
    delay_mins = float(b.get("delay_mins", 0) or 0)
    dist_km = b.get("distance_km")
    gps_eta = b.get("gps_eta_mins")
    mso = max(12, int(b.get("mins_since_origin", 12) or 12))
    ta = b.get("transfer_alert")

    if status == "CANCELED":
        return "Operator officially marked this trip CANCELED in the live TFI feed."
    if status == "CROWD_CONFIRMED_GHOST":
        return "Commuters at the stop confirmed this scheduled bus never arrived (crowd-verified ghost)."
    if status == "GPS_FAULT_VERIFIED":
        return "Commuters verified this bus arrived physically despite an offline onboard GPS transponder."
    if ta:
        buf = ta.get("buffer_mins", 1)
        dest = ta.get("train_dest", "Dublin")
        return f"Bus delay cuts your Maynooth Station walk buffer to {buf} min \u2014 likely to miss the next {dest} train."
    if status == "SILENT_GHOST_RISK":
        return f"No GPS signal detected {mso}+ mins after scheduled terminus departure \u2014 high silent-ghost risk."
    if status == "STALE_GPS":
        return "Vehicle GPS transponder froze over 5 minutes ago \u2014 possible depot-parked or stalled bus."
    if status == "STALLED_GHOST":
        return "TFI delay prediction is abnormally stalled (+45m+ phantom ETA) \u2014 treat as high ghost risk."
    if status == "UNTRACKED_OPERATOR":
        return "Operator does not broadcast live GPS telemetry on this route; rely on scheduled timetable."
    if dist_km is not None and gps_eta is not None and gps_eta >= mins_away + 5:
        return f"Schedule claims due in {mins_away} min, but live GPS places the bus {dist_km} km away (~{gps_eta} min by road)."
    if status == "HEAVY_DELAY" or delay_mins >= 8:
        dist_txt = f" ({dist_km} km away)" if dist_km is not None else ""
        return f"Running {round(delay_mins, 1)} min behind schedule in live traffic{dist_txt}."
    if status == "GPS_LIVE":
        dist_txt = f"{dist_km} km away" if dist_km is not None else "active on route"
        delay_txt = f"+{round(delay_mins, 1)} min delay" if delay_mins > 0 else "on schedule"
        return f"Live GPS transponder active ({dist_txt}, {delay_txt})."
    return "Scheduled service awaiting terminus departure or initial GPS login."


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
        stale_gps_trips = set()
        stale_gps_fallback = set()
        live_gps_routes = set()
        live_gps_coords: dict[str, tuple[float, float]] = {}
        live_gps_coords_fallback: dict[tuple[str, str], tuple[float, float]] = {}
        feed_ts = int(vp_feed.header.timestamp) if vp_feed.header.HasField("timestamp") else int(now_ts)

        for entity in vp_feed.entity:
            if not entity.HasField("vehicle"):
                continue
            veh = entity.vehicle
            t = veh.trip
            if t.route_id:
                live_gps_routes.add(t.route_id)

            ping_ts = int(veh.timestamp) if (veh.HasField("timestamp") and veh.timestamp > 0) else feed_ts
            is_stale = (ping_ts > 0) and ((now_ts - ping_ts) > STALE_GPS_THRESHOLD_SECONDS)

            if is_stale:
                if t.trip_id:
                    stale_gps_trips.add(t.trip_id)
                if t.route_id and t.start_time:
                    stale_gps_fallback.add((t.route_id, t.start_time))
                continue

            coords = None
            if veh.HasField("position"):
                lat = float(veh.position.latitude)
                lon = float(veh.position.longitude)
                if lat != 0.0 or lon != 0.0:
                    coords = (lat, lon)
            if t.trip_id:
                live_gps_trips.add(t.trip_id)
                if coords:
                    live_gps_coords[t.trip_id] = coords
            if t.route_id and t.start_time:
                fb = (t.route_id, t.start_time)
                live_gps_fallback.add(fb)
                if coords:
                    live_gps_coords_fallback[fb] = coords

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
            first_delay = None
            stop_delays = {}
            seq_delays = []
            for stu in tu.stop_time_update:
                d_sec = None
                if stu.HasField("arrival") and stu.arrival.HasField("delay"):
                    d_sec = stu.arrival.delay
                elif stu.HasField("departure") and stu.departure.HasField("delay"):
                    d_sec = stu.departure.delay
                if d_sec is not None:
                    d_min = round(d_sec / 60, 1)
                    if first_delay is None:
                        first_delay = d_min
                    if stu.stop_id:
                        stop_delays[stu.stop_id] = d_min
                    if stu.HasField("stop_sequence"):
                        seq_delays.append((int(stu.stop_sequence), d_min))

            seq_delays.sort(key=lambda x: x[0])
            payload = {
                "status": rel,
                "first_delay": first_delay,
                "stop_delays": stop_delays,
                "seq_delays": seq_delays,
            }
            if t.trip_id:
                trip_updates[t.trip_id] = payload
            if t.route_id and t.start_time:
                trip_updates_fallback[(t.route_id, t.start_time)] = payload

        try:
            trains = fetch_trains(now)
        except Exception:
            trains = []

        # 1. Background Phase 2 Logging (Trip-level ghosts + Dedicated Station-stop transfers)
        active_window_trips = {}
        station_stop_visits = []
        seen_station_visit = set()

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

            s_date_str = s_date.strftime("%Y-%m-%d")
            tid = item["trip_id"]
            enriched = {
                **item,
                "service_date": s_date_str,
                "origin_dt": origin_dt,
                "arr_dt": arr_dt,
            }

            if tid not in active_window_trips or arr_dt < active_window_trips[tid]["arr_dt"]:
                active_window_trips[tid] = enriched

            if (
                item["stop_id"] in STATION_BUS_STOP_IDS
                and item.get("stop_role") != "ORIGIN"
                and item.get("drop_off_type", "0") != "1"
            ):
                sv_key = (s_date_str, tid, item["stop_id"])
                if sv_key not in seen_station_visit:
                    seen_station_visit.add(sv_key)
                    station_stop_visits.append(enriched)

        for info in station_stop_visits:
            tid = info["trip_id"]
            fb_key = (info["route_id"], info["origin_departure_time"])
            tu_info = trip_updates.get(tid) or trip_updates_fallback.get(fb_key)
            api_delay_mins = resolve_stop_delay(tu_info, info["stop_id"], info.get("stop_sequence"))
            _, _, kin_delay_mins = compute_kinematic_delay(
                info, tid, fb_key, now, live_gps_coords, live_gps_coords_fallback
            )
            is_zombie = api_delay_mins > MAX_VALID_DELAY_MINS
            delay_mins = api_delay_mins if is_zombie else max(api_delay_mins, kin_delay_mins)
            pred_arr_dt = info["arr_dt"] + timedelta(minutes=delay_mins)

            if (
                not is_zombie
                and delay_mins >= 2
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

        summary_dirty = False
        for tid, info in active_window_trips.items():
            mins_since_origin = (now - info["origin_dt"]).total_seconds() / 60.0
            mins_until_stop = (info["arr_dt"] - now).total_seconds() / 60.0
            fb_key = (info["route_id"], info["origin_departure_time"])

            tu_info = trip_updates.get(tid) or trip_updates_fallback.get(fb_key)
            has_gps = (tid in live_gps_trips) or (fb_key in live_gps_fallback)
            is_stale_gps = ((tid in stale_gps_trips) or (fb_key in stale_gps_fallback)) and not has_gps
            is_stale_gps = ((tid in stale_gps_trips) or (fb_key in stale_gps_fallback)) and not has_gps
            delay_mins = resolve_stop_delay(tu_info, info["stop_id"], info.get("stop_sequence"))
            is_zombie = delay_mins > MAX_VALID_DELAY_MINS

            if has_gps and (info["service_date"], tid, "SILENT_GHOST_RISK") in SEEN_EVENTS:
                rec_key = (info["service_date"], tid, "LATE_AVL_RECOVERY")
                if rec_key not in SEEN_EVENTS:
                    SEEN_EVENTS.add(rec_key)
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
                            "alert_type": "LATE_AVL_RECOVERY",
                            "origin_dep": info["origin_departure_time"],
                            "sched_maynooth_arr": info["arrival_time"],
                            "delay_mins": delay_mins,
                            "has_gps": True,
                            "stop_name": info["stop_name"],
                            "stop_role": info.get("stop_role", ""),
                        },
                    )

            if mins_since_origin < 10 or mins_until_stop < -10:
                continue

            is_rt_tracked = bool(info.get("gtfs_rt_expected", True)) or (info["route_id"] in live_gps_routes)
            s_key = (
                info["service_date"],
                info["route_short_name"],
                (info.get("agency_name") or "").strip() or "Unlabeled",
            )
            bucket = SUMMARY_STATE.setdefault(
                s_key,
                {
                    "gtfs_rt_expected": is_rt_tracked,
                    "total_evaluated": 0,
                    "ghost_or_cancel": 0,
                    "heavy_delays": 0,
                    "restored_on_boot": False,
                },
            )
            ev_trip_key = (info["service_date"], tid)
            if ev_trip_key not in SEEN_EVALUATED_TRIPS:
                SEEN_EVALUATED_TRIPS.add(ev_trip_key)
                if not bucket.get("restored_on_boot") or info["arr_dt"] > BOOT_TIME:
                    bucket["total_evaluated"] += 1
                    summary_dirty = True
            alert_type = None
            if tu_info and tu_info["status"] == "CANCELED":
                alert_type = "OFFICIAL_CANCELED"
            elif is_zombie:
                alert_type = "STALLED_GHOST_RISK"
            elif is_stale_gps:
                alert_type = "STALE_GPS_ZOMBIE"
            elif not has_gps and tu_info is None:
                if is_rt_tracked:
                    alert_type = "SILENT_GHOST_RISK"
            elif delay_mins >= 10:
                alert_type = "HEAVY_DELAY"
            else:
                _, _, kin_delay_mins = compute_kinematic_delay(
                    info, tid, fb_key, now, live_gps_coords, live_gps_coords_fallback
                )
                if kin_delay_mins >= 10 and delay_mins < 5:
                    alert_type = "KINEMATIC_DELAY_HIDDEN"
                    delay_mins = kin_delay_mins

            if alert_type:
                e_key = (info["service_date"], tid, alert_type)
                if e_key not in SEEN_EVENTS:
                    SEEN_EVENTS.add(e_key)
                    trip_pair = (info["service_date"], tid)
                    if alert_type in ("SILENT_GHOST_RISK", "OFFICIAL_CANCELED", "STALLED_GHOST_RISK", "STALE_GPS_ZOMBIE"):
                        if trip_pair not in SEEN_GHOST_TRIPS:
                            SEEN_GHOST_TRIPS.add(trip_pair)
                            bucket["ghost_or_cancel"] += 1
                            summary_dirty = True
                    elif alert_type in ("HEAVY_DELAY", "KINEMATIC_DELAY_HIDDEN"):
                        if trip_pair not in SEEN_DELAY_TRIPS:
                            SEEN_DELAY_TRIPS.add(trip_pair)
                            bucket["heavy_delays"] += 1
                            summary_dirty = True
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

        if summary_dirty:
            flush_daily_summary(now)

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
            bus_coords = live_gps_coords.get(tid) or live_gps_coords_fallback.get(fb_key)
            stop_meta = LOOKUP.get("stops", {}).get(item["stop_id"], {})
            s_lat = stop_meta.get("stop_lat")
            s_lon = stop_meta.get("stop_lon")
            distance_km = None
            gps_eta_mins = None
            if bus_coords and s_lat is not None and s_lon is not None:
                distance_km = round(haversine_km(bus_coords[0], bus_coords[1], float(s_lat), float(s_lon)), 2)
                gps_eta_mins = 0 if distance_km < 0.15 else max(1, round(distance_km * 3.0))

            delay_mins = resolve_stop_delay(tu_info, item["stop_id"], item.get("stop_sequence"))
            is_zombie = delay_mins > MAX_VALID_DELAY_MINS
            pred_arr_dt = arr_dt if is_zombie else arr_dt + timedelta(minutes=delay_mins)
            mins_away = round((pred_arr_dt - now).total_seconds() / 60.0)

            if mins_away < -4 or mins_away > 75:
                continue

            dedup_key = (tid, item["stop_id"])
            if dedup_key in seen_trip_stop:
                continue
            seen_trip_stop.add(dedup_key)

            is_rt_tracked = bool(item.get("gtfs_rt_expected", True)) or (item["route_id"] in live_gps_routes)
            mins_since_origin = (now - origin_dt).total_seconds() / 60.0
            if tu_info and tu_info["status"] == "CANCELED":
                status = "CANCELED"
            elif is_zombie:
                status = "STALLED_GHOST"
            elif is_stale_gps:
                status = "STALE_GPS"
            elif not has_gps and tu_info is None and not is_rt_tracked:
                status = "UNTRACKED_OPERATOR"
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
                and item.get("drop_off_type", "0") != "1"
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
                "operator": item.get("agency_name", "") or "TFI Operator",
                "is_rt_tracked": is_rt_tracked,
                "headsign": item["headsign"],
                "stop_id": item["stop_id"],
                "stop_name": item["stop_name"],
                "stop_zone": stop_zone,
                "stop_role": item.get("stop_role", ""),
                "maynooth_flow": item.get("maynooth_flow", "THROUGH"),
                "pickup_type": item.get("pickup_type", "0"),
                "drop_off_type": item.get("drop_off_type", "0"),
                "sched_time": item["arrival_time"][:5],
                "pred_time": pred_arr_dt.strftime("%H:%M"),
                "mins_away": mins_away,
                "delay_mins": delay_mins,
                "has_gps": has_gps,
                "distance_km": distance_km,
                "gps_eta_mins": gps_eta_mins,
                "bus_lat": round((live_gps_coords.get(tid) or live_gps_coords_fallback.get(fb_key))[0], 6) if (live_gps_coords.get(tid) or live_gps_coords_fallback.get(fb_key)) else None,
                "bus_lon": round((live_gps_coords.get(tid) or live_gps_coords_fallback.get(fb_key))[1], 6) if (live_gps_coords.get(tid) or live_gps_coords_fallback.get(fb_key)) else None,
                "mins_since_origin": max(0, int(round(mins_since_origin))),
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
            if bus["status"] in ("CANCELED", "SILENT_GHOST_RISK", "STALLED_GHOST", "STALE_GPS"):
                backup = next(
                    (
                        b2
                        for b2 in upcoming_buses[i + 1 :]
                        if b2["route"] == bus["route"]
                        and b2["stop_id"] == bus["stop_id"]
                        and b2["status"] not in ("CANCELED", "SILENT_GHOST_RISK", "STALLED_GHOST", "STALE_GPS")
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
            "buses": [{**b, "diagnostic": build_bus_diagnostic(b)} for b in upcoming_buses[:40]],
            "stops": LOOKUP.get("stops", {}),
            "analytics": analytics,
            "stats": {
                "active_gps_national": len(live_gps_trips),
                "ghost_or_cancel": sum(
                    1 for b in upcoming_buses if b["status"] in ("CANCELED", "SILENT_GHOST_RISK", "STALLED_GHOST", "STALE_GPS")
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
    .map-card {
      background: var(--card); border: 1px solid var(--border); border-radius: 14px;
      padding: 12px; margin-bottom: 14px;
    }
    .map-header {
      display: flex; justify-content: space-between; align-items: center;
      gap: 8px; margin-bottom: 8px; flex-wrap: wrap;
    }
    .map-title { font-weight: 700; font-size: 0.92rem; }
    .map-controls { display: flex; gap: 6px; align-items: center; }
    .map-btn {
      background: rgba(148,163,184,0.14); color: var(--text); border: 1px solid var(--border);
      border-radius: 8px; padding: 4px 9px; font-size: 0.74rem; font-weight: 600; cursor: pointer;
    }
    #commuteMap { height: 265px; width: 100%; border-radius: 10px; z-index: 1; background: #0b1220; }
    .bus-pin {
      color: #fff; font-weight: 800; font-size: 11px; padding: 3px 7px; border-radius: 999px;
      border: 2px solid #0f172a; box-shadow: 0 2px 6px rgba(0,0,0,0.45); white-space: nowrap;
      text-align: center;
    }
    .stop-pin {
      background: #1e293b; color: #38bdf8; font-weight: 700; font-size: 10px;
      padding: 2px 6px; border-radius: 6px; border: 1.5px solid #38bdf8; white-space: nowrap;
    }
    .diag-box {
      margin-top: 8px; padding: 7px 10px; border-radius: 8px; font-size: 0.8rem; line-height: 1.35;
      background: rgba(15,23,42,0.65); border-left: 3px solid #38bdf8; color: #e2e8f0;
    }
    .diag-box.diag-warn { border-left-color: var(--amber); background: rgba(245,158,11,0.10); }
    .diag-box.diag-bad { border-left-color: var(--red); background: rgba(239,68,68,0.12); }
    .bus-card.card-highlight {
      border-color: #38bdf8 !important; box-shadow: 0 0 0 2px rgba(56,189,248,0.35);
    }
    body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }
    .eta-mins, .eta-clock, .kpi-val, .route-badge { font-family: 'JetBrains Mono', monospace; font-variant-numeric: tabular-nums; }
    .pill-bar { scrollbar-width: none; -ms-overflow-style: none; }
    .pill-bar::-webkit-scrollbar { display: none; }
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
      .hero-banner {
      border-radius: 14px;
      padding: 14px 16px;
      margin-bottom: 14px;
      border: 1px solid var(--border);
      border-left-width: 5px;
      background: var(--card);
    }
    .hero-banner.hero-safe {
      border-color: rgba(34, 197, 94, 0.35);
      border-left-color: var(--green);
      background: linear-gradient(135deg, rgba(34, 197, 94, 0.14), rgba(15, 23, 42, 0.92));
    }
    .hero-banner.hero-warn {
      border-color: rgba(245, 158, 11, 0.40);
      border-left-color: var(--amber);
      background: linear-gradient(135deg, rgba(245, 158, 11, 0.16), rgba(15, 23, 42, 0.92));
    }
    .hero-banner.hero-risk {
      border-color: rgba(239, 68, 68, 0.45);
      border-left-color: var(--red);
      background: linear-gradient(135deg, rgba(239, 68, 68, 0.18), rgba(15, 23, 42, 0.92));
    }
    .hero-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 6px;
    }
    .hero-pill {
      display: inline-block;
      font-size: 0.72rem;
      font-weight: 800;
      letter-spacing: 0.05em;
      padding: 3px 8px;
      border-radius: 6px;
      text-transform: uppercase;
    }
    .hero-safe .hero-pill { background: rgba(34, 197, 94, 0.22); color: #86efac; }
    .hero-warn .hero-pill { background: rgba(245, 158, 11, 0.24); color: #fcd34d; }
    .hero-risk .hero-pill { background: rgba(239, 68, 68, 0.25); color: #fca5a5; }
    .hero-meta {
      font-size: 0.74rem;
      color: var(--muted);
      font-weight: 600;
    }
    .hero-msg {
      font-size: 0.96rem;
      font-weight: 600;
      line-height: 1.4;
      color: #f8fafc;
    }
      * {
      scrollbar-width: thin;
      scrollbar-color: #334155 #0b1220;
    }
    *::-webkit-scrollbar {
      width: 6px;
      height: 6px;
    }
    *::-webkit-scrollbar-track {
      background: #0b1220;
    }
    *::-webkit-scrollbar-thumb {
      background: #334155;
      border-radius: 999px;
    }
    .pill-bar {
      scrollbar-width: none;
      -ms-overflow-style: none;
    }
    .pill-bar::-webkit-scrollbar {
      display: none;
    }
      .train-card.train-primary {
      border-color: rgba(56, 189, 248, 0.55);
      background: linear-gradient(135deg, rgba(56, 189, 248, 0.12), rgba(30, 41, 59, 0.95));
    }
    .train-card.train-secondary {
      opacity: 0.80;
    }
    .train-tag {
      display: inline-block;
      font-size: 0.65rem;
      font-weight: 800;
      letter-spacing: 0.05em;
      color: #7dd3fc;
      text-transform: uppercase;
      margin-bottom: 3px;
    }
    .tag-risk-low { background: rgba(34, 197, 94, 0.14); color: #86efac; }
    .tag-risk-med { background: rgba(245, 158, 11, 0.18); color: #fcd34d; }
    .tag-risk-high { background: rgba(239, 68, 68, 0.22); color: #fca5a5; }
    .tfi-compare {
      margin-top: 7px;
      padding: 5px 9px;
      border-radius: 7px;
      font-size: 0.76rem;
      background: rgba(148, 163, 184, 0.10);
      color: #cbd5e1;
    }
    .card-details {
      margin-top: 8px;
      font-size: 0.76rem;
      color: var(--muted);
    }
    .card-details summary {
      cursor: pointer;
      user-select: none;
      font-weight: 600;
      color: #94a3b8;
      padding: 2px 0;
    }
    .card-details summary:hover {
      color: #e2e8f0;
    }
    .card-details-body {
      margin-top: 6px;
      padding: 8px 10px;
      border-radius: 8px;
      background: rgba(15, 23, 42, 0.55);
      border: 1px solid rgba(148, 163, 184, 0.15);
      line-height: 1.45;
      color: #cbd5e1;
    }
    .bus-card.bus-terminating {
      opacity: 0.62;
      border-style: dashed;
    }
    .term-divider {
      margin: 18px 0 10px;
      font-size: 0.74rem;
      font-weight: 700;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      color: var(--muted);
    }
      .map-legend {
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 0.72rem;
      color: var(--muted);
      margin-top: 2px;
    }
    .legend-item {
      display: inline-flex;
      align-items: center;
      gap: 4px;
    }
    .legend-dot {
      width: 8px;
      height: 8px;
      border-radius: 999px;
      display: inline-block;
    }
    .map-preview-bar {
      display: none;
      padding: 10px 12px;
      border-radius: 10px;
      background: rgba(15, 23, 42, 0.75);
      border: 1px dashed rgba(56, 189, 248, 0.35);
      font-size: 0.8rem;
      color: #cbd5e1;
      cursor: pointer;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
    }
    .map-status-strip {
      display: none;
      margin-top: 8px;
      padding: 7px 10px;
      border-radius: 8px;
      background: rgba(15, 23, 42, 0.78);
      border: 1px solid rgba(56, 189, 248, 0.28);
      font-size: 0.78rem;
      line-height: 1.4;
      color: #e2e8f0;
    }
    .leaflet-popup-content-wrapper {
      background: #0f172a !important;
      color: #f8fafc !important;
      border: 1px solid #334155 !important;
      border-radius: 10px !important;
      box-shadow: 0 8px 20px rgba(0, 0, 0, 0.55) !important;
    }
    .leaflet-popup-tip {
      background: #0f172a !important;
    }
    .leaflet-popup-content {
      margin: 8px 10px !important;
      font-family: 'Inter', sans-serif !important;
      font-size: 0.76rem !important;
      line-height: 1.35 !important;
      max-width: 190px !important;
    }
    .leaflet-container a.leaflet-popup-close-button {
      color: #94a3b8 !important;
      padding: 4px 5px 0 0 !important;
    }
  </style>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin="" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
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
    <button class="pill active" onclick="setTab('all', this)">All Maynooth</button>
    <button class="pill" onclick="setTab('outbound', this)">Outbound (Leaving)</button>
    <button class="pill" onclick="setTab('inbound', this)">Inbound (To Maynooth)</button>
    <button class="pill" onclick="setTab('station', this)">Maynooth Station</button>
    <button class="pill" onclick="setTab('campus', this)">MU Campus</button>
    <button class="pill" onclick="setTab('analytics', this)">Reliability Data</button>
  </div>

  <div id="liveView">
      <div id="heroGuardBanner" class="hero-banner hero-safe">
        <div class="hero-top">
          <span class="hero-pill">CONNECTION GUARD</span>
          <span class="hero-meta">Live Telemetry</span>
        </div>
        <div class="hero-msg">Evaluating live bus positions and Maynooth train departures...</div>
      </div>
    <div class="section-title">Next Trains Departing Maynooth Station</div>
    <div class="train-strip" id="trainsBox"></div>

    <div class="section-title">Live Buses & Crowd Ground-Truth Check</div>
          <div class="map-card" id="mapCard">
        <div class="map-header">
          <div>
            <div class="map-title">Live Maynooth Map <span id="mapLiveCount" style="font-size:0.75rem;color:var(--muted);font-weight:500"></span></div>
            <div class="map-legend">
              <span class="legend-item"><span class="legend-dot" style="background:#22c55e"></span>On time</span>
              <span class="legend-item"><span class="legend-dot" style="background:#f59e0b"></span>Late (4m+)</span>
              <span class="legend-item"><span class="legend-dot" style="background:#ef4444"></span>Ghost / Suspect</span>
            </div>
          </div>
          <div class="map-controls">
            <button class="map-btn" type="button" onclick="resetMapView()">Center</button>
            <button class="map-btn" type="button" id="toggleMapBtn" onclick="toggleMapPanel()">Hide Map</button>
          </div>
        </div>
        <div class="map-preview-bar" id="mapPreviewBar" onclick="toggleMapPanel()">
          <span id="mapPreviewText">Map collapsed ? tap to view live bus pins & station corridor</span>
          <span style="color:#38bdf8;font-weight:700;white-space:nowrap">Show Map</span>
        </div>
        <div id="commuteMap"></div>
        <div class="map-status-strip" id="mapStatusStrip"></div>
      </div>
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
        return `<span class="tag tag-ghost">CROWD CONFIRMED GHOST (${b.crowd.GHOST})</span>`;
      }
      if (b.crowd.ARRIVED > 0 && !b.has_gps) {
        return `<span class="tag tag-delay">UNTRACKED BUS • GPS FAULT (${b.crowd.ARRIVED})</span>`;
      }
      if (b.crowd.ARRIVED > 0 && b.has_gps) {
        return `<span class="tag tag-live">CROWD VERIFIED ARRIVED (${b.crowd.ARRIVED})</span>`;
      }
      if (b.status === 'UNTRACKED_OPERATOR') return '<span class="tag tag-sched">OPERATOR NO LIVE GPS</span>';
      if (b.status === 'CANCELED') return '<span class="tag tag-ghost">OFFICIAL CANCELED</span>';
      if (b.status === 'SILENT_GHOST_RISK') return '<span class="tag tag-ghost">SILENT GHOST RISK (NO GPS)</span>';
      if (b.status === 'STALLED_GHOST') return '<span class="tag tag-ghost">STALLED / PHANTOM ETA</span>';
      if (b.status === 'STALE_GPS') return '<span class="tag tag-ghost">FROZEN GPS (STALE PING)</span>';
      const distTxt = (b.distance_km !== null && b.distance_km !== undefined) ? ` ? ${b.distance_km} km away` : '';
      const lagTxt = (b.gps_eta_mins !== null && b.gps_eta_mins !== undefined && b.gps_eta_mins >= b.mins_away + 5)
        ? ` (~${b.gps_eta_mins}m by GPS)`
        : '';
      if (b.status === 'HEAVY_DELAY') return `<span class="tag tag-delay">+${b.delay_mins}m LATE${distTxt}${lagTxt}</span>`;
      if (b.status === 'GPS_LIVE') {
        const d = b.delay_mins > 0 ? ` (+${b.delay_mins}m)` : ' (On time)';
        return `<span class="tag tag-live">GPS LIVE${d}${distTxt}${lagTxt}</span>`;
      }
      return '<span class="tag tag-sched">SCHEDULED (NO PING YET)</span>';
    }

    function flowBadge(b) {
      if (b.pickup_type === '1') return '<span class="tag tag-sched">INBOUND ? DROP-OFF ONLY</span>';
      if (b.drop_off_type === '1') return '<span class="tag tag-hist">OUTBOUND ? PICK-UP ONLY</span>';
      if (b.maynooth_flow === 'OUTBOUND') return '<span class="tag tag-hist">OUTBOUND ? LEAVING MAYNOOTH</span>';
      if (b.maynooth_flow === 'INBOUND') return '<span class="tag tag-sched">INBOUND ? TO MAYNOOTH</span>';
      return '<span class="tag tag-sched">THROUGH ROUTE</span>';
    }

    function formatRole(role) {
      if (role === 'TERMINUS') return '<span class="role-note">• Arriving Only (Terminates Here)</span>';
      if (role === 'ORIGIN') return '• Starts Here';
      return '• Through Stop';
    }

    function ghostRiskBadge(b) {
      const highStatuses = ['CANCELED', 'SILENT_GHOST_RISK', 'CROWD_CONFIRMED_GHOST', 'STALE_GPS', 'STALLED_GHOST'];
      if (highStatuses.includes(b.status) || (b.crowd && b.crowd.GHOST > 0)) {
        return '<span class="tag tag-risk-high">Ghost Risk: High</span>';
      }
      const histGhosts = (b.history && b.history.ghosts) ? b.history.ghosts : 0;
      const delayMins = Number(b.delay_mins || 0);
      if (b.status === 'HEAVY_DELAY' || delayMins >= 7 || histGhosts >= 5 || !b.has_gps) {
        return '<span class="tag tag-risk-med">Ghost Risk: Medium</span>';
      }
      return '<span class="tag tag-risk-low">Ghost Risk: Low</span>';
    }

    function buildTfiComparison(b) {
      const highStatuses = ['CANCELED', 'SILENT_GHOST_RISK', 'CROWD_CONFIRMED_GHOST', 'STALE_GPS', 'STALLED_GHOST'];
      const delayMins = Math.round(Number(b.delay_mins || 0));
      if (b.status === 'CANCELED') {
        return `<div class="tfi-compare"><strong>TFI Timetable:</strong> ${b.sched_time} ? <strong>Guard:</strong> Officially canceled in live feed</div>`;
      }
      if (highStatuses.includes(b.status)) {
        const tfiClaim = b.mins_away <= 1 ? 'Due' : `${b.sched_time}`;
        return `<div class="tfi-compare"><strong>TFI Shows:</strong> ${tfiClaim} ? <strong>Guard:</strong> High ghost risk (no active GPS ping)</div>`;
      }
      if (delayMins >= 3) {
        const tfiClaim = b.mins_away <= 1 ? 'Due' : `Sched ${b.sched_time}`;
        return `<div class="tfi-compare"><strong>TFI Shows:</strong> ${tfiClaim} ? <strong>Guard:</strong> Running +${delayMins} min late (Pred ${b.pred_time})</div>`;
      }
      return '';
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
              <span class="sub">${r.ghosts} ghost (${r.ghost_rate_pct}%) ? ${r.delays} delay ? ${r.otp_pct}% OTP (${r.evaluated} trips)</span>
            </div>
            <div class="bar-track">
              <div class="bar-ghost" style="width:${gPct}%"></div>
              <div class="bar-delay" style="width:${dPct}%"></div>
            </div>
          </div>
        `;
      }).join('') || '<div class="sub">No baseline events logged yet.</div>';

      const maxOpTotal = (a.operators && a.operators.length) ? Math.max(...a.operators.map(o => o.total)) : 1;
      const opBarsHtml = (a.operators || []).map(o => {
        const gPct = Math.round((o.ghosts / maxOpTotal) * 100);
        const dPct = Math.round((o.delays / maxOpTotal) * 100);
        return `
          <div class="bar-row">
            <div class="bar-head">
              <strong>${o.operator}</strong>
              <span class="sub">${o.ghosts} ghost/cancel ? ${o.delays} heavy delays</span>
            </div>
            <div class="bar-track">
              <div class="bar-ghost" style="width:${gPct}%"></div>
              <div class="bar-delay" style="width:${dPct}%"></div>
            </div>
          </div>
        `;
      }).join('') || '<div class="sub">No operator events logged yet.</div>';

      document.getElementById('analyticsView').innerHTML = `
        <div class="section-title">Phase 2 Baseline Telemetry Summary (${t.total_evaluated} Scheduled Trips Evaluated)</div>
        <div class="kpi-grid">
          <div class="kpi-card">
            <div class="kpi-val" style="color:var(--green)">${t.otp_pct}%</div>
            <div class="kpi-lbl">On-Time Performance (${t.clean_trips} of ${t.total_evaluated} clean)</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-val" style="color:var(--red)">${t.ghost_rate_pct}%</div>
            <div class="kpi-lbl">Overall Ghost / Cancel Rate</div>
          </div>
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

        <div class="section-title">Disruptions by Operator (agency.txt)</div>
        <div class="chart-card">${opBarsHtml}</div>

        <div class="section-title">Export NTA Evidence CSVs</div>
        <div class="pill-bar">
          <a class="pill" href="/api/export/daily_summary.csv" style="text-decoration:none">daily_summary.csv</a>
          <a class="pill" href="/api/export/baseline_events.csv" style="text-decoration:none">baseline_events.csv</a>
          <a class="pill" href="/api/export/transfer_alerts.csv" style="text-decoration:none">transfer_alerts.csv</a>
          <a class="pill" href="/api/export/crowd_reports.csv" style="text-decoration:none">crowd_reports.csv</a>
        </div>
      `;
    }

        function hhmmToMins(hhmm) {
      if (!hhmm || !hhmm.includes(':')) return null;
      const parts = hhmm.split(':');
      const h = parseInt(parts[0], 10);
      const m = parseInt(parts[1], 10);
      if (Number.isNaN(h) || Number.isNaN(m)) return null;
      return h * 60 + m;
    }

    function renderHeroBanner() {
      const box = document.getElementById('heroGuardBanner');
      if (!box || !latestData) return;

      const buses = latestData.buses || [];
      const trains = latestData.trains || [];
      const ghostStatuses = ['CANCELED', 'SILENT_GHOST_RISK', 'CROWD_CONFIRMED_GHOST', 'STALE_GPS', 'STALLED_GHOST'];

      let cls = 'hero-safe';
      let pill = 'ALL CLEAR';
      let meta = 'Live Connection Guard';
      let msg = `Monitoring ${buses.length} Maynooth bus services and ${trains.length} departing trains. No active connection risks detected.`;

      // Priority 1: Check explicit transfer alerts (tight or missed train connections)
      const riskyTransfer = buses.find(b => b.transfer_alert && (ghostStatuses.includes(b.status) || b.transfer_alert.buffer_mins <= 2));
      const tightTransfer = buses.find(b => b.transfer_alert && b.transfer_alert.buffer_mins > 2 && b.transfer_alert.buffer_mins <= 5);

      if (riskyTransfer) {
        const ta = riskyTransfer.transfer_alert;
        cls = 'hero-risk';
        pill = 'CONNECTION AT RISK';
        meta = `Route ${riskyTransfer.route} to Train`;
        const reason = riskyTransfer.status === 'CANCELED'
          ? 'officially canceled'
          : (ghostStatuses.includes(riskyTransfer.status)
              ? 'flagged high ghost risk (no live GPS)'
              : `running +${Math.round(riskyTransfer.delay_mins || 0)} min late`);
        const backupTxt = ta.backup_train ? ` Next safe option: ${ta.backup_train}.` : '';
        msg = `Route ${riskyTransfer.route} is ${reason}. Likely to miss the ${ta.train_dep} train to ${ta.train_dest}.${backupTxt}`;
      } else if (tightTransfer) {
        const ta = tightTransfer.transfer_alert;
        cls = 'hero-warn';
        pill = 'TIGHT CONNECTION';
        meta = `${ta.buffer_mins} min buffer`;
        msg = `Route ${tightTransfer.route} predicted at ${tightTransfer.pred_time}, leaving a ${ta.buffer_mins} min buffer for the ${ta.train_dep} train to ${ta.train_dest}.`;
      } else {
        // Priority 2: Pair the next active boardable bus with the next departing train
        const nextBus = buses.find(b => b.pickup_type !== '1' && b.stop_role !== 'TERMINUS' && !ghostStatuses.includes(b.status));
        const ghostBus = buses.find(b => b.pickup_type !== '1' && b.stop_role !== 'TERMINUS' && ghostStatuses.includes(b.status));

        if (nextBus && trains.length > 0) {
          const bMins = hhmmToMins(nextBus.pred_time);
          let matchedTrain = null;
          let buffer = null;
          for (const tr of trains) {
            const tMins = hhmmToMins(tr.exp_depart);
            if (bMins !== null && tMins !== null) {
              let diff = tMins - bMins;
              if (diff < -720) diff += 1440;
              if (diff >= 3 && diff <= 40) {
                matchedTrain = tr;
                buffer = diff;
                break;
              }
            }
          }
          if (matchedTrain && buffer !== null) {
            if (buffer <= 5) {
              cls = 'hero-warn';
              pill = 'TIGHT CONNECTION';
              meta = `${buffer} min buffer`;
              msg = `Route ${nextBus.route} arrives ${nextBus.pred_time}, train to ${matchedTrain.destination} departs ${matchedTrain.exp_depart} (${buffer} min buffer).`;
            } else {
              cls = 'hero-safe';
              pill = 'CONNECTION SAFE';
              meta = `${buffer} min buffer`;
              msg = `Route ${nextBus.route} arrives ${nextBus.pred_time}, train to ${matchedTrain.destination} departs ${matchedTrain.exp_depart} (${buffer} min buffer).`;
            }
          } else if (ghostBus) {
            cls = 'hero-risk';
            pill = 'GHOST BUS ALERT';
            meta = `Route ${ghostBus.route}`;
            const backupTxt = ghostBus.backup_bus ? ` Next option: ${ghostBus.backup_bus}.` : '';
            msg = `Route ${ghostBus.route} (${ghostBus.sched_time} to ${ghostBus.headsign}) shows no live GPS signal.${backupTxt}`;
          }
        } else if (ghostBus) {
          cls = 'hero-risk';
          pill = 'GHOST BUS ALERT';
          meta = `Route ${ghostBus.route}`;
          const backupTxt = ghostBus.backup_bus ? ` Next option: ${ghostBus.backup_bus}.` : '';
          msg = `Route ${ghostBus.route} (${ghostBus.sched_time} to ${ghostBus.headsign}) shows no live GPS signal.${backupTxt}`;
        }
      }

      box.className = `hero-banner ${cls}`;
      box.innerHTML = `
        <div class="hero-top">
          <span class="hero-pill">${pill}</span>
          <span class="hero-meta">${meta}</span>
        </div>
        <div class="hero-msg">${msg}</div>
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

      renderHeroBanner();
      const trainsBox = document.getElementById('trainsBox');
      trainsBox.innerHTML = latestData.trains.map((tr, idx) => `
        <div class="train-card ${idx === 0 ? 'train-primary' : 'train-secondary'}">
          ${idx === 0 ? '<div class="train-tag">Next Departure</div>' : ''}
          <div class="train-dest">To ${tr.destination}</div>
          <div class="train-meta">Dep ${tr.exp_depart} ? Due in <strong>${tr.due_in}m</strong> ${tr.late_mins > 0 ? `(+${tr.late_mins}m late)` : ''}</div>
        </div>
      `).join('') || '<div class="sub">No departing trains in window.</div>';

      const voted = getVoted();
      const seenTripsAll = new Set();
      const filteredBuses = latestData.buses.filter(b => {
        if (currentTab === 'all') {
          const tKey = `${b.service_date}|${b.trip_id}`;
          if (seenTripsAll.has(tKey)) return false;
          seenTripsAll.add(tKey);
          return true;
        }
        if (currentTab === 'outbound') return b.maynooth_flow === 'OUTBOUND' || b.maynooth_flow === 'THROUGH';
        if (currentTab === 'inbound') return b.maynooth_flow === 'INBOUND' || b.maynooth_flow === 'THROUGH';
        return b.stop_zone === currentTab;
      });

      const busesBox = document.getElementById('busesBox');
      try {
        updateCommuteMap(filteredBuses, (latestData && latestData.stops) ? latestData.stops : {});
      } catch (mapErr) {
        console.error('Map render error:', mapErr);
      }

      const boardableBuses = filteredBuses.filter(b => !(b.stop_role === 'TERMINUS' || b.pickup_type === '1'));
      const terminatingBuses = filteredBuses.filter(b => (b.stop_role === 'TERMINUS' || b.pickup_type === '1'));

      const renderBusCard = (b, isTerminating) => {
        const isCanceled = b.status === 'CANCELED';
        const delayRounded = Math.round(Number(b.delay_mins || 0));

        let etaHtml = '';
        if (isCanceled) {
          etaHtml = `<div class="eta-mins" style="color:var(--red)">Canceled</div><div class="eta-clock" style="text-decoration:line-through">Sched ${b.sched_time}</div>`;
        } else if (isTerminating) {
          etaHtml = `<div class="eta-mins" style="color:var(--muted)">Arriving</div><div class="eta-clock">Pred ${b.pred_time} (Sched ${b.sched_time})</div>`;
        } else if (delayRounded >= 2) {
          const delayColor = delayRounded >= 8 ? 'var(--red)' : 'var(--amber)';
          const minsTxt = b.mins_away <= 0 ? 'Due' : `${b.mins_away}m`;
          etaHtml = `<div class="eta-mins" style="color:${delayColor}">+${delayRounded} min</div><div class="eta-clock">Pred ${b.pred_time} (${minsTxt}) ? Sched ${b.sched_time}</div>`;
        } else {
          const minsTxt = b.mins_away <= 0 ? 'Due now' : `in ${b.mins_away} min`;
          etaHtml = `<div class="eta-mins">Pred ${b.pred_time}</div><div class="eta-clock">${minsTxt} ? Sched ${b.sched_time}</div>`;
        }

        const transferHtml = b.transfer_alert
          ? `<div class="transfer-box"><strong>Tight Train Transfer:</strong> ${b.transfer_alert.buffer_mins} min margin for ${b.transfer_alert.train_dep} train to ${b.transfer_alert.train_dest}${b.transfer_alert.backup_train ? `<br><em>${b.transfer_alert.backup_train}</em>` : ''}</div>`
          : '';

        const backupHtml = b.backup_bus
          ? `<div class="backup-box"><strong>Backup Option:</strong> ${b.backup_bus}</div>`
          : '';

        const compareHtml = buildTfiComparison(b);
        const distInfo = (b.distance_km !== null && b.distance_km !== undefined) ? `${b.distance_km} km from stop` : 'Distance unavailable';
        const histInfo = (b.history && (b.history.ghosts > 0 || b.history.delays > 0))
          ? `${b.history.ghosts} logged ghosts, ${b.history.delays} delays`
          : 'No prior disruptions logged';

        const vKey = `${b.service_date}|${b.trip_id}`;
        const userVote = voted[vKey];
        const arrivedDisabled = (isCanceled || userVote) ? 'disabled' : '';
        const ghostDisabled = (isTerminating || userVote) ? 'disabled' : '';

        return `
          <div class="bus-card ${isTerminating ? 'bus-terminating' : ''}" id="card-${b.trip_id}" onclick="focusBusOnMap('${b.trip_id}', false, ${b.bus_lat ?? 'null'}, ${b.bus_lon ?? 'null'})">
            <div class="bus-top">
              <div class="route-badge">${b.route}</div>
              <div class="bus-info">
                <div class="bus-head">${b.headsign || 'Maynooth Service'}</div>
                <div class="bus-stop">${b.stop_name} ? <strong>${b.operator}</strong></div>
              </div>
              <div class="eta">${etaHtml}</div>
            </div>
            <div class="tags">${statusBadge(b)}${ghostRiskBadge(b)}</div>
            ${compareHtml}
            ${transferHtml}
            ${backupHtml}
            ${b.diagnostic ? `<div class="diag-box ${['CANCELED','SILENT_GHOST_RISK','CROWD_CONFIRMED_GHOST','STALE_GPS','STALLED_GHOST'].includes(b.status) ? 'diag-bad' : (['HEAVY_DELAY','GPS_FAULT_VERIFIED'].includes(b.status) ? 'diag-warn' : '')}"><strong>Diagnostic:</strong> ${b.diagnostic}${(b.bus_lat !== null && b.bus_lon !== null) ? ` <span style="color:#38bdf8;font-weight:600;cursor:pointer;text-decoration:underline" onclick="event.stopPropagation();focusBusOnMap('${b.trip_id}', true, ${b.bus_lat ?? 'null'}, ${b.bus_lon ?? 'null'})">[Locate on Map]</span>` : ''}</div>` : ''}
            <details class="card-details" onclick="event.stopPropagation()">
              <summary>Telemetry & historical details</summary>
              <div class="card-details-body">
                <div><strong>Direction:</strong> ${b.maynooth_flow || 'THROUGH'} (${isTerminating ? 'Drop-off / Terminates here' : 'Boarding stop'})</div>
                <div><strong>Live Telemetry:</strong> ${distInfo} ? Scheduled ${b.sched_time}</div>
                <div><strong>Route History:</strong> ${histInfo}</div>
              </div>
            </details>
            <div class="verify-row" onclick="event.stopPropagation()">
              <button class="btn btn-ok ${userVote === 'ARRIVED' ? 'btn-voted' : ''}" ${arrivedDisabled}
                onclick="sendVerify('${b.trip_id}','${b.service_date}','${b.route}','${b.stop_name.replace(/'/g, "")}','${b.status}','ARRIVED')">
                ${userVote === 'ARRIVED' ? 'Voted Arrived' : 'Bus Arrived'} (${b.crowd.ARRIVED})
              </button>
              <button class="btn btn-bad ${userVote === 'GHOST' ? 'btn-voted' : ''}" ${ghostDisabled}
                onclick="sendVerify('${b.trip_id}','${b.service_date}','${b.route}','${b.stop_name.replace(/'/g, "")}','${b.status}','GHOST')">
                ${userVote === 'GHOST' ? 'Voted Ghost' : 'Ghost Bus'} (${b.crowd.GHOST})
              </button>
            </div>
          </div>
        `;
      };

      let finalHtml = boardableBuses.map(b => renderBusCard(b, false)).join('');
      if (!finalHtml) {
        finalHtml = '<div class="sub">No boardable buses scheduled in the next 75 minutes for this filter.</div>';
      }
      if (terminatingBuses.length > 0) {
        finalHtml += `<div class="term-divider">Arriving Only ? Terminates Here (Cannot Board)</div>`;
        finalHtml += terminatingBuses.map(b => renderBusCard(b, true)).join('');
      }
      busesBox.innerHTML = finalHtml;
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
  
    let commuteMap = null;
    let stopLayer = null;
    let busLayer = null;
    let focusLineLayer = null;
    let busMarkersById = {};
    let lastRenderedBuses = [];
    let lastRenderedStops = {};
    let mapVisible = (typeof window !== 'undefined' && window.innerWidth <= 640) ? false : true;

    function syncMapVisibilityUI() {
      const el = document.getElementById('commuteMap');
      const preview = document.getElementById('mapPreviewBar');
      const strip = document.getElementById('mapStatusStrip');
      const btn = document.getElementById('toggleMapBtn');
      if (!el || !btn) return;
      el.style.display = mapVisible ? 'block' : 'none';
      if (preview) preview.style.display = mapVisible ? 'none' : 'flex';
      if (!mapVisible && strip) strip.style.display = 'none';
      btn.textContent = mapVisible ? 'Hide Map' : 'Show Map';
    }

    function initCommuteMap() {
      syncMapVisibilityUI();
      if (!mapVisible) return;
      if (commuteMap || typeof L === 'undefined') return;
      const el = document.getElementById('commuteMap');
      if (!el) return;
      commuteMap = L.map('commuteMap', { zoomControl: true }).setView([53.3812, -6.5935], 14);
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 18,
        attribution: '? OpenStreetMap contributors'
      }).addTo(commuteMap);
      stopLayer = L.layerGroup().addTo(commuteMap);
      busLayer = L.layerGroup().addTo(commuteMap);
      focusLineLayer = L.layerGroup().addTo(commuteMap);
    }

    function resetMapView() {
      if (!mapVisible) {
        mapVisible = true;
        syncMapVisibilityUI();
      }
      initCommuteMap();
      if (focusLineLayer) focusLineLayer.clearLayers();
      const strip = document.getElementById('mapStatusStrip');
      if (strip) strip.style.display = 'none';
      if (commuteMap) {
        commuteMap.invalidateSize();
        commuteMap.setView([53.3812, -6.5935], 14);
      }
    }

    function toggleMapPanel() {
      mapVisible = !mapVisible;
      syncMapVisibilityUI();
      if (mapVisible) {
        initCommuteMap();
        if (commuteMap) {
          setTimeout(() => {
            commuteMap.invalidateSize();
            updateCommuteMap(lastRenderedBuses, lastRenderedStops);
          }, 90);
        }
      }
    }

    function getTargetStopCoords(bus) {
      const stopName = (bus && bus.stop_name ? bus.stop_name : '').toLowerCase();
      if (stopName.includes('campus') || stopName.includes('university')) {
        return [53.3846, -6.6006];
      }
      return [53.3779, -6.5897]; // Default to Maynooth Station hub
    }

    function updateMapBottomStrip(b) {
      const strip = document.getElementById('mapStatusStrip');
      if (!strip || !b) return;
      const etaPhrase = b.status === 'CANCELED'
        ? 'Canceled'
        : (b.mins_away <= 0 ? `Due at ${b.stop_name}` : `Arrives ${b.stop_name} in ${b.mins_away} min (Pred ${b.pred_time})`);
      strip.innerHTML = `<strong>Route ${b.route}</strong> ? ${b.headsign} ? <strong>${etaPhrase}</strong><div style="color:#94a3b8;margin-top:2px">${b.diagnostic || ''}</div>`;
      strip.style.display = 'block';
    }

    function drawBusSightline(lat, lon, bus) {
      if (!focusLineLayer) return;
      focusLineLayer.clearLayers();
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
      const targetCoords = getTargetStopCoords(bus);
      L.polyline([[lat, lon], targetCoords], {
        color: '#38bdf8',
        weight: 2.5,
        opacity: 0.85,
        dashArray: '5, 6'
      }).addTo(focusLineLayer);
    }

    function focusBusOnMap(tripId, scrollToMap = false, fallbackLat = null, fallbackLon = null) {
      document.querySelectorAll('.bus-card.card-highlight').forEach(c => c.classList.remove('card-highlight'));
      document.querySelectorAll('[id="card-' + tripId + '"]').forEach(c => c.classList.add('card-highlight'));

      if (!mapVisible) {
        mapVisible = true;
        syncMapVisibilityUI();
      }
      initCommuteMap();
      if (!commuteMap) return;

      if (scrollToMap) {
        const mapCard = document.getElementById('mapCard');
        if (mapCard) mapCard.scrollIntoView({ behavior: 'smooth', block: 'center' });
      }

      const matchedBus = (lastRenderedBuses || []).find(item => item.trip_id === tripId);

      const panAndOpen = () => {
        commuteMap.invalidateSize();
        updateCommuteMap(lastRenderedBuses, lastRenderedStops);
        const marker = busMarkersById[tripId];
        if (marker) {
          const ll = marker.getLatLng();
          commuteMap.setView(ll, 15, { animate: false });
          marker.openPopup();
          if (matchedBus) {
            drawBusSightline(ll.lat, ll.lng, matchedBus);
            updateMapBottomStrip(matchedBus);
          }
          return;
        }
        const lat = Number(fallbackLat);
        const lon = Number(fallbackLon);
        if (Number.isFinite(lat) && Number.isFinite(lon) && (lat !== 0 || lon !== 0)) {
          commuteMap.setView([lat, lon], 15, { animate: false });
          if (matchedBus) {
            drawBusSightline(lat, lon, matchedBus);
            updateMapBottomStrip(matchedBus);
          }
        }
      };

      setTimeout(panAndOpen, scrollToMap ? 320 : 60);
    }

    function updateCommuteMap(buses, stopsMap) {
      lastRenderedBuses = buses || [];
      lastRenderedStops = stopsMap || {};

      const gpsCount = lastRenderedBuses.filter(b => {
        const la = Number(b.bus_lat);
        const lo = Number(b.bus_lon);
        return Number.isFinite(la) && Number.isFinite(lo) && (la !== 0 || lo !== 0);
      }).length;

      const countEl = document.getElementById('mapLiveCount');
      if (countEl) countEl.textContent = `(${gpsCount} live GPS)`;
      const prevTxt = document.getElementById('mapPreviewText');
      if (prevTxt) prevTxt.textContent = `Map collapsed (${gpsCount} live GPS buses around Maynooth)`;

      initCommuteMap();
      if (!mapVisible || !commuteMap || !stopLayer || !busLayer) return;

      stopLayer.clearLayers();
      // Draw Maynooth Campus -> Town -> Station connection corridor (~8 min walk / 3 min bus)
      L.polyline([
        [53.3846, -6.6006],
        [53.3812, -6.5915],
        [53.3779, -6.5897]
      ], {
        color: '#64748b',
        weight: 3,
        opacity: 0.65,
        dashArray: '4, 6'
      }).bindPopup('<strong>Maynooth Connection Corridor</strong><br>Campus to Station (~8 min walk)').addTo(stopLayer);

      const entries = Object.entries(lastRenderedStops);
      if (entries.length > 0) {
        entries.forEach(([sid, st]) => {
          const sLat = st.stop_lat ?? st.lat;
          const sLon = st.stop_lon ?? st.lon;
          if (sLat == null || sLon == null) return;
          const isStn = (st.stop_name || '').toLowerCase().includes('station');
          const label = isStn ? 'Station' : 'Campus';
          const icon = L.divIcon({
            className: '',
            html: `<div class="stop-pin">${label}</div>`,
            iconSize: [52, 20],
            iconAnchor: [26, 10]
          });
          L.marker([sLat, sLon], { icon })
            .bindPopup(`<strong>${st.stop_name}</strong><br>Stop ID: ${sid}`)
            .addTo(stopLayer);
        });
      } else {
        L.marker([53.3779, -6.5897]).bindPopup('<strong>Maynooth Station</strong>').addTo(stopLayer);
        L.marker([53.3846, -6.6006]).bindPopup('<strong>MU Campus</strong>').addTo(stopLayer);
      }

      busLayer.clearLayers();
      busMarkersById = {};

      lastRenderedBuses.forEach(b => {
        const lat = Number(b.bus_lat);
        const lon = Number(b.bus_lon);
        if (!Number.isFinite(lat) || !Number.isFinite(lon) || (lat === 0 && lon === 0)) return;

        let bg = '#22c55e';
        const delayVal = Number(b.delay_mins || 0);
        if (['CANCELED', 'SILENT_GHOST_RISK', 'CROWD_CONFIRMED_GHOST', 'STALE_GPS', 'STALLED_GHOST'].includes(b.status)) {
          bg = '#ef4444';
        } else if (b.status === 'HEAVY_DELAY' || delayVal >= 4 || (b.gps_eta_mins !== null && b.gps_eta_mins >= b.mins_away + 5)) {
          bg = '#f59e0b';
        }

        const icon = L.divIcon({
          className: '',
          html: `<div class="bus-pin" style="background:${bg}">${b.route}</div>`,
          iconSize: [44, 22],
          iconAnchor: [22, 11]
        });

        const etaShort = b.status === 'CANCELED'
          ? 'Canceled'
          : (b.mins_away <= 0 ? `Due at ${b.stop_name}` : `Arrives ${b.stop_name} in ${b.mins_away}m`);
        const delayShort = delayVal >= 2 ? ` (+${Math.round(delayVal)}m)` : '';

        const popupHtml = `
          <div>
            <strong>${b.route}</strong> ? ${b.headsign || 'Maynooth'}<br>
            <span style="color:#93c5fd;font-weight:600">${etaShort}${delayShort}</span>
          </div>
        `;

        const marker = L.marker([lat, lon], { icon })
          .bindPopup(popupHtml, { maxWidth: 195, closeButton: true })
          .on('click', () => {
            document.querySelectorAll('.bus-card.card-highlight').forEach(c => c.classList.remove('card-highlight'));
            const card = document.getElementById('card-' + b.trip_id);
            if (card) {
              card.classList.add('card-highlight');
              card.scrollIntoView({ behavior: 'smooth', block: 'center' });
            }
            drawBusSightline(lat, lon, b);
            updateMapBottomStrip(b);
          })
          .addTo(busLayer);
        busMarkersById[b.trip_id] = marker;
      });
    }

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
    expired, status_msg = check_lookup_expiry()
    return jsonify({
        "ok": True,
        "time": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "feed_end_date": LOOKUP.get("feed_info", {}).get("feed_end_date", "unknown"),
        "lookup_expired": expired,
        "lookup_status": status_msg,
    })


@app.route("/api/refresh-gtfs", methods=["POST", "GET"])
def api_refresh_gtfs():
    force = request.args.get("force", "0") == "1"
    return jsonify(maybe_refresh_gtfs(force=force))


@app.route("/api/export/<filename>")
def api_export(filename: str):
    allowed = {"baseline_events.csv", "transfer_alerts.csv", "crowd_reports.csv", "daily_summary.csv"}
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
