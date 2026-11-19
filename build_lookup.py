import csv
import gc
import io
import json
import os
import tempfile
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

GTFS_REALTIME_ZIP_URL = "https://www.transportforireland.ie/transitData/Data/GTFS_Realtime.zip"
LOOKUP_FILE = Path("data/maynooth_lookup.json")
TZ = ZoneInfo("Europe/Dublin")

KNOWN_MAYNOOTH_STOPS = {
    "8260DB003917",
    "8260DB007779",
    "8260B1354601",
    "8260B1031601",
    "8260B1034301",
    "8260PB10049",
}


CORE_RT_OPERATORS = (
    "dublin bus",
    "bus \u00e1tha cliath",
    "bus atha cliath",
    "go-ahead",
    "bus \u00e9ireann",
    "bus eireann",
)
UNTRACKED_ROUTE_CODES = {"22"}


def is_core_rt_operator(agency_name: str, route_short_name: str = "") -> bool:
    if route_short_name.strip().upper() in UNTRACKED_ROUTE_CODES:
        return False
    low = agency_name.strip().lower()
    return any(tok in low for tok in CORE_RT_OPERATORS)


def read_zip_csv(zf: zipfile.ZipFile, filename: str):
    with zf.open(filename) as raw:
        wrapper = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        for row in csv.DictReader(wrapper):
            yield row


def check_lookup_expiry(
    lookup_path: Path = LOOKUP_FILE,
    expiry_buffer_days: int = 1,
    max_age_days: int = 3,
) -> tuple[bool, str]:
    if not lookup_path.exists() or lookup_path.stat().st_size == 0:
        return True, "maynooth_lookup.json does not exist yet"

    try:
        data = json.loads(lookup_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return True, f"Invalid lookup JSON ({exc})"

    feed_info = data.get("feed_info", {})
    now = datetime.now(TZ)
    today_str = now.strftime("%Y%m%d")
    cutoff_str = (now + timedelta(days=expiry_buffer_days)).strftime("%Y%m%d")

    feed_end = str(feed_info.get("feed_end_date", "")).strip()
    if not feed_end:
        cal_ends = [
            str(v.get("end_date", "")).strip()
            for v in data.get("calendar", {}).values()
            if v.get("end_date")
        ]
        feed_end = max(cal_ends) if cal_ends else ""

    if not feed_end:
        return True, "Missing feed_end_date metadata (legacy lookup format)"

    if cutoff_str >= feed_end:
        return True, f"Feed expired or expiring soon (feed_end_date={feed_end}, today={today_str})"

    built_at_str = feed_info.get("built_at", "")
    if built_at_str:
        try:
            built_dt = datetime.strptime(built_at_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
            age_days = (now - built_dt).total_seconds() / 86400.0
            if age_days >= max_age_days:
                return True, f"Daily TFI refresh due (built {age_days:.1f} days ago; feed_end_date={feed_end})"
        except ValueError:
            pass

    return False, f"Valid until {feed_end} (built {built_at_str or 'unknown'})"


def build_from_zip_path(zip_path: Path, output_path: Path = LOOKUP_FILE) -> dict:
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = set(zf.namelist())

        # 1. Read feed_info.txt
        feed_info = {}
        if "feed_info.txt" in names:
            for row in read_zip_csv(zf, "feed_info.txt"):
                feed_info = {
                    "feed_publisher_name": row.get("feed_publisher_name", "").strip(),
                    "feed_start_date": row.get("feed_start_date", "").strip(),
                    "feed_end_date": row.get("feed_end_date", "").strip(),
                    "feed_version": row.get("feed_version", "").strip(),
                }
                break

        # 2. Identify Maynooth stops & coordinates
        maynooth_stops: dict[str, dict] = {}
        for row in read_zip_csv(zf, "stops.txt"):
            sid = row.get("stop_id", "").strip()
            sname = row.get("stop_name", "").strip()
            if sid in KNOWN_MAYNOOTH_STOPS or "maynooth" in sname.lower():
                try:
                    lat = float(row["stop_lat"]) if row.get("stop_lat") else None
                    lon = float(row["stop_lon"]) if row.get("stop_lon") else None
                except ValueError:
                    lat, lon = None, None
                maynooth_stops[sid] = {
                    "stop_name": sname,
                    "stop_lat": lat,
                    "stop_lon": lon,
                }

        # 3. Pass 1 over stop_times.txt: collect trip_ids touching Maynooth (low RAM)
        maynooth_trip_ids: set[str] = set()
        for row in read_zip_csv(zf, "stop_times.txt"):
            if row.get("stop_id", "").strip() in maynooth_stops:
                tid = row.get("trip_id", "").strip()
                if tid:
                    maynooth_trip_ids.add(tid)

        # 4. Read trips, routes, agencies, and calendars filtered to Maynooth trips
        trips_meta: dict[str, dict] = {}
        needed_route_ids: set[str] = set()
        needed_service_ids: set[str] = set()
        for row in read_zip_csv(zf, "trips.txt"):
            tid = row.get("trip_id", "").strip()
            if tid in maynooth_trip_ids:
                rid = row.get("route_id", "").strip()
                svid = row.get("service_id", "").strip()
                needed_route_ids.add(rid)
                needed_service_ids.add(svid)
                trips_meta[tid] = {
                    "route_id": rid,
                    "service_id": svid,
                    "headsign": row.get("trip_headsign", "").strip(),
                    "direction_id": row.get("direction_id", "").strip(),
                }

        agencies: dict[str, str] = {}
        if "agency.txt" in names:
            for row in read_zip_csv(zf, "agency.txt"):
                aid = row.get("agency_id", "").strip()
                agencies[aid] = row.get("agency_name", "").strip()

        routes_meta: dict[str, dict] = {}
        for row in read_zip_csv(zf, "routes.txt"):
            rid = row.get("route_id", "").strip()
            if rid in needed_route_ids:
                aid = row.get("agency_id", "").strip()
                rtype = row.get("route_type", "3").strip()
                short_name = row.get("route_short_name", "").strip() or row.get("route_long_name", "").strip()
                if rtype == "2" or aid == "IR" or short_name.lower() == "rail":
                    continue
                aname = agencies.get(aid, "")
                routes_meta[rid] = {
                    "route_short_name": short_name,
                    "agency_id": aid,
                    "agency_name": aname,
                    "gtfs_rt_expected": is_core_rt_operator(aname, short_name),
                }

        calendar: dict[str, dict] = {}
        if "calendar.txt" in names:
            day_cols = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
            for row in read_zip_csv(zf, "calendar.txt"):
                svid = row.get("service_id", "").strip()
                if svid in needed_service_ids:
                    calendar[svid] = {
                        "start_date": row.get("start_date", "").strip(),
                        "end_date": row.get("end_date", "").strip(),
                        "days": [int(row.get(d, "0") or 0) for d in day_cols],
                    }

        calendar_dates: dict[str, list] = {}
        if "calendar_dates.txt" in names:
            for row in read_zip_csv(zf, "calendar_dates.txt"):
                svid = row.get("service_id", "").strip()
                if svid in needed_service_ids:
                    calendar_dates.setdefault(svid, []).append({
                        "date": row.get("date", "").strip(),
                        "exception_type": row.get("exception_type", "").strip(),
                    })

        # 5. Pass 2 over stop_times.txt: extract origin/terminus bounds & Maynooth stop rows
        trip_bounds: dict[str, dict] = {}
        maynooth_rows: list[dict] = []

        for row in read_zip_csv(zf, "stop_times.txt"):
            tid = row.get("trip_id", "").strip()
            if tid not in maynooth_trip_ids:
                continue

            seq = int(row.get("stop_sequence", "0") or 0)
            arr = row.get("arrival_time", "").strip()
            dep = row.get("departure_time", "").strip()
            sid = row.get("stop_id", "").strip()

            b = trip_bounds.get(tid)
            if b is None:
                trip_bounds[tid] = {
                    "min_seq": seq,
                    "max_seq": seq,
                    "origin_dep": dep or arr,
                }
            else:
                if seq < b["min_seq"]:
                    b["min_seq"] = seq
                    b["origin_dep"] = dep or arr
                if seq > b["max_seq"]:
                    b["max_seq"] = seq

            if sid in maynooth_stops:
                maynooth_rows.append({
                    "trip_id": tid,
                    "stop_id": sid,
                    "stop_sequence": seq,
                    "arrival_time": arr,
                    "departure_time": dep or arr,
                    "pickup_type": row.get("pickup_type", "0").strip(),
                    "drop_off_type": row.get("drop_off_type", "0").strip(),
                })

    # 6. Assemble final lookup records
    stop_times_out: list[dict] = []
    for item in maynooth_rows:
        tid = item["trip_id"]
        tmeta = trips_meta.get(tid)
        if not tmeta:
            continue
        rmeta = routes_meta.get(tmeta["route_id"])
        if not rmeta:
            continue
        bounds = trip_bounds[tid]
        seq = item["stop_sequence"]

        if seq == bounds["min_seq"]:
            role = "ORIGIN"
        elif seq == bounds["max_seq"]:
            role = "TERMINUS"
        else:
            role = "THROUGH"

        pickup_type = item["pickup_type"] or "0"
        drop_off_type = item["drop_off_type"] or "0"
        seq_span = max(1, bounds["max_seq"] - bounds["min_seq"])
        rel_pos = (seq - bounds["min_seq"]) / seq_span

        if role == "ORIGIN" or drop_off_type == "1" or rel_pos <= 0.25:
            maynooth_flow = "OUTBOUND"
        elif role == "TERMINUS" or pickup_type == "1" or rel_pos >= 0.75:
            maynooth_flow = "INBOUND"
        else:
            maynooth_flow = "THROUGH"

        smeta = maynooth_stops[item["stop_id"]]
        stop_times_out.append({
            "trip_id": tid,
            "route_id": tmeta["route_id"],
            "route_short_name": rmeta.get("route_short_name", ""),
            "agency_id": rmeta.get("agency_id", ""),
            "agency_name": rmeta.get("agency_name", ""),
            "gtfs_rt_expected": rmeta.get("gtfs_rt_expected", True),
            "headsign": tmeta["headsign"],
            "direction_id": tmeta["direction_id"],
            "service_id": tmeta["service_id"],
            "stop_id": item["stop_id"],
            "stop_name": smeta["stop_name"],
            "stop_sequence": seq,
            "stop_role": role,
            "maynooth_flow": maynooth_flow,
            "pickup_type": pickup_type,
            "drop_off_type": drop_off_type,
            "arrival_time": item["arrival_time"],
            "departure_time": item["departure_time"],
            "origin_departure_time": bounds["origin_dep"],
        })

    feed_info["built_at"] = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    feed_info["source_url"] = GTFS_REALTIME_ZIP_URL

    agencies_summary: dict[str, dict] = {}
    for st in stop_times_out:
        aid = st["agency_id"] or "unknown"
        entry = agencies_summary.setdefault(
            aid,
            {
                "agency_name": st["agency_name"] or "Unknown Operator",
                "gtfs_rt_expected": st["gtfs_rt_expected"],
                "routes": set(),
            },
        )
        if st["route_short_name"]:
            entry["routes"].add(st["route_short_name"])

    for aid, entry in agencies_summary.items():
        entry["routes"] = sorted(entry["routes"])

    payload = {
        "feed_info": feed_info,
        "agencies": agencies_summary,
        "stops": maynooth_stops,
        "calendar": calendar,
        "calendar_dates": calendar_dates,
        "maynooth_stop_times": stop_times_out,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = output_path.with_suffix(".json.tmp")
    tmp_out.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp_out, output_path)
    gc.collect()
    return payload


def refresh_lookup_if_needed(force: bool = False) -> dict:
    expired, reason = check_lookup_expiry()
    if not force and not expired:
        return {"refreshed": False, "reason": reason}

    print(f"[GTFS] Downloading fresh GTFS_Realtime.zip ({reason})...")
    tmp_zip = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tf:
            tmp_zip = Path(tf.name)
            headers = {"User-Agent": "MaynoothConnectionGuard/1.0"}
            with requests.get(GTFS_REALTIME_ZIP_URL, headers=headers, stream=True, timeout=(10, 60)) as resp:
                resp.raise_for_status()
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        tf.write(chunk)

        payload = build_from_zip_path(tmp_zip, LOOKUP_FILE)
        return {
            "refreshed": True,
            "reason": reason,
            "feed_info": payload.get("feed_info", {}),
            "stops_count": len(payload.get("stops", {})),
            "stop_times_count": len(payload.get("maynooth_stop_times", [])),
        }
    finally:
        if tmp_zip and tmp_zip.exists():
            try:
                tmp_zip.unlink()
            except OSError:
                pass


if __name__ == "__main__":
    result = refresh_lookup_if_needed(force=True)
    print(json.dumps(result, indent=2))
