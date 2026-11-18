import csv
import json
from pathlib import Path

GTFS_DIR = Path("gtfs_static")
OUTPUT_FILE = Path("data/maynooth_lookup.json")


def read_csv(filename: str):
    path = GTFS_DIR / filename
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def main():
    agencies = {
        row["agency_id"]: row.get("agency_name", "").strip()
        for row in read_csv("agency.txt")
    }

    feed_info = read_csv("feed_info.txt")
    feed_expiry = feed_info[0].get("feed_end_date", "unknown") if feed_info else "unknown"

    all_routes = {}
    for row in read_csv("routes.txt"):
        aid = row.get("agency_id", "")
        all_routes[row["route_id"]] = {
            "route_short_name": row.get("route_short_name", "").strip(),
            "route_long_name": row.get("route_long_name", "").strip(),
            "route_type": row.get("route_type", "").strip(),
            "agency_id": aid,
            "agency_name": agencies.get(aid, "Unknown"),
        }

    maynooth_stops = {}
    for row in read_csv("stops.txt"):
        stop_name = row.get("stop_name", "").strip()
        if "maynooth" in stop_name.lower():
            maynooth_stops[row["stop_id"]] = {
                "stop_name": stop_name,
                "stop_code": row.get("stop_code", "").strip(),
                "lat": float(row["stop_lat"]) if row.get("stop_lat") else None,
                "lon": float(row["stop_lon"]) if row.get("stop_lon") else None,
            }

    all_trips = {}
    for row in read_csv("trips.txt"):
        rid = row.get("route_id", "")
        if rid in all_routes:
            all_trips[row["trip_id"]] = {
                "route_id": rid,
                "route_short_name": all_routes[rid]["route_short_name"],
                "route_type": all_routes[rid]["route_type"],
                "agency_name": all_routes[rid]["agency_name"],
                "service_id": row.get("service_id", ""),
                "trip_headsign": row.get("trip_headsign", "").strip(),
                "direction_id": row.get("direction_id", ""),
            }

    trip_origin_times = {}
    trip_max_seq = {}
    raw_maynooth_rows = []
    active_route_ids = set()
    active_trip_ids = set()
    active_service_ids = set()

    stop_times_path = GTFS_DIR / "stop_times.txt"
    with stop_times_path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            tid = row.get("trip_id", "")
            if tid not in all_trips:
                continue

            seq = int(row.get("stop_sequence", 0))
            if seq == 1:
                trip_origin_times[tid] = row.get("departure_time", "")
            if seq > trip_max_seq.get(tid, 0):
                trip_max_seq[tid] = seq

            sid = row.get("stop_id", "")
            if sid in maynooth_stops and all_trips[tid]["route_type"] == "3":
                rid = all_trips[tid]["route_id"]
                svid = all_trips[tid]["service_id"]
                active_route_ids.add(rid)
                active_trip_ids.add(tid)
                active_service_ids.add(svid)
                raw_maynooth_rows.append((tid, rid, svid, sid, seq, row))

    maynooth_stop_times = []
    for tid, rid, svid, sid, seq, row in raw_maynooth_rows:
        max_seq = trip_max_seq.get(tid, seq)
        if seq == 1:
            role = "ORIGIN"
        elif seq == max_seq:
            role = "TERMINUS"
        else:
            role = "THROUGH"

        maynooth_stop_times.append({
            "trip_id": tid,
            "route_id": rid,
            "route_short_name": all_trips[tid]["route_short_name"],
            "agency_name": all_trips[tid]["agency_name"],
            "headsign": all_trips[tid]["trip_headsign"],
            "service_id": svid,
            "stop_id": sid,
            "stop_name": maynooth_stops[sid]["stop_name"],
            "stop_role": role,
            "arrival_time": row.get("arrival_time", ""),
            "departure_time": row.get("departure_time", ""),
            "origin_departure_time": trip_origin_times.get(tid, ""),
            "stop_sequence": seq,
        })

    routes = {rid: all_routes[rid] for rid in active_route_ids}
    trips = {tid: all_trips[tid] for tid in active_trip_ids}

    calendar = {}
    for row in read_csv("calendar.txt"):
        svid = row.get("service_id", "")
        if svid in active_service_ids:
            calendar[svid] = {
                "days": [
                    int(row.get("monday", 0)),
                    int(row.get("tuesday", 0)),
                    int(row.get("wednesday", 0)),
                    int(row.get("thursday", 0)),
                    int(row.get("friday", 0)),
                    int(row.get("saturday", 0)),
                    int(row.get("sunday", 0)),
                ],
                "start_date": row.get("start_date", ""),
                "end_date": row.get("end_date", ""),
            }

    calendar_dates = {}
    for row in read_csv("calendar_dates.txt"):
        svid = row.get("service_id", "")
        if svid in active_service_ids:
            calendar_dates.setdefault(svid, []).append({
                "date": row.get("date", ""),
                "exception_type": row.get("exception_type", ""),
            })

    payload = {
        "feed_end_date": feed_expiry,
        "routes": routes,
        "maynooth_stops": maynooth_stops,
        "trips": trips,
        "maynooth_stop_times": maynooth_stop_times,
        "calendar": calendar,
        "calendar_dates": calendar_dates,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Feed valid through: {feed_expiry}")
    print(f"Found {len(routes)} realtime-enabled Maynooth bus routes:")
    for rid, info in sorted(routes.items(), key=lambda x: x[1]["route_short_name"]):
        print(f"  - {info['route_short_name']:<6} | {info['agency_name']:<22} | {info['route_long_name']}")
    print(f"Saved {len(maynooth_stop_times)} stop-time entries across {len(trips)} trips.")


if __name__ == "__main__":
    main()
