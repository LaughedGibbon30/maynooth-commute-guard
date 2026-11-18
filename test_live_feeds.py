import json
import xml.etree.ElementTree as ET
from pathlib import Path
import requests
from google.transit import gtfs_realtime_pb2

NTA_API_KEY = "7328fbd4f7e5446b8775c45332c298a7"
VEHICLE_POSITIONS_URL = "https://api.nationaltransport.ie/gtfsr/v2/Vehicles"
TRIP_UPDATES_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
IRISH_RAIL_URL = (
    "http://api.irishrail.ie/realtime/realtime.asmx/"
    "getStationDataByNameXML?StationDesc=Maynooth"
)

LOOKUP_FILE = Path("data/maynooth_lookup.json")


def fetch_gtfs_rt(url: str) -> gtfs_realtime_pb2.FeedMessage:
    headers = {
        "x-api-key": NTA_API_KEY,
        "Cache-Control": "no-cache",
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return feed


def check_nta_feeds(lookup: dict):
    target_route_ids = set(lookup["routes"].keys())
    routes_meta = lookup["routes"]

    print("=== 1. NTA LIVE VEHICLE POSITIONS (GPS PINGS) ===")
    vp_feed = fetch_gtfs_rt(VEHICLE_POSITIONS_URL)
    active_buses = []
    for entity in vp_feed.entity:
        if not entity.HasField("vehicle"):
            continue
        veh = entity.vehicle
        rid = veh.trip.route_id
        if rid in target_route_ids:
            active_buses.append({
                "route": routes_meta[rid]["route_short_name"],
                "trip_id": veh.trip.trip_id,
                "vehicle_id": veh.vehicle.id,
                "lat": round(veh.position.latitude, 5),
                "lon": round(veh.position.longitude, 5),
            })

    print(f"Active Maynooth-bound/serving buses with live GPS right now: {len(active_buses)}")
    for bus in active_buses[:10]:
        print(
            f"  Route {bus['route']:<5} | Trip {bus['trip_id']} | "
            f"Bus {bus['vehicle_id']} @ ({bus['lat']}, {bus['lon']})"
        )

    print("\n=== 2. NTA LIVE TRIP UPDATES (DELAYS / CANCELLATIONS) ===")
    tu_feed = fetch_gtfs_rt(TRIP_UPDATES_URL)
    matched_updates = 0
    for entity in tu_feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        rid = tu.trip.route_id
        if rid in target_route_ids:
            matched_updates += 1
            rel = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(
                tu.trip.schedule_relationship
            )
            if matched_updates <= 8:
                print(
                    f"  Route {routes_meta[rid]['route_short_name']:<5} | "
                    f"Trip {tu.trip.trip_id} | Status: {rel}"
                )
    print(f"Total active TripUpdates for Maynooth routes: {matched_updates}")


def check_irish_rail():
    print("\n=== 3. IRISH RAIL LIVE DEPARTURES (MAYNOOTH STATION) ===")
    resp = requests.get(IRISH_RAIL_URL, timeout=15)
    resp.raise_for_status()

    root = ET.fromstring(resp.text)
    ns = {"ir": "http://api.irishrail.ie/realtime/"}
    trains = root.findall("ir:objStationData", ns)

    if not trains:
        print("No trains currently listed at Maynooth Station.")
        return

    for tr in trains[:8]:
        dest = tr.findtext("ir:Destination", default="Unknown", namespaces=ns)
        due_in = tr.findtext("ir:Duein", default="?", namespaces=ns)
        exp_depart = tr.findtext("ir:Expdepart", default="?", namespaces=ns)
        late = tr.findtext("ir:Late", default="0", namespaces=ns)
        print(
            f"  To {dest:<22} | Due in {due_in:>2} min | "
            f"Exp Depart: {exp_depart} | Late: {late} min"
        )


def main():
    with LOOKUP_FILE.open("r", encoding="utf-8") as f:
        lookup = json.load(f)

    check_nta_feeds(lookup)
    check_irish_rail()


if __name__ == "__main__":
    main()
