# Maynooth Commute Reliability & Connection Guard — Project State

## Goal & Target
- **Competition:** NTA Smarter Travel Student Awards (Semester 2 Deadline: April 1, 2027) + NTA Summer Internship application.
- **Problem:** "Ghost buses" (trips vanishing without an official CANCELED status) and missed bus-to-train transfers at Maynooth Station pushing MU students to drive or miss 9 AM labs.
- **Directory:** `C:\Users\nivis\maynooth-commute-guard` (Python `.venv` with `requests`, `gtfs-realtime-bindings`, `protobuf`).

## Completed Work
1. **Baseline Student Survey (Microsoft Forms):** Created 5-question anonymous survey; 22+ initial responses collected (confirmed routes 66/67 are retired; active corridors are C3, C4, W6, 52, 115, X-routes, and Irish Rail).
2. **Phase 1 (`build_lookup.py` & `test_live_feeds.py`):**
   - Filtered TFI static GTFS down to 14 Maynooth stops (Campus: `8260B1031601`, `8260B1034301`, `8260PB10049`; Station Bus Stops: `8260DB003917`, `8260DB007779`, `8260B1354601`; Irish Rail Maynooth: `8260IR0058`) and 17 active bus routes across 3,790 trips -> saved to `data/maynooth_lookup.json`.
   - Verified live NTA GTFS-Realtime v2 feeds (`Vehicles` and `TripUpdates`) and Irish Rail XML departures (`getStationDataByNameXML?StationDesc=Maynooth`).
3. **Phase 2 Draft (`commute_guard_logger.py`):**
   - Added `calendar.txt` and `calendar_dates.txt` service-day filtering, `SILENT_GHOST_RISK` vs `OFFICIAL_CANCELED` detection, and Maynooth Station bus-to-train transfer buffer calculation (`WALK_BUFFER_MINS = 3`, `MIN_SAFE_TRANSFER_MINS = 5`) logging to `data/baseline_events.csv` and `data/transfer_alerts.csv`.

## Planned Upgrades & Next Steps
- **Phase 1 Hardening:** Switch static source to TFI `GTFS_Realtime.zip`, check `feed_info.txt` expiry, tag operators via `agency.txt`, label Inbound vs. Outbound direction, and move `NTA_API_KEY` to `.env`.
- **Phase 2 Data Run:** Log 5+ weekdays of peak commute telemetry (7:30–10:30 AM, 4:00–6:30 PM) to quantify baseline ghost-bus and missed-transfer rates.
- **Phase 3 Build:** Lightweight mobile web dashboard / 1-tap crowd-verification ("Bus Arrived" / "Ghost Bus") for a 2–3 week MU student pilot in Semester 2.
