"""
MarktWaarde — Live Busyness Monitor
-----------------------------------
Polls the BestTime Live API for a fixed registry of venues and appends each
reading as a row to a CSV log. Designed to run unattended on GitHub Actions.

Required: an environment variable BESTTIME_API_KEY (set it as a GitHub
Actions Secret named BESTTIME_API_KEY).
"""

import os
import csv
import time
import requests
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# 1. Configuration
# ---------------------------------------------------------------------------
API_KEY = os.environ.get("BESTTIME_API_KEY")  # injected by GitHub Secrets
LIVE_API_URL = "https://besttime.app/api/v1/forecasts/live"
LOG_FILE = "marktwaarde_live_log.csv"
FIELDNAMES = [
    "timestamp", "venue_id", "venue_name", "city", "venue_type",
    "venue_lat", "venue_lng",
    "live_available", "live_busyness", "normal_busyness", "busyness_delta",
]

# ---------------------------------------------------------------------------
# 2. Hardcoded venue registry (polled by venue_id only)
# ---------------------------------------------------------------------------
MONITORED_VENUES = [
    # --- Heist-op-den-Berg ---------------------------------------------------
    {"venue_id": "ven_3439594f74754357745555526355775a5a4f535a482d754a496843", "city": "Heist-op-den-Berg", "venue_name": "'t Hoekske",                  "venue_type": "BEER",             "venue_address": "Bergstraat 2 2220 Heist-op-den-Berg Belgium",        "venue_lat": 51.0753,    "venue_lng": 4.7270},
    {"venue_id": "ven_3032585955325a44424645526355775a427444496a34364a496843", "city": "Heist-op-den-Berg", "venue_name": "ALDI",                        "venue_type": "SHOPPING",         "venue_address": "Lostraat 41 2220 Heist-op-den-Berg Belgium",        "venue_lat": 51.0697,    "venue_lng": 4.7348},
    {"venue_id": "ven_38326c7559424753424f2d526355775a3164754e4a615a4a496843", "city": "Heist-op-den-Berg", "venue_name": "Staminee The Living",         "venue_type": "CAFE",             "venue_address": "Noordstraat 2 2220 Heist-op-den-Berg Belgium",       "venue_lat": 51.0775,    "venue_lng": 4.7214},
    {"venue_id": "ven_776579784a6f697448492d526355775a5a4f4f784258524a496843", "city": "Heist-op-den-Berg", "venue_name": "'t Pleintje",                 "venue_type": "BEER",             "venue_address": "Oude Godstraat 2/16 2220 Heist-op-den-Berg Belgium", "venue_lat": 51.0749,    "venue_lng": 4.7270},
    {"venue_id": "ven_51746135563255664b5271526355775a78746a2d7a546d4a496843", "city": "Heist-op-den-Berg", "venue_name": "Theatercafé Het 3de Bedrijf", "venue_type": "RESTAURANT",       "venue_address": "Cultuurplein 2220 Heist-op-den-Berg Belgium",        "venue_lat": 51.0761,    "venue_lng": 4.7174},
    {"venue_id": "ven_55456c466e72456b786747526355775a786437364633504a496843", "city": "Heist-op-den-Berg", "venue_name": "Punch",                       "venue_type": "RESTAURANT",       "venue_address": "Kattestraat 6 2220 Heist-op-den-Berg Belgium",       "venue_lat": 51.0759,    "venue_lng": 4.7183},
    {"venue_id": "ven_7764476568304264307061526355775a314e667a5952314a496843", "city": "Heist-op-den-Berg", "venue_name": "Domino's",                    "venue_type": "PIZZA_RESTAURANT", "venue_address": "Bergstraat 68 2220 Heist-op-den-Berg Belgium",       "venue_lat": 51.0761,    "venue_lng": 4.7230},
    {"venue_id": "ven_6b3979673436346d627131526355775a78456d7274367a4a496843", "city": "Heist-op-den-Berg", "venue_name": "Belchicken",                  "venue_type": "FAST_FOOD",        "venue_address": "Bergstraat 99 2220 Heist-op-den-Berg Belgium",       "venue_lat": 51.0764,    "venue_lng": 4.7208},
    {"venue_id": "ven_457334345570636c34616b526355775a78395a4f5253694a496843", "city": "Heist-op-den-Berg", "venue_name": "Carrefour market",            "venue_type": "SUPERMARKET",      "venue_address": "Bergstraat 140 2220 Heist-op-den-Berg Belgium",      "venue_lat": 51.0770,    "venue_lng": 4.7186},
    {"venue_id": "ven_675175417454594e4d5464526355775a642d6c327a35344a496843", "city": "Heist-op-den-Berg", "venue_name": "Hema",                        "venue_type": "SHOPPING_CENTER",  "venue_address": "Bergstraat 15 2220 Heist-op-den-Berg Belgium",       "venue_lat": 51.0752,    "venue_lng": 4.7259},

    # --- Brecht --------------------------------------------------------------
    {"venue_id": "ven_63584a33535a386876304152634578434944753752634e4a496843", "city": "Brecht", "venue_name": "Carré Confituur", "venue_type": "BREAKFAST_RESTAURANT", "venue_address": "Gemeenteplaats 15 2960 Brecht Belgium", "venue_lat": 51.3509098, "venue_lng": 4.6430454},

    # --- Herentals -----------------------------------------------------------
    {"venue_id": "ven_6f686f63526b39454a527a52635577535a6b62355f586d4a496843", "city": "Herentals", "venue_name": "De Repertoire",              "venue_type": "RESTAURANT",           "venue_address": "Poederleeseweg 15 2200 Herentals Belgium",    "venue_lat": 51.1886606, "venue_lng": 4.8331029},
    {"venue_id": "ven_4d47796171386446706e7652635577536c7a6c544e5f6e4a496843", "city": "Herentals", "venue_name": "Colruyt",                    "venue_type": "SUPERMARKET",          "venue_address": "Belgiëlaan 42 2200 Herentals Belgium",        "venue_lat": 51.1804452, "venue_lng": 4.8329995},
    {"venue_id": "ven_6f7963595561722d49436a52635577536c44566d6432644a496843", "city": "Herentals", "venue_name": "Lidl",                       "venue_type": "SUPERMARKET",          "venue_address": "Augustijnenlaan 4 2200 Herentals Belgium",    "venue_lat": 51.17956,   "venue_lng": 4.83662},
    {"venue_id": "ven_496d5a3231313065434c4a5263557753787a65624c30354a496843", "city": "Herentals", "venue_name": "Delhaize Herentals",         "venue_type": "SUPERMARKET",          "venue_address": "Grote Markt 46 2200 Herentals Belgium",       "venue_lat": 51.1777125, "venue_lng": 4.8361567},
    {"venue_id": "ven_733968684f505a7432556e5263557753746a70535757364a496843", "city": "Herentals", "venue_name": "Coffee & Sweets",            "venue_type": "BREAKFAST_RESTAURANT", "venue_address": "Zandstraat 39 2200 Herentals Belgium",        "venue_lat": 51.1768238, "venue_lng": 4.8334178},
    {"venue_id": "ven_5556595f6a317a383872655263557753467850386879714a496843", "city": "Herentals", "venue_name": "McDonald's",                 "venue_type": "FAST_FOOD",            "venue_address": "Augustijnenlaan 97 2200 Herentals Belgium",   "venue_lat": 51.1775703, "venue_lng": 4.8498434},
    {"venue_id": "ven_6b6d636e55315539454b6b52635577537454673167526b4a496843", "city": "Herentals", "venue_name": "Carrefour Market Herentals", "venue_type": "SUPERMARKET",          "venue_address": "Noorderwijksebaan 1 2200 Herentals Belgium",  "venue_lat": 51.1708422, "venue_lng": 4.8346631},

    # --- Mechelen ------------------------------------------------------------
    {"venue_id": "ven_595a766d4c6a7058554641526330776c5f634f5953586e4a496843", "city": "Mechelen", "venue_name": "Antverpia - Mechelen",  "venue_type": "CAFE",        "venue_address": "Korenmarkt 2 2800 Mechelen Belgium",          "venue_lat": 51.025177,  "venue_lng": 4.4763284},
    {"venue_id": "ven_49654576387a7549752d4c526330776b6e552d384f31744a496843", "city": "Mechelen", "venue_name": "Sima Supermarket",     "venue_type": "GROCERY",     "venue_address": "Nekkerspoelstraat 115 2800 Mechelen Belgium",  "venue_lat": 51.0311163, "venue_lng": 4.494569},
    {"venue_id": "ven_7743663138717049544845526330776c6a63577468536d4a496843", "city": "Mechelen", "venue_name": "KUUB",                 "venue_type": "BEER",        "venue_address": "Minderbroedersgang 3A 2800 Mechelen Belgium",  "venue_lat": 51.0287722, "venue_lng": 4.4771761},
    {"venue_id": "ven_514564675944475941556a526330776c447332714e7a6c4a496843", "city": "Mechelen", "venue_name": "Bio-Planet Mechelen",       "venue_type": "SUPERMARKET",     "venue_address": "Battelsesteenweg 68 2800 Mechelen Belgium",    "venue_lat": 51.0294781, "venue_lng": 4.4681674},
    {"venue_id": "ven_6737487775395a534a4163526330776c5051772d7133534a496843", "city": "Mechelen", "venue_name": "Albert Heijn - XL Mechelen", "venue_type": "SUPERMARKET",     "venue_address": "Nora Tilleylaan 40 2800 Mechelen Belgium",     "venue_lat": 51.0389836, "venue_lng": 4.4616372},
    {"venue_id": "ven_4132445478366431467567526330776c723837515363714a496843", "city": "Mechelen", "venue_name": "D’Hanekeef Café",            "venue_type": "CAFE",            "venue_address": "Keizerstraat 8 2800 Mechelen Belgium",         "venue_lat": 51.0288269, "venue_lng": 4.4853303},
    {"venue_id": "ven_3866784663684f6d724142526330776c4441567a457a6f4a496843", "city": "Mechelen", "venue_name": "Brasserie Mavue",            "venue_type": "RESTAURANT",      "venue_address": "Vijfhoek 2 2800 Mechelen Belgium",             "venue_lat": 51.0229446, "venue_lng": 4.4820301},
    {"venue_id": "ven_6734727051584452726458526330776b7230636e3236584a496843", "city": "Mechelen", "venue_name": "Kruidtuin Mechelen",         "venue_type": "PARK",            "venue_address": "Lange Schipstraat 2800 Mechelen Belgium",      "venue_lat": 51.0233971, "venue_lng": 4.4854511},
    {"venue_id": "ven_594e746b55567851727442526330776c6a6d51615778664a496843", "city": "Mechelen", "venue_name": "Mimi",                       "venue_type": "COFFEE",          "venue_address": "Bruul 131 2800 Mechelen Belgium",              "venue_lat": 51.0233542, "venue_lng": 4.4821945},
    {"venue_id": "ven_7344392d3535754d727137526330776c446f52706b374b4a496843", "city": "Mechelen", "venue_name": "Lidl",                       "venue_type": "SUPERMARKET",     "venue_address": "Nora Tilleylaan 2 2800 Mechelen Belgium",      "venue_lat": 51.04047,   "venue_lng": 4.45942},
    {"venue_id": "ven_5167474475307275703531526330776c6a4d55327751524a496843", "city": "Mechelen", "venue_name": "Saint Rumbold's Cathedral",  "venue_type": "CHURCH",          "venue_address": "Onder-Den-Toren 20a 2800 Mechelen Belgium",    "venue_lat": 51.0288042, "venue_lng": 4.4777973},
    {"venue_id": "ven_495f4949576a7a61746949526330776c6e3864704e375a4a496843", "city": "Mechelen", "venue_name": "Grote Markt",                "venue_type": "OTHER",           "venue_address": "Grote Markt 2800 Mechelen Belgium",            "venue_lat": 51.0281204, "venue_lng": 4.4805126},
    {"venue_id": "ven_385758534945655963662d526330776c766e74776b77424a496843", "city": "Mechelen", "venue_name": "Rosco Coffee & more",        "venue_type": "COFFEE",          "venue_address": "Bruul 11 2800 Mechelen Belgium",               "venue_lat": 51.0273688, "venue_lng": 4.480965},
    {"venue_id": "ven_5538515943727857676151526330776c6e38584d5277394a496843", "city": "Mechelen", "venue_name": "HEMA",                       "venue_type": "SHOPPING_CENTER", "venue_address": "Bruul 34/36 2800 Mechelen Belgium",            "venue_lat": 51.0267354, "venue_lng": 4.4808563},
    {"venue_id": "ven_6f61526d2d515150797453526330776c543966666b64704a496843", "city": "Mechelen", "venue_name": "Coffice",                    "venue_type": "COFFEE",          "venue_address": "IJzerenleen 33 2800 Mechelen Belgium",         "venue_lat": 51.0266177, "venue_lng": 4.4787675},
    {"venue_id": "ven_416e476435424652367563526330776c6e734c4e43586a4a496843", "city": "Mechelen", "venue_name": "Torfs",                      "venue_type": "SHOPPING",        "venue_address": "47 IJzerenleen Mechelen",                      "venue_lat": 51.0263147, "venue_lng": 4.4785005},
    {"venue_id": "ven_735a502d51636237764c74526330776c6e634a5056636d4a496843", "city": "Mechelen", "venue_name": "Le Pain Quotidien",          "venue_type": "RESTAURANT",      "venue_address": "IJzerenleen 35 2800 Mechelen Belgium",         "venue_lat": 51.0265884, "venue_lng": 4.4787325},
    {"venue_id": "ven_416f7a6f434d657a6d3474526330776c6638566a682d434a496843", "city": "Mechelen", "venue_name": "Bakkerij Atlas",             "venue_type": "BAKERY",          "venue_address": "Liersesteenweg 179 2800 Mechelen Belgium",     "venue_lat": 51.0386837, "venue_lng": 4.4799569},
]


# ---------------------------------------------------------------------------
# 3. Polling
# ---------------------------------------------------------------------------
def poll_venue(venue, api_key, timestamp):
    """Poll one venue's live busyness. Returns a log entry dict."""
    response = requests.post(
        LIVE_API_URL,
        params={"api_key_private": api_key, "venue_id": venue["venue_id"]},
        timeout=15,
    )
    response.raise_for_status()
    data = response.json()

    analysis = data.get("analysis", {})
    live_avail = analysis.get("venue_live_busyness_available", False)

    # Capture every field the response offers, regardless of live availability.
    # venue_forecasted_busyness (the "expected" busyness for the current hour) is
    # returned in the same response even when there's no live signal, so we record
    # it unconditionally rather than discarding it on no-live rows. .get() yields
    # None when a field is genuinely absent (e.g. venue closed at this hour).
    return {
        "timestamp": timestamp,
        "venue_id": venue["venue_id"],
        "venue_name": venue["venue_name"],
        "city": venue["city"],
        "venue_type": venue["venue_type"],
        "venue_lat": venue["venue_lat"],
        "venue_lng": venue["venue_lng"],
        "live_available": live_avail,
        "live_busyness": analysis.get("venue_live_busyness"),
        "normal_busyness": analysis.get("venue_forecasted_busyness"),
        "busyness_delta": analysis.get("venue_live_forecasted_delta"),
    }


def main():
    if not API_KEY:
        raise SystemExit("❌ BESTTIME_API_KEY is not set. Add it as a GitHub Actions Secret.")

    # UTC timestamp — unambiguous in logs. GitHub Actions runners are on UTC.
    timestamp = datetime.now(timezone.utc).isoformat()
    print(f"📡 Polling {len(MONITORED_VENUES)} venues at {timestamp} (UTC)...")

    current_logs = []
    for v in MONITORED_VENUES:
        try:
            entry = poll_venue(v, API_KEY, timestamp)
            current_logs.append(entry)
            busy = entry["live_busyness"]
            busy_str = f"{busy}%" if busy is not None else "n/a"
            print(f"✅ {v['venue_name'][:24]:<24} | {v['city'][:18]:<18} | Live: {busy_str}")
        except Exception as e:
            print(f"❌ Error {v['venue_name']}: {e}")
        time.sleep(0.5)  # gentle on the API

    # --- Append rows to the CSV log -----------------------------------------
    # Append-only: we never read the existing log, so writes stay instant no
    # matter how large it grows. The header is written only when the file is new.
    new_file = not os.path.isfile(LOG_FILE) or os.path.getsize(LOG_FILE) == 0
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if new_file:
            writer.writeheader()
        writer.writerows(current_logs)

    live_count = sum(1 for e in current_logs if e["live_available"])
    print(f"\n💾 Appended {len(current_logs)} rows "
          f"({live_count} with live data) to '{LOG_FILE}'.")


if __name__ == "__main__":
    main()
