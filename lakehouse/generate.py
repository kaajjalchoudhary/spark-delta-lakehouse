"""Generate a deterministic, intentionally messy ride-hailing feed.

Landing layout (one file per feed per day, as an upstream export would drop them):

    landing/zones/zones.csv                reference data
    landing/drivers/YYYY-MM-DD.jsonl       driver CDC events
    landing/trips/YYYY-MM-DD.jsonl         trip events

What makes it realistic, and what the pipeline has to cope with:
- ~5% of trip events arrive a day late, in the next day's file.
- Some completed trips get a fare adjustment the next day (same trip_id, later event_ts).
- ~2% of lines are delivered twice.
- A few lines per week are not JSON at all (an upstream error page, a truncated write).
- ~1% of trips fail business rules (negative fare, drop-off before pick-up).
- From `tips_from_day` the producer adds `tip_amount` (already in the contract), and from
  `surge_from_day` it adds `surge_multiplier`, which the contract does not know about yet.
- Drivers change vehicle type and go on or off duty during the day, and sometimes the
  CDC feed re-sends an unchanged record.

Output is deterministic for a given seed and start date, and generating more days
reproduces earlier files byte for byte.
"""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

# city -> (currency, base fare, per-km rate, zones)
CITIES: dict[str, tuple[str, float, float, list[str]]] = {
    "Dubai": ("AED", 12.0, 2.2, ["Downtown", "Marina", "Deira", "JLT"]),
    "Mumbai": ("INR", 50.0, 18.0, ["Bandra", "Andheri", "Colaba", "Powai"]),
    "London": ("GBP", 4.0, 1.8, ["Soho", "Camden", "Shoreditch", "Canary Wharf"]),
}
VEHICLE_MULTIPLIER = {"economy": 1.0, "electric": 1.1, "comfort": 1.3, "xl": 1.6}
TIP_STEPS = {"AED": [2, 5, 10], "INR": [20, 50, 100], "GBP": [1, 2, 5]}
FIRST = ["Aarav", "Bilal", "Chloe", "Dev", "Emma", "Fatima", "George", "Hana", "Ivan", "Jia",
         "Karan", "Layla", "Mateo", "Nadia", "Oliver", "Pooja", "Rami", "Sofia", "Tom", "Yusuf"]
LAST = ["Ali", "Brown", "Chopra", "Das", "El-Sayed", "Green", "Hussain", "Iyer", "Jones", "Kapoor",
        "Lee", "Malik", "Nguyen", "Owen", "Rao", "Shah", "Taylor", "Usman", "Verma", "Wilson"]
GARBAGE_LINES = [
    "<!DOCTYPE html><html><title>502 Bad Gateway</title></html>",
    '{trip_id: T-unquoted, "fare_amount": 12.5}',
    '{"trip_id": "T2026',  # truncated write
]
TS_FMT = "%Y-%m-%dT%H:%M:%S"


@dataclass(frozen=True)
class GenerateConfig:
    start: date = date(2026, 1, 1)
    days: int = 14
    seed: int = 7
    trips_per_day: int = 250
    initial_drivers: int = 45
    tips_from_day: int = 3
    surge_from_day: int = 8


def _ts(value: datetime) -> str:
    return value.strftime(TS_FMT)


def _random_time(rng: random.Random, day: date, start_hour: int = 0, end_hour: int = 24) -> datetime:
    seconds = rng.randrange(start_hour * 3600, end_hour * 3600)
    return datetime.combine(day, time(0, 0)) + timedelta(seconds=seconds)


class _Drivers:
    """Driver state over time, so a trip can be priced with the vehicle in use at pick-up."""

    def __init__(self) -> None:
        self.history: dict[int, list[tuple[datetime, dict]]] = {}

    def record(self, at: datetime, state: dict) -> None:
        self.history.setdefault(state["driver_id"], []).append((at, dict(state)))

    def latest(self, driver_id: int) -> dict:
        return dict(self.history[driver_id][-1][1])

    def at(self, driver_id: int, when: datetime) -> dict | None:
        past = [(changed_at, s) for changed_at, s in self.history[driver_id] if changed_at <= when]
        return max(past, key=lambda item: item[0])[1] if past else None


def write_zones(landing: Path) -> list[dict]:
    zones = []
    for city, (_, _, _, names) in CITIES.items():
        for name in names:
            zones.append({"zone_id": len(zones) + 1, "zone_name": name, "city": city})
    path = landing / "zones" / "zones.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["zone_id", "zone_name", "city"], lineterminator="\n")
        writer.writeheader()
        writer.writerows(zones)
    return zones


def generate(landing: Path, cfg: GenerateConfig | None = None) -> dict[str, int]:
    """Write the landing files and return line counts per feed."""
    cfg = cfg or GenerateConfig()
    rng = random.Random(cfg.seed)
    zones = write_zones(landing)
    zones_by_city: dict[str, list[dict]] = {}
    for zone in zones:
        zones_by_city.setdefault(zone["city"], []).append(zone)

    drivers = _Drivers()
    next_driver_id = 1
    trip_lines: dict[date, list[str]] = {}
    counts = {"zones": len(zones), "drivers": 0, "trips": 0}
    end = cfg.start + timedelta(days=cfg.days)

    def new_driver(onboarded_at: datetime) -> dict:
        nonlocal next_driver_id
        state = {
            "driver_id": next_driver_id,
            "full_name": f"{rng.choice(FIRST)} {rng.choice(LAST)}",
            "city": rng.choice(list(CITIES)),
            "vehicle_type": rng.choices(list(VEHICLE_MULTIPLIER), [50, 15, 25, 10])[0],
            "status": "active",
            "updated_at": _ts(onboarded_at),
        }
        next_driver_id += 1
        drivers.record(onboarded_at, state)
        return state

    for offset in range(cfg.days):
        day = cfg.start + timedelta(days=offset)
        next_day = day + timedelta(days=1)
        driver_events: list[dict] = []

        # --- driver CDC --------------------------------------------------------------
        if offset == 0:
            for _ in range(cfg.initial_drivers):
                onboarded = datetime.combine(day, time(0, 0)) - timedelta(days=rng.randint(30, 400))
                driver_events.append(new_driver(onboarded))

        existing = sorted(drivers.history)
        for driver_id in rng.sample(existing, k=rng.randint(1, 3)):
            state = drivers.latest(driver_id)
            state["vehicle_type"] = rng.choice([v for v in VEHICLE_MULTIPLIER if v != state["vehicle_type"]])
            changed_at = _random_time(rng, day, 0, 12)  # vehicle changes happen in the morning
            state["updated_at"] = _ts(changed_at)
            drivers.record(changed_at, state)
            driver_events.append(state)
        if rng.random() < 0.5:
            state = drivers.latest(rng.choice(existing))
            state["status"] = "inactive" if state["status"] == "active" else "active"
            changed_at = _random_time(rng, day, 12, 24)  # duty changes in the afternoon
            state["updated_at"] = _ts(changed_at)
            drivers.record(changed_at, state)
            driver_events.append(state)
        if rng.random() < 0.3:
            # Re-sent record: nothing tracked changed, only the timestamp moved.
            state = drivers.latest(rng.choice(existing))
            state["updated_at"] = _ts(datetime.strptime(state["updated_at"], TS_FMT) + timedelta(seconds=1))
            driver_events.append(state)
        if offset > 0 and rng.random() < 0.25:
            driver_events.append(new_driver(_random_time(rng, day)))

        # --- trips -------------------------------------------------------------------
        n_trips = max(1, round(rng.gauss(cfg.trips_per_day, cfg.trips_per_day * 0.1)))
        all_drivers = sorted(drivers.history)
        for n in range(1, n_trips + 1):
            pickup = _random_time(rng, day)
            state = None
            for _ in range(10):
                candidate = drivers.at(rng.choice(all_drivers), pickup)
                if candidate and candidate["status"] == "active":
                    state = candidate
                    break
            if state is None:
                continue
            currency, base, per_km, _ = CITIES[state["city"]]
            pickup_zone, dropoff_zone = rng.choice(zones_by_city[state["city"]]), rng.choice(zones_by_city[state["city"]])
            requested = pickup - timedelta(minutes=rng.randint(2, 12))
            completed = rng.random() < 0.92
            trip = {
                "trip_id": f"T{day:%Y%m%d}-{n:05d}",
                "driver_id": state["driver_id"],
                "rider_id": rng.randint(1, 5000),
                "pickup_zone_id": pickup_zone["zone_id"],
                "dropoff_zone_id": dropoff_zone["zone_id"],
                "requested_at": _ts(requested),
                "pickup_at": _ts(pickup),
                "dropoff_at": None,
                "status": "completed" if completed else "cancelled",
                "distance_km": None,
                "fare_amount": 0.0,
                "currency": currency,
                "event_ts": _ts(requested + timedelta(minutes=5)),
            }
            if completed:
                distance = round(min(rng.lognormvariate(1.6, 0.6), 40.0), 2)
                dropoff = pickup + timedelta(minutes=distance * rng.uniform(2.0, 4.0) + rng.uniform(2, 8))
                fare = round((base + per_km * distance) * VEHICLE_MULTIPLIER[state["vehicle_type"]], 2)
                trip.update(distance_km=distance, dropoff_at=_ts(dropoff), fare_amount=fare, event_ts=_ts(dropoff))
            if offset >= cfg.tips_from_day:
                tipped = completed and rng.random() < 0.3
                trip["tip_amount"] = float(rng.choice(TIP_STEPS[currency])) if tipped else 0.0
            if offset >= cfg.surge_from_day:
                trip["surge_multiplier"] = rng.choice([1.0, 1.0, 1.0, 1.2, 1.5])

            # Business-rule violations the silver layer must quarantine.
            roll = rng.random()
            if completed and roll < 0.006:
                trip["fare_amount"] = -trip["fare_amount"]
            elif completed and roll < 0.01:
                trip["dropoff_at"] = _ts(pickup - timedelta(minutes=3))

            lands_on = next_day if rng.random() < 0.05 else day
            trip_lines.setdefault(lands_on, []).append(json.dumps(trip))

            if completed and roll > 0.97:
                # Fare adjustment issued the next day.
                adjusted = dict(trip)
                adjusted["fare_amount"] = round(trip["fare_amount"] * rng.choice([0.8, 0.9, 1.15]), 2)
                adjusted["event_ts"] = _ts(_random_time(rng, next_day))
                trip_lines.setdefault(next_day, []).append(json.dumps(adjusted))

        lines = trip_lines.pop(day, [])
        lines += [line for line in lines if rng.random() < 0.02]
        if rng.random() < 0.4:
            lines.insert(rng.randrange(len(lines) + 1), rng.choice(GARBAGE_LINES))

        _write_lines(landing / "drivers" / f"{day}.jsonl", [json.dumps(e) for e in driver_events])
        _write_lines(landing / "trips" / f"{day}.jsonl", lines)
        counts["drivers"] += len(driver_events)
        counts["trips"] += len(lines)

    # Events destined for the day after the window are dropped: they "arrive tomorrow".
    assert all(d >= end for d in trip_lines)
    return counts


def _write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{line}\n" for line in lines), encoding="utf-8")
