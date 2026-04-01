#!/usr/bin/env python3
"""Milkyseas V2: 多地点荧光海日更预测器

功能：
- 抓取 Open-Meteo 公开天气与海洋预报
- 对 20 个中国沿海/近海城市生成日度荧光海概率评分
- 输出静态站点可直接消费的 latest.json 与 history 快照
- 为后续历史标签校准预留参数结构

说明：
- 这是启发式可校准模板，不是“已训练好的真值模型”。
- 评分表示“夜间观察到荧光海的综合概率倾向”，结合连续升温、持续高温、风向朝岸、降雨、海温与海流。
- 若海洋接口缺字段，脚本自动降级，仅保留天气特征。
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOCATIONS = ROOT / "data" / "locations.json"
TEMP_ARCHIVE_PATH = ROOT / "docs" / "data" / "temp_archive.json"
PAST_DAYS = 7
FUTURE_DAYS = 7

USER_AGENT = "Milkyseas-V2/1.0 (+https://github.com/lzq1206/Milkyseas)"

REGION_PRESETS = {
    "渤海/黄海北部": {"temp_peak": 24.0, "sst_peak": 22.5, "wind_peak": 5.0, "onshore_dir": 90.0, "bias": 0.03},
    "黄海": {"temp_peak": 25.0, "sst_peak": 23.5, "wind_peak": 5.0, "onshore_dir": 100.0, "bias": 0.04},
    "东海": {"temp_peak": 26.0, "sst_peak": 24.5, "wind_peak": 5.2, "onshore_dir": 110.0, "bias": 0.05},
    "东海/台湾海峡": {"temp_peak": 26.5, "sst_peak": 25.0, "wind_peak": 5.2, "onshore_dir": 115.0, "bias": 0.05},
    "台湾海峡": {"temp_peak": 27.0, "sst_peak": 25.5, "wind_peak": 5.2, "onshore_dir": 120.0, "bias": 0.06},
    "南海北部": {"temp_peak": 28.0, "sst_peak": 26.0, "wind_peak": 4.8, "onshore_dir": 125.0, "bias": 0.06},
    "南海": {"temp_peak": 28.5, "sst_peak": 26.5, "wind_peak": 4.8, "onshore_dir": 130.0, "bias": 0.07},
}


def clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


def gaussian(x: float, mu: float, sigma: float) -> float:
    if sigma <= 0:
        return 0.0
    return math.exp(-((x - mu) ** 2) / (2 * sigma * sigma))


def triangular(x: float, low: float, peak: float, high: float) -> float:
    if x <= low or x >= high:
        return 0.0
    if x == peak:
        return 1.0
    if x < peak:
        return (x - low) / (peak - low)
    return (high - x) / (high - peak)


def score_to_probability(score: float, threshold: float, scale: float = 0.08) -> float:
    z = (score - threshold) / max(scale, 1e-6)
    return 1.0 / (1.0 + math.exp(-z))


def fetch_json(url: str, params: Dict[str, Any], retries: int = 2, timeout: int = 30) -> Dict[str, Any]:
    query = urllib.parse.urlencode(params)
    full_url = f"{url}?{query}"
    req = urllib.request.Request(full_url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = resp.read().decode("utf-8")
            return json.loads(payload)
        except Exception as exc:  # pragma: no cover - network fallback
            last_exc = exc
            if attempt < retries:
                time.sleep(0.8 * (attempt + 1))
            else:
                raise last_exc
    raise RuntimeError("unreachable")


def date_range(start: dt.date, end: dt.date) -> List[dt.date]:
    days = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += dt.timedelta(days=1)
    return days


def fetch_period_payload(endpoint: str, lat: float, lon: float, start_date: dt.date, end_date: dt.date, hourly: str) -> Dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": hourly,
        "timezone": "Asia/Shanghai",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    return fetch_json(endpoint, params)


def fetch_weather_series(lat: float, lon: float, start_date: dt.date, end_date: dt.date) -> Dict[str, Any]:
    endpoint = "https://api.open-meteo.com/v1/archive" if end_date < dt.date.today() else "https://api.open-meteo.com/v1/forecast"
    return fetch_period_payload(endpoint, lat, lon, start_date, end_date, "temperature_2m,wind_speed_10m,wind_direction_10m,precipitation,cloud_cover")


def fetch_marine_series(lat: float, lon: float, start_date: dt.date, end_date: dt.date) -> Dict[str, Any]:
    archive_endpoint = "https://marine-api.open-meteo.com/v1/archive"
    forecast_endpoint = "https://marine-api.open-meteo.com/v1/marine"
    endpoint = archive_endpoint if end_date < dt.date.today() else forecast_endpoint
    try:
        return fetch_period_payload(endpoint, lat, lon, start_date, end_date, "sea_surface_temperature,ocean_current_velocity,ocean_current_direction")
    except Exception:
        if endpoint == archive_endpoint:
            return {"hourly": {"time": []}}
        try:
            return fetch_period_payload(forecast_endpoint, lat, lon, start_date, end_date, "sea_surface_temperature,ocean_current_velocity,ocean_current_direction")
        except Exception:
            return {"hourly": {"time": []}}


def aggregate_hourly_by_day(hourly: Dict[str, List[Any]]) -> Dict[str, Dict[str, List[float]]]:
    by_day: Dict[str, Dict[str, List[float]]] = {}
    times = hourly.get("time", [])
    fields = [k for k in hourly.keys() if k != "time"]
    for i, t in enumerate(times):
        day = t.split("T")[0]
        bucket = by_day.setdefault(day, {})
        for f in fields:
            val = hourly[f][i]
            if val is None:
                continue
            try:
                bucket.setdefault(f, []).append(float(val))
            except Exception:
                continue
    return by_day


def stat(vals: List[float], mode: str = "mean") -> Optional[float]:
    if not vals:
        return None
    if mode == "sum":
        return float(sum(vals))
    if mode == "max":
        return float(max(vals))
    return float(statistics.mean(vals))


def onshore_score(direction_deg: Optional[float], preferred: float) -> float:
    if direction_deg is None:
        return 0.5
    d = direction_deg % 360.0
    diff = abs(((d - preferred + 180) % 360) - 180)
    return clamp01(math.exp(-(diff ** 2) / (2 * 55.0 * 55.0)))


def classify_level(prob: float) -> str:
    if prob >= 0.72:
        return "高概率"
    if prob >= 0.48:
        return "中概率"
    return "低概率"


def mean_safe(vals: Iterable[Optional[float]]) -> Optional[float]:
    clean = [v for v in vals if v is not None]
    if not clean:
        return None
    return float(statistics.mean(clean))


def warm_persistence_score(temp_history: List[Optional[float]], threshold: float) -> float:
    """连续多日维持高温的加分：最近四天里，越多天高于阈值越好。"""
    recent = [t for t in temp_history[-4:] if t is not None]
    if not recent:
        return 0.5
    hits = sum(1 for t in recent if t >= threshold)
    return clamp01(hits / 4.0)


def warming_trend_score(temp_history: List[Optional[float]]) -> float:
    """连续升温加分：对最近三天做简化斜率。"""
    recent = [t for t in temp_history[-3:] if t is not None]
    if len(recent) < 2:
        return 0.5
    slope = recent[-1] - recent[0]
    return clamp01((slope + 3.0) / 6.0)


def short_term_rise_score(temp_history: List[Optional[float]], window: int = 4, target_jump: float = 10.0) -> float:
    """短时间内升温加分：最近窗口内当前值相对历史最低值的抬升幅度。"""
    recent = [t for t in temp_history[-window:] if t is not None]
    if len(recent) < 2:
        return 0.5
    rise = recent[-1] - min(recent[:-1])
    return clamp01(rise / max(target_jump, 1e-6))


def build_location_forecast(
    location: Dict[str, Any],
    days: int = 15,
    air_history: Optional[List[float]] = None,
    sea_history: Optional[List[float]] = None,
) -> Dict[str, Any]:
    lat = location["lat"]
    lon = location["lon"]
    region = location.get("group", "东海")
    preset = REGION_PRESETS.get(region, REGION_PRESETS["东海"])
    geo_prior = float(location.get("geo_prior", 1.0))

    today = dt.date.today()
    start = today - dt.timedelta(days=PAST_DAYS)
    end = today + dt.timedelta(days=FUTURE_DAYS)

    weather = fetch_weather_series(lat, lon, start, end)
    marine = fetch_marine_series(lat, lon, start, end)

    weather_by_day = aggregate_hourly_by_day(weather.get("hourly", {}))
    marine_by_day = aggregate_hourly_by_day(marine.get("hourly", {}))
    days_all = sorted(set(weather_by_day) | set(marine_by_day))

    daily_rows = []
    prev_rain: Optional[float] = None
    air_history_values: List[Optional[float]] = list(air_history or [])
    sea_history_values: List[Optional[float]] = list(sea_history or [])
    today_row: Optional[Dict[str, Any]] = None

    for day in days_all:
        w = weather_by_day.get(day, {})
        m = marine_by_day.get(day, {})

        temp = stat(w.get("temperature_2m", []), "mean")
        wind = stat(w.get("wind_speed_10m", []), "mean")
        wind_dir = stat(w.get("wind_direction_10m", []), "mean")
        rain = stat(w.get("precipitation", []), "sum")
        cloud = stat(w.get("cloud_cover", []), "mean")
        sst = stat(m.get("sea_surface_temperature", []), "mean")
        current = stat(m.get("ocean_current_velocity", []), "mean")
        current_dir = stat(m.get("ocean_current_direction", []), "mean")

        air_history_values.append(temp)
        sea_history_values.append(sst)

        sst_score = 0.5 if sst is None else triangular(sst, low=preset["sst_peak"] - 8, peak=preset["sst_peak"], high=preset["sst_peak"] + 6)
        temp_score = 0.5 if temp is None else triangular(temp, low=preset["temp_peak"] - 10, peak=preset["temp_peak"], high=preset["temp_peak"] + 7)
        air_warm_trend = warming_trend_score(air_history_values)
        air_warm_persist = warm_persistence_score(air_history_values, threshold=preset["temp_peak"] - 1.5)
        air_short_rise = short_term_rise_score(air_history_values, window=4, target_jump=10.0)
        air_heat_build_score = clamp01(0.28 * temp_score + 0.22 * air_warm_trend + 0.20 * air_warm_persist + 0.30 * air_short_rise)

        sea_temp_score = 0.5 if sst is None else triangular(sst, low=preset["sst_peak"] - 7, peak=preset["sst_peak"], high=preset["sst_peak"] + 5)
        sea_warm_trend = warming_trend_score(sea_history_values)
        sea_warm_persist = warm_persistence_score(sea_history_values, threshold=preset["sst_peak"] - 1.2)
        sea_short_rise = short_term_rise_score(sea_history_values, window=4, target_jump=10.0)
        sea_heat_build_score = clamp01(0.28 * sea_temp_score + 0.22 * sea_warm_trend + 0.20 * sea_warm_persist + 0.30 * sea_short_rise)

        heat_build_score = clamp01(0.45 * sea_heat_build_score + 0.35 * air_heat_build_score + 0.20 * temp_score)
        wind_score = 0.5 if wind is None else gaussian(wind, mu=preset["wind_peak"], sigma=2.7)
        onshore = onshore_score(wind_dir, preset["onshore_dir"])
        shore_transport_score = clamp01(onshore * (0.45 + 0.55 * wind_score))

        if rain is None:
            rain_score = 0.5
        else:
            rain_today = 1.0 - clamp01(rain / 15.0)
            lag_bonus = 0.0
            if prev_rain is not None:
                lag_bonus = triangular(prev_rain, low=0.5, peak=4.0, high=9.0) * 0.3
            rain_score = clamp01(0.8 * rain_today + lag_bonus)

        current_score = 0.5 if current is None else gaussian(current, mu=0.25, sigma=0.18)
        cloud_vis = 0.6 if cloud is None else 1.0 - clamp01(cloud / 100.0)
        wind_vis = 0.6 if wind is None else gaussian(wind, mu=4.0, sigma=2.0)
        visibility = clamp01(0.58 * cloud_vis + 0.42 * wind_vis)

        raw_risk = (
            0.38 * heat_build_score
            + 0.18 * shore_transport_score
            + 0.15 * sst_score
            + 0.12 * wind_score
            + 0.09 * rain_score
            + 0.08 * current_score
        )
        raw_risk = clamp01(raw_risk + preset["bias"])
        geo_prior_score = clamp01((geo_prior - 0.9) / 0.4)
        prior_boost = clamp01(0.5 * geo_prior_score + 0.5 * (geo_prior - 1.0 + 0.5))

        calibrated_score = clamp01(0.63 * raw_risk + 0.22 * visibility + 0.15 * prior_boost)
        probability = clamp01(score_to_probability(calibrated_score, threshold=0.58, scale=0.085))
        level = classify_level(probability)
        if day < today.isoformat():
            observation = "历史回看"
        elif day == today.isoformat():
            observation = "今日观测"
        else:
            observation = "适合观察" if (visibility >= 0.55 and probability >= 0.48) else "一般"

        daily_rows.append(
            {
                "date": day,
                "features": {
                    "temperature_2m": temp,
                    "wind_speed_10m": wind,
                    "wind_direction_10m": wind_dir,
                    "precipitation": rain,
                    "cloud_cover": cloud,
                    "sea_surface_temperature": sst,
                    "ocean_current_velocity": current,
                    "ocean_current_direction": current_dir,
                },
                "scores": {
                    "sst_score": round(sst_score, 3),
                    "temp_score": round(temp_score, 3),
                    "air_warming_trend_score": round(air_warm_trend, 3),
                    "air_warm_persistence_score": round(air_warm_persist, 3),
                    "air_short_rise_score": round(air_short_rise, 3),
                    "air_heat_build_score": round(air_heat_build_score, 3),
                    "sea_temperature_score": round(sea_temp_score, 3),
                    "sea_warming_trend_score": round(sea_warm_trend, 3),
                    "sea_warm_persistence_score": round(sea_warm_persist, 3),
                    "sea_short_rise_score": round(sea_short_rise, 3),
                    "sea_heat_build_score": round(sea_heat_build_score, 3),
                    "heat_build_score": round(heat_build_score, 3),
                    "wind_score": round(wind_score, 3),
                    "onshore_score": round(onshore, 3),
                    "shore_transport_score": round(shore_transport_score, 3),
                    "rain_score": round(rain_score, 3),
                    "current_score": round(current_score, 3),
                    "raw_risk": round(raw_risk, 3),
                    "visibility": round(visibility, 3),
                    "calibrated_score": round(calibrated_score, 3),
                    "probability": round(probability, 3),
                },
                "level": level,
                "observation": observation,
                "phase": "past" if day < today.isoformat() else ("today" if day == today.isoformat() else "future"),
            }
        )
        if day == today.isoformat():
            today_row = daily_rows[-1]
        prev_rain = rain

    best_day = max(daily_rows, key=lambda x: x["scores"]["probability"]) if daily_rows else None
    hottest = max((row["scores"]["probability"] for row in daily_rows), default=0.0)
    today_prob = today_row["scores"]["probability"] if today_row else (daily_rows[0]["scores"]["probability"] if daily_rows else 0.0)

    return {
        "location": location,
                "region_preset": preset,
        "geo_prior": round(geo_prior, 3),
        "geo_prior_score": round(geo_prior_score, 3),
        "prior_boost": round(prior_boost, 3),
        "summary": {
            "today_date": today.isoformat(),
            "today_probability": round(today_prob, 3),
            "best_probability": round(hottest, 3),
            "best_date": best_day["date"] if best_day else None,
            "best_level": best_day["level"] if best_day else None,
            "today_level": today_row["level"] if today_row else (daily_rows[0]["level"] if daily_rows else None),
            "today_index": (daily_rows.index(today_row) if today_row in daily_rows else None),
        },
        "daily": daily_rows,
    }


def load_locations(path: Path) -> List[Dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_temp_archive(path: Path) -> Dict[str, List[Dict[str, Any]]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    cities = payload.get("cities", {}) if isinstance(payload, dict) else {}
    if not isinstance(cities, dict):
        return {}
    cleaned: Dict[str, List[Dict[str, Any]]] = {}
    for city, rows in cities.items():
        if not isinstance(rows, list):
            continue
        cleaned[city] = dedupe_archive_rows([row for row in rows if isinstance(row, dict) and row.get("date")])
    return cleaned


def dedupe_archive_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[str, Dict[str, Any]] = {}
    for row in sorted(rows, key=lambda r: (r.get("date", ""), r.get("generated_at", ""))):
        date_key = row.get("date")
        if not date_key:
            continue
        deduped[date_key] = row
    return list(deduped.values())


def trim_temp_archive(archive: Dict[str, List[Dict[str, Any]]], keep_days: int = 30) -> Dict[str, List[Dict[str, Any]]]:
    cutoff = dt.date.today() - dt.timedelta(days=keep_days - 1)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for city, rows in archive.items():
        rows_sorted = dedupe_archive_rows(rows)
        filtered = [r for r in rows_sorted if r.get("date") and r["date"] >= cutoff.isoformat()]
        if filtered:
            out[city] = filtered[-keep_days:]
    return out


def append_temp_archive(
    archive: Dict[str, List[Dict[str, Any]]],
    locations: List[Dict[str, Any]],
    results: List[Dict[str, Any]],
    archive_date: str,
) -> Dict[str, List[Dict[str, Any]]]:
    indexed = {row["location"]["city"]: row for row in results}
    for loc in locations:
        city = loc["city"]
        row = indexed.get(city)
        if not row or not row.get("daily"):
            continue
        today_date = row.get("summary", {}).get("today_date")
        today_row = next((d for d in row["daily"] if d.get("date") == today_date), row["daily"][0])
        temp = today_row.get("features", {}).get("temperature_2m")
        sea_temp = today_row.get("features", {}).get("sea_surface_temperature")
        if temp is None and sea_temp is None:
            continue
        archive[city] = [r for r in archive.get(city, []) if r.get("date") != archive_date]
        entry = {
            "date": archive_date,
            "city": city,
            "province": loc.get("province"),
            "group": loc.get("group"),
            "temperature_2m": round(float(temp), 3),
            "sea_surface_temperature": round(float(sea_temp), 3) if sea_temp is not None else None,
            "generated_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        archive.setdefault(city, []).append(entry)
    return trim_temp_archive(archive, keep_days=30)


def main() -> None:
    parser = argparse.ArgumentParser(description="Milkyseas V2: 多地点荧光海预测")
    parser.add_argument("--days", type=int, default=15, help="展示天数，默认15（前7天+后7天）")
    parser.add_argument("--locations", type=str, default=str(DEFAULT_LOCATIONS), help="地点JSON路径")
    parser.add_argument("--output", type=str, default=str(ROOT / "docs" / "data" / "latest.json"), help="输出JSON路径")
    parser.add_argument("--history-dir", type=str, default=str(ROOT / "docs" / "data" / "history"), help="历史快照目录")
    parser.add_argument("--workers", type=int, default=6, help="并发数")
    args = parser.parse_args()

    if args.days < 1 or args.days > 16:
        raise SystemExit("--days 建议在 1~16 之间")

    locations = load_locations(Path(args.locations))
    temp_archive = load_temp_archive(TEMP_ARCHIVE_PATH)
    run_at = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    run_date = dt.date.today().isoformat()

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {
            pool.submit(
                build_location_forecast,
                loc,
                args.days,
                [r.get("temperature_2m") for r in temp_archive.get(loc["city"], []) if r.get("temperature_2m") is not None][-30:],
                [r.get("sea_surface_temperature") for r in temp_archive.get(loc["city"], []) if r.get("sea_surface_temperature") is not None][-30:],
            ): loc
            for loc in locations
        }
        for future in as_completed(futures):
            loc = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                results.append(
                    {
                        "location": loc,
                        "region_preset": REGION_PRESETS.get(loc.get("group", "东海"), REGION_PRESETS["东海"]),
                        "summary": {
                        "today_probability": None,
                        "best_probability": None,
                        "best_date": None,
                        "best_level": "低概率",
                        "today_level": "低概率",
                        "error": str(exc),
                    },
                    "daily": [],
                }
            )

    results.sort(key=lambda x: (x["summary"].get("today_probability") or -1), reverse=True)

    all_today = [r for r in results if r["summary"].get("today_probability") is not None]
    top_today = all_today[:5]
    top_best = sorted(all_today, key=lambda x: x["summary"].get("best_probability") or -1, reverse=True)[:5]

    payload = {
        "meta": {
            "model": "milkyseas_v2_calibrated_template",
            "version": "v2.0.0",
            "generated_at": run_at,
            "generated_date": run_date,
            "days": args.days,
            "source": {
                "weather": "Open-Meteo forecast API",
                "marine": "Open-Meteo marine API",
            },
            "note": "启发式可校准模板；后续可接入历史荧光海标签做参数再训练。",
        },
        "ranking_today": [
            {
                "city": r["location"]["city"],
                "province": r["location"]["province"],
                "group": r["location"]["group"],
                "geo_prior": r.get("geo_prior"),
                "today_probability": r["summary"].get("today_probability"),
                "today_level": r["summary"].get("today_level"),
                "best_probability": r["summary"].get("best_probability"),
                "best_date": r["summary"].get("best_date"),
                "best_level": r["summary"].get("best_level"),
            }
            for r in results
        ],
        "top_today": [
            {
                "city": r["location"]["city"],
                "province": r["location"]["province"],
                "today_probability": r["summary"].get("today_probability"),
                "today_level": r["summary"].get("today_level"),
            }
            for r in top_today
        ],
        "top_best": [
            {
                "city": r["location"]["city"],
                "province": r["location"]["province"],
                "best_probability": r["summary"].get("best_probability"),
                "best_date": r["summary"].get("best_date"),
                "best_level": r["summary"].get("best_level"),
            }
            for r in top_best
        ],
        "locations": results,
    }

    temp_archive = append_temp_archive(temp_archive, locations, results, run_date)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    TEMP_ARCHIVE_PATH.parent.mkdir(parents=True, exist_ok=True)
    TEMP_ARCHIVE_PATH.write_text(
        json.dumps(
            {
                "meta": {
                    "generated_at": run_at,
                    "generated_date": run_date,
                    "keep_days": 30,
                    "note": "Rolling 30-day temperature archive used for warming trend calibration.",
                },
                "cities": temp_archive,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    history_dir = Path(args.history_dir)
    history_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = history_dir / f"{run_date}.json"
    snapshot_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"已写出: {out_path}")
    print(f"已归档: {snapshot_path}")
    if top_today:
        print("\n今日重点地点：")
        for row in top_today:
            print(f"- {row['location']['city']}: {row['summary']['today_probability']:.3f} / {row['summary']['today_level']}")


if __name__ == "__main__":
    main()
