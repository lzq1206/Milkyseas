#!/usr/bin/env python3
"""Milkyseas V2: 多地点荧光海日更预测器

功能：
- 抓取 Open-Meteo 公开天气与海洋预报
- 对 20 个中国沿海/近海城市生成日度荧光海风险评分
- 输出静态站点可直接消费的 latest.json 与 history 快照
- 为后续历史标签校准预留参数结构

说明：
- 这是启发式可校准模板，不是“已训练好的真值模型”。
- 评分表示“夜间观察到荧光海的综合机会”，结合温度、风场、降雨、海温与海流。
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
        return "高"
    if prob >= 0.48:
        return "中"
    return "低"


def build_location_forecast(location: Dict[str, Any], days: int = 7) -> Dict[str, Any]:
    lat = location["lat"]
    lon = location["lon"]
    region = location.get("group", "东海")
    preset = REGION_PRESETS.get(region, REGION_PRESETS["东海"])

    today = dt.date.today()
    end = today + dt.timedelta(days=days - 1)

    weather_params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m,precipitation,cloud_cover",
        "timezone": "Asia/Shanghai",
        "start_date": today.isoformat(),
        "end_date": end.isoformat(),
    }
    weather = fetch_json("https://api.open-meteo.com/v1/forecast", weather_params)

    marine_params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "sea_surface_temperature,ocean_current_velocity,ocean_current_direction",
        "timezone": "Asia/Shanghai",
        "start_date": today.isoformat(),
        "end_date": end.isoformat(),
    }
    marine: Dict[str, Any]
    try:
        marine = fetch_json("https://marine-api.open-meteo.com/v1/marine", marine_params)
    except Exception:
        marine = {"hourly": {"time": []}}

    weather_by_day = aggregate_hourly_by_day(weather.get("hourly", {}))
    marine_by_day = aggregate_hourly_by_day(marine.get("hourly", {}))
    days_all = sorted(set(weather_by_day) | set(marine_by_day))

    daily_rows = []
    prev_rain: Optional[float] = None

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

        sst_score = 0.5 if sst is None else triangular(sst, low=preset["sst_peak"] - 8, peak=preset["sst_peak"], high=preset["sst_peak"] + 6)
        temp_score = 0.5 if temp is None else triangular(temp, low=preset["temp_peak"] - 10, peak=preset["temp_peak"], high=preset["temp_peak"] + 7)
        wind_score = 0.5 if wind is None else gaussian(wind, mu=preset["wind_peak"], sigma=2.7)
        onshore = onshore_score(wind_dir, preset["onshore_dir"])

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
            0.30 * sst_score
            + 0.17 * temp_score
            + 0.17 * wind_score
            + 0.14 * onshore
            + 0.11 * rain_score
            + 0.11 * current_score
        )
        raw_risk = clamp01(raw_risk + preset["bias"])

        calibrated_score = clamp01(0.70 * raw_risk + 0.30 * visibility)
        probability = clamp01(score_to_probability(calibrated_score, threshold=0.58, scale=0.085))
        level = classify_level(probability)
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
                    "wind_score": round(wind_score, 3),
                    "onshore_score": round(onshore, 3),
                    "rain_score": round(rain_score, 3),
                    "current_score": round(current_score, 3),
                    "raw_risk": round(raw_risk, 3),
                    "visibility": round(visibility, 3),
                    "calibrated_score": round(calibrated_score, 3),
                    "probability": round(probability, 3),
                },
                "level": level,
                "observation": observation,
            }
        )
        prev_rain = rain

    best_day = max(daily_rows, key=lambda x: x["scores"]["probability"]) if daily_rows else None
    hottest = max((row["scores"]["probability"] for row in daily_rows), default=0.0)
    today_prob = daily_rows[0]["scores"]["probability"] if daily_rows else 0.0

    return {
        "location": location,
        "region_preset": preset,
        "summary": {
            "today_probability": round(today_prob, 3),
            "best_probability": round(hottest, 3),
            "best_date": best_day["date"] if best_day else None,
            "best_level": best_day["level"] if best_day else None,
            "today_level": daily_rows[0]["level"] if daily_rows else None,
        },
        "daily": daily_rows,
    }


def load_locations(path: Path) -> List[Dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Milkyseas V2: 多地点荧光海预测")
    parser.add_argument("--days", type=int, default=7, help="预测天数，默认7")
    parser.add_argument("--locations", type=str, default=str(DEFAULT_LOCATIONS), help="地点JSON路径")
    parser.add_argument("--output", type=str, default=str(ROOT / "docs" / "data" / "latest.json"), help="输出JSON路径")
    parser.add_argument("--history-dir", type=str, default=str(ROOT / "docs" / "data" / "history"), help="历史快照目录")
    parser.add_argument("--workers", type=int, default=6, help="并发数")
    args = parser.parse_args()

    if args.days < 1 or args.days > 16:
        raise SystemExit("--days 建议在 1~16 之间")

    locations = load_locations(Path(args.locations))
    run_at = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    run_date = dt.date.today().isoformat()

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {pool.submit(build_location_forecast, loc, args.days): loc for loc in locations}
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
                            "best_level": "低",
                            "today_level": "低",
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

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

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
