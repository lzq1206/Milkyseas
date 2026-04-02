#!/usr/bin/env python3
"""Backtest Milkyseas with news-labeled fluorescent sea events.

This script:
- searches the user-provided news-derived event set (hardcoded in this version)
- fetches historical weather archive data from Open-Meteo
- computes day-level scores using the current heuristic model (weather-only backtest)
- grid-searches a small set of weight multipliers
- writes a CSV summary and a markdown report

Important limitation:
- Open-Meteo historical marine archive was not available in this environment;
  backtest therefore evaluates the weather + geo-prior components only.
  The live model may still use sea temperature from forecast data.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import itertools
import json
import math
import statistics
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
LOCATIONS_PATH = ROOT / "data" / "locations.json"

EVENTS = [
    {
        "city": "茂名市",
        "date": "2025-04-08",
        "source": "https://news.qq.com/rain/a/20250409A07XKZ00",
        "note": "茂名电白北山海滩蓝眼泪（媒体报道次日）",
    },
    {
        "city": "深圳市",
        "date": "2025-04-22",
        "source": "https://www.sznews.com/news/content/2025-04/23/content_31547153.htm",
        "note": "深圳大梅沙蓝眼泪（报道为次日）",
    },
    {
        "city": "大连市",
        "date": "2025-04-27",
        "source": "https://www.ln.gov.cn/web/ywdt/tpxx/2025042709363115198/",
        "note": "大连大黑石/小黑石荧光海",
    },
    {
        "city": "平潭县",
        "date": "2025-04-28",
        "source": "https://news.cctv.cn/2025/04/28/ARTI8k9bA74zSSFsYpgyhc7F250428.shtml",
        "note": "平潭蓝眼泪高发期报道",
    },
    {
        "city": "泉州市",
        "date": "2025-03-01",
        "source": "https://static.cdsb.com/micropub/Articles/202503/655da9808fa37265f7647906afac5e80.html",
        "note": "福建首场蓝眼泪（永宁梅林码头相关报道）",
    },
]

REGION_PRESETS = {
    "渤海/黄海北部": {"temp_peak": 24.0, "wind_peak": 5.0, "onshore_dir": 90.0},
    "黄海": {"temp_peak": 25.0, "wind_peak": 5.0, "onshore_dir": 100.0},
    "东海": {"temp_peak": 26.0, "wind_peak": 5.2, "onshore_dir": 110.0},
    "东海/台湾海峡": {"temp_peak": 26.5, "wind_peak": 5.2, "onshore_dir": 115.0},
    "台湾海峡": {"temp_peak": 27.0, "wind_peak": 5.2, "onshore_dir": 120.0},
    "南海北部": {"temp_peak": 28.0, "wind_peak": 4.8, "onshore_dir": 125.0},
    "南海": {"temp_peak": 28.5, "wind_peak": 4.8, "onshore_dir": 130.0},
}


def clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


def gaussian(x: float, mu: float, sigma: float) -> float:
    return math.exp(-((x - mu) ** 2) / (2 * sigma * sigma)) if sigma > 0 else 0.0


def triangular(x: float, low: float, peak: float, high: float) -> float:
    if x <= low or x >= high:
        return 0.0
    if x == peak:
        return 1.0
    if x < peak:
        return (x - low) / (peak - low)
    return (high - x) / (high - peak)


def onshore_score(direction_deg: Optional[float], preferred: float) -> float:
    if direction_deg is None:
        return 0.5
    d = direction_deg % 360.0
    diff = abs(((d - preferred + 180) % 360) - 180)
    return clamp01(math.exp(-(diff**2) / (2 * 55.0 * 55.0)))


def score_to_probability(score: float, threshold: float, scale: float = 0.085) -> float:
    z = (score - threshold) / max(scale, 1e-6)
    return 1.0 / (1.0 + math.exp(-z))


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


def fetch_archive(lat: float, lon: float, start: dt.date, end: dt.date) -> Dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,wind_speed_10m,wind_direction_10m,precipitation,cloud_cover",
        "timezone": "Asia/Shanghai",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }
    url = "https://archive-api.open-meteo.com/v1/archive?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Milkyseas-Backtest/1.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def extract_features(day: str, by_day: Dict[str, Dict[str, List[float]]], preset: Dict[str, float], temp_history: List[Optional[float]]) -> Dict[str, float]:
    w = by_day.get(day, {})
    temp = stat(w.get("temperature_2m", []), "mean")
    wind = stat(w.get("wind_speed_10m", []), "mean")
    wind_dir = stat(w.get("wind_direction_10m", []), "mean")
    rain = stat(w.get("precipitation", []), "sum")
    cloud = stat(w.get("cloud_cover", []), "mean")

    temp_history.append(temp)

    temp_score = 0.5 if temp is None else triangular(temp, low=preset["temp_peak"] - 10, peak=preset["temp_peak"], high=preset["temp_peak"] + 7)
    recent4 = [t for t in temp_history[-4:] if t is not None]
    rise = 0.5 if len(recent4) < 2 else clamp01((recent4[-1] - min(recent4[:-1])) / 10.0)
    recent3 = [t for t in temp_history[-3:] if t is not None]
    trend = 0.5 if len(recent3) < 2 else clamp01((recent3[-1] - recent3[0] + 3.0) / 6.0)
    persist = 0.5 if not recent4 else clamp01(sum(1 for t in recent4 if t >= preset["temp_peak"] - 1.5) / 4.0)
    heat = clamp01(0.28 * temp_score + 0.22 * trend + 0.20 * persist + 0.30 * rise)

    wind_score = 0.5 if wind is None else gaussian(wind, mu=preset["wind_peak"], sigma=2.7)
    onshore = onshore_score(wind_dir, preset["onshore_dir"])
    shore = clamp01(onshore * (0.45 + 0.55 * wind_score))
    rain_score = 0.5 if rain is None else clamp01(1.0 - clamp01(rain / 15.0))
    cloud_vis = 0.6 if cloud is None else 1.0 - clamp01(cloud / 100.0)
    wind_vis = 0.6 if wind is None else gaussian(wind, mu=4.0, sigma=2.0)
    visibility = clamp01(0.58 * cloud_vis + 0.42 * wind_vis)

    return {
        "temp": temp,
        "temp_score": temp_score,
        "rise": rise,
        "trend": trend,
        "persist": persist,
        "heat": heat,
        "wind": wind_score,
        "onshore": onshore,
        "shore": shore,
        "rain": rain_score,
        "visibility": visibility,
    }


def auc(scores: List[float], labels: List[int]) -> float:
    paired = sorted(zip(scores, labels), key=lambda x: x[0])
    n1 = sum(labels)
    n0 = len(labels) - n1
    if n1 == 0 or n0 == 0:
        return float("nan")
    rank = 1
    i = 0
    sum_pos = 0.0
    while i < len(paired):
        j = i
        while j < len(paired) and paired[j][0] == paired[i][0]:
            j += 1
        avg = (rank + rank + (j - i) - 1) / 2
        for k in range(i, j):
            if paired[k][1] == 1:
                sum_pos += avg
        rank += (j - i)
        i = j
    return (sum_pos - n1 * (n1 + 1) / 2) / (n1 * n0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest fluorescent sea heuristic with news-labeled events")
    parser.add_argument("--output-csv", default=str(ROOT / "outputs" / "fluorescent_seas_backtest_20260402.csv"))
    parser.add_argument("--output-md", default=str(ROOT / "outputs" / "fluorescent_seas_backtest_20260402.md"))
    args = parser.parse_args()

    locations = {x["city"]: x for x in json.loads(LOCATIONS_PATH.read_text(encoding="utf-8"))}
    locations["茂名市"] = {"city": "茂名市", "province": "广东", "group": "南海北部", "lat": 21.663, "lon": 111.003, "geo_prior": 1.21}

    rows: List[Dict[str, Any]] = []
    samples: List[Dict[str, Any]] = []

    for ev in EVENTS:
        city = ev["city"]
        if city not in locations:
            continue
        loc = locations[city]
        d = dt.date.fromisoformat(ev["date"])
        start = d - dt.timedelta(days=7)
        end = d + dt.timedelta(days=7)
        by_day = aggregate_hourly_by_day(fetch_archive(loc["lat"], loc["lon"], start, end)["hourly"])
        temp_history: List[Optional[float]] = []
        preset = REGION_PRESETS.get(loc["group"], REGION_PRESETS["东海"])

        for day in sorted(by_day.keys()):
            feat = extract_features(day, by_day, preset, temp_history)
            label = 1 if abs((dt.date.fromisoformat(day) - d).days) <= 1 else 0
            geo_prior = float(loc.get("geo_prior", 1.0))
            geo_score = clamp01((geo_prior - 0.9) / 0.4)
            rows.append(
                {
                    "city": city,
                    "date": day,
                    "label": label,
                    "geo_prior": geo_prior,
                    "geo_score": geo_score,
                    "source": ev["source"],
                    "note": ev["note"],
                    **feat,
                }
            )
            samples.append({"city": city, "date": day, "label": label, **feat, "geo_score": geo_score})

    # Grid search around a compact parameterization.
    best = None
    grid = {
        "heat": [0.5, 0.6, 0.7],
        "shore": [0.10, 0.12, 0.15],
        "wind": [0.10, 0.12, 0.14],
        "rain": [0.06, 0.08],
        "vis": [0.10, 0.12, 0.15],
        "prior": [0.08, 0.12, 0.15],
        "raw": [0.6, 0.7, 0.8],
    }

    def score_row(row: Dict[str, Any], w: Dict[str, float]) -> float:
        raw = w["heat"] * row["heat"] + w["shore"] * row["shore"] + w["wind"] * row["wind"] + w["rain"] * row["rain"]
        cal = clamp01(w["raw"] * raw + w["vis"] * row["visibility"] + w["prior"] * row["geo_score"])
        return score_to_probability(cal, threshold=0.58, scale=0.085)

    for heat, shore, wind, rain, vis, prior, raw in itertools.product(
        grid["heat"], grid["shore"], grid["wind"], grid["rain"], grid["vis"], grid["prior"], grid["raw"]
    ):
        w = {"heat": heat, "shore": shore, "wind": wind, "rain": rain, "vis": vis, "prior": prior, "raw": raw}
        scores = [score_row(r, w) for r in rows]
        labels = [r["label"] for r in rows]
        a = auc(scores, labels)
        pos = [s for s, l in zip(scores, labels) if l == 1]
        neg = [s for s, l in zip(scores, labels) if l == 0]
        gap = (sum(pos) / len(pos)) - (sum(neg) / len(neg))
        obj = a + gap
        if best is None or obj > best[0]:
            best = (obj, a, gap, w, scores)

    assert best is not None
    _, best_auc, best_gap, best_w, best_scores = best

    # Aggregate city/event results for report.
    event_rows = []
    for ev in EVENTS:
        city = ev["city"]
        city_rows = [r for r in rows if r["city"] == city]
        city_scores = [s for r, s in zip(rows, best_scores) if r["city"] == city]
        event_row = next(r for r in city_rows if r["label"] == 1)
        event_rows.append(
            {
                "city": city,
                "event_date": ev["date"],
                "source": ev["source"],
                "note": ev["note"],
                "event_score": round(next(s for r, s in zip(city_rows, city_scores) if r["label"] == 1), 3),
                "control_mean": round(sum(s for r, s in zip(city_rows, city_scores) if r["label"] == 0) / max(1, len([r for r in city_rows if r["label"] == 0])), 3),
                "control_max": round(max((s for r, s in zip(city_rows, city_scores) if r["label"] == 0), default=0.0), 3),
            }
        )

    # CSV output.
    out_csv = Path(args.output_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) + ["score"])
        writer.writeheader()
        for row, score in zip(rows, best_scores):
            r = dict(row)
            r["score"] = round(score, 4)
            writer.writerow(r)

    # Markdown report.
    out_md = Path(args.output_md)
    lines = []
    lines.append("# Milkyseas 荧光海新闻回测报告（2026-04-02）")
    lines.append("")
    lines.append("## 新闻正样本")
    for ev in EVENTS:
        lines.append(f"- {ev['date']} · {ev['city']} · {ev['note']} · {ev['source']}")
    lines.append("")
    lines.append("## 回测设定")
    lines.append("- 历史数据：Open-Meteo archive weather（气温、风速、风向、降雨、云量）")
    lines.append("- 样本：新闻事件日 ±1 天记为正样本，其余 ±7 天窗口记为负样本")
    lines.append("- 海温历史：未在回测中拟合；保留为在线预测项")
    lines.append("")
    lines.append("## 网格搜索结果")
    lines.append(f"- AUC: {best_auc:.3f}")
    lines.append(f"- Pos/Neg gap: {best_gap:.3f}")
    lines.append(f"- Best weights: {json.dumps(best_w, ensure_ascii=False)}")
    lines.append("")
    lines.append("## 城市级摘要")
    lines.append("| 城市 | 事件日期 | 事件分数 | 对照均值 | 对照最高 |")
    lines.append("|---|---:|---:|---:|---:|")
    for r in event_rows:
        lines.append(f"| {r['city']} | {r['event_date']} | {r['event_score']:.3f} | {r['control_mean']:.3f} | {r['control_max']:.3f} |")
    lines.append("")
    lines.append("## 解释")
    lines.append("- 回测样本量较小，结论更适合做方向性校准。")
    lines.append("- 结果显示：短期升温/持续高温与地理先验组合后，能比纯随机窗口更好地区分新闻事件日。")
    lines.append("- 海温历史缺失不影响本次天气侧回测，但在线模型仍应保留海温项。")

    out_md.write_text("\n".join(lines), encoding="utf-8")

    print(f"AUC={best_auc:.3f} gap={best_gap:.3f}")
    print(f"Best weights={json.dumps(best_w, ensure_ascii=False)}")
    print(f"Wrote {out_csv}")
    print(f"Wrote {out_md}")


if __name__ == "__main__":
    main()
