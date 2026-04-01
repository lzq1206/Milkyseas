# Milkyseas V2

多地点荧光海预测与可视化站点。

## 功能

- 每日抓取公开天气/海洋预报
- 输出 20 个中国沿海/近海城市的荧光海风险评分
- 自动生成 GitHub Pages 可视化页面
- GitHub Actions 每天定时更新 `docs/data/latest.json`

## 覆盖城市

大连市、秦皇岛、天津市、东营市、青岛市、威海市、舟山市、台州市、温州市、宁波市、平潭县、宁德市、厦门市、泉州市、深圳市、珠海市、惠州市、汕头市、陵水县、三亚市。

## 本地运行

```bash
python scripts/fetch_fluorescent_seas.py --days 7 --output docs/data/latest.json --history-dir docs/data/history
```

然后直接打开 `docs/index.html` 或使用本地静态服务器查看：

```bash
python -m http.server -d docs 8000
```

## 部署说明

- GitHub Actions 工作流：`.github/workflows/update_pages.yml`
- Pages 输出目录：`docs/`
- 数据快照：`docs/data/latest.json`
- 历史快照：`docs/data/history/`

## 模型口径

这是一个“可校准模板版”预测器：

- 输入：公开天气、海温、海流、风向风速、降雨、云量
- 输出：今日概率、最佳日期、风险等级、趋势图
- 定位：先可用、可复核、可扩展，再做历史标签监督训练

> 说明：结果仅表示观测机会的启发式估计，不构成保证。
