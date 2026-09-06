const latestUrls = [
  `data/latest.json?v=${Date.now()}`,
  `data/latest_good.json?v=20260402`,
];

let probabilityChart;
let temperatureChart;
let seaTemperatureChart;
let windChart;
let map;
let mapMarkers = [];
let selectedIndex = 0;

const fmtPct = (v) => (v == null || Number.isNaN(v) ? '—' : `${(v * 100).toFixed(1)}%`);
const fmtNum = (v, digits = 1) => (v == null || Number.isNaN(v) ? '—' : `${Number(v).toFixed(digits)}`);
const fmtDate = (v) => (v ? v : '—');

function levelClass(level) {
  if (level === '高概率') return 'high';
  if (level === '中概率') return 'medium';
  return 'low';
}

function scoreLabel(level) {
  if (level === '高概率') return '高概率';
  if (level === '中概率') return '中概率';
  return '低概率';
}

function colorForProbability(v) {
  if (v >= 0.72) return '#fb7185';
  if (v >= 0.48) return '#fbbf24';
  return '#34d399';
}

function makeCard(row, idx) {
  const city = row.location.city;
  const prov = row.location.province;
  const today = row.summary.today_probability;
  const best = row.summary.best_probability;
  const bestDate = row.summary.best_date;
  const level = row.summary.today_level || '低概率';
  const ob = row.daily?.[0]?.observation || '—';
  return `
    <article class="city-card ${idx === selectedIndex ? 'active' : ''}" data-index="${idx}" role="button" tabindex="0">
      <div class="city-card__head">
        <div>
          <h3>${city}</h3>
          <div class="small">${prov} · ${row.location.group}</div>
        </div>
        <span class="badge ${levelClass(level)}">${scoreLabel(level)}</span>
      </div>
      <div class="city-card__score">
        <strong>${fmtPct(today)}</strong>
        <span>今日概率</span>
      </div>
      <div class="city-card__meta">最佳日期：<span class="kpi">${fmtDate(bestDate)}</span><br/>最佳概率：<span class="kpi">${fmtPct(best)}</span></div>
      <div class="city-card__foot">建议：${ob}</div>
    </article>
  `;
}

function populateSelect(locations) {
  const select = document.getElementById('city-select');
  select.innerHTML = locations.map((row, idx) => `<option value="${idx}">${row.location.city} · ${row.location.province}</option>`).join('');
  select.value = String(selectedIndex);
}

function buildLineChart(canvasId, label, series, color, fillColor, yOpts = {}) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;
  const labels = series.labels;
  const data = series.values;
  const datasets = [{
    label,
    data,
    borderColor: color,
    backgroundColor: fillColor || 'rgba(255,255,255,0.08)',
    fill: true,
    tension: 0.35,
    pointRadius: 3,
    pointHoverRadius: 5,
  }];
  if (window[canvasId + '_chart']) {
    window[canvasId + '_chart'].destroy();
  }
  window[canvasId + '_chart'] = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: true, labels: { color: '#edf4ff' } } },
      scales: {
        x: {
          ticks: { color: '#9db1cc' },
          grid: { color: 'rgba(255,255,255,0.05)' },
        },
        y: {
          beginAtZero: false,
          ticks: { color: '#9db1cc' },
          grid: { color: 'rgba(255,255,255,0.05)' },
          ...yOpts,
        },
      },
    },
  });
  return window[canvasId + '_chart'];
}

function buildProbabilityChart(row) {
  const labels = row.daily.map((d) => d.date.slice(5));
  const values = row.daily.map((d) => d.scores.probability);
  buildLineChart('probability-chart', `${row.location.city} 概率（前后 7 天）`, { labels, values }, 'rgba(76, 201, 240, 0.95)', 'rgba(76, 201, 240, 0.14)', {
    beginAtZero: true,
    max: 1,
    ticks: { callback: (v) => `${Math.round(v * 100)}%` },
  });
}

function buildTemperatureChart(row) {
  const labels = row.daily.map((d) => d.date.slice(5));
  const values = row.daily.map((d) => d.features.temperature_2m ?? null);
  buildLineChart('temperature-chart', `${row.location.city} 气温（前后 7 天）`, { labels, values }, 'rgba(124, 92, 255, 0.95)', 'rgba(124, 92, 255, 0.14)');
}

function buildSeaTemperatureChart(row) {
  const labels = row.daily.map((d) => d.date.slice(5));
  const values = row.daily.map((d) => d.features.sea_surface_temperature ?? null);
  buildLineChart('sea-temperature-chart', `${row.location.city} 海温（前后 7 天）`, { labels, values }, 'rgba(52, 211, 153, 0.95)', 'rgba(52, 211, 153, 0.14)');
}

function buildWindChart(row) {
  const labels = row.daily.map((d) => d.date.slice(5));
  const values = row.daily.map((d) => d.features.wind_direction_10m ?? null);
  buildLineChart('wind-chart', `${row.location.city} 风向（前后 7 天）`, { labels, values }, 'rgba(251, 191, 36, 0.95)', 'rgba(251, 191, 36, 0.14)', {
    min: 0,
    max: 360,
    ticks: { callback: (v) => `${v}°` },
  });
}

function mapPointColor(prob) {
  if (prob >= 0.72) return '#fb7185';
  if (prob >= 0.48) return '#fbbf24';
  return '#34d399';
}

function createMap(locations) {
  if (map) return;
  map = L.map('map', { scrollWheelZoom: false }).setView([28, 121], 4.5);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors',
    maxZoom: 18,
  }).addTo(map);

  const markerGroup = L.layerGroup().addTo(map);
  mapMarkers = locations.map((row, idx) => {
    const prob = row.summary.today_probability ?? 0;
    const marker = L.circleMarker([row.location.lat, row.location.lon], {
      radius: idx === selectedIndex ? 13 : 8 + prob * 10,
      color: idx === selectedIndex ? '#ffffff' : mapPointColor(prob),
      weight: idx === selectedIndex ? 3 : 2,
      fillColor: mapPointColor(prob),
      fillOpacity: idx === selectedIndex ? 0.95 : 0.75,
    });
    marker.bindPopup(`<strong>${row.location.city}</strong><br/>今日概率：${fmtPct(prob)}<br/>最佳日期：${fmtDate(row.summary.best_date)}`);
    marker.on('click', () => selectCity(idx, locations));
    markerGroup.addLayer(marker);
    return marker;
  });
}

function refreshMap(locations) {
  if (!mapMarkers.length) return;
  mapMarkers.forEach((marker, idx) => {
    const row = locations[idx];
    const prob = row.summary.today_probability ?? 0;
    const selected = idx === selectedIndex;
    marker.setStyle({
      radius: selected ? 13 : 8 + prob * 10,
      color: selected ? '#ffffff' : mapPointColor(prob),
      weight: selected ? 3 : 2,
      fillColor: mapPointColor(prob),
      fillOpacity: selected ? 0.98 : 0.75,
    });
  });
  const row = locations[selectedIndex];
  if (row) {
    map.setView([row.location.lat, row.location.lon], 6, { animate: true });
    mapMarkers[selectedIndex].openPopup();
  }
}

function renderSelected(row) {
  const todayDate = row.summary.today_date;
  const todayRow = row.daily.find((d) => d.date === todayDate) || row.daily[row.summary.today_index ?? 0] || row.daily[0];
  document.getElementById('selected-title').textContent = `${row.location.city} · ${row.location.province}`;
  document.getElementById('selected-caption').textContent = `地区：${row.location.group}，时间窗为前后 7 天；今日概率 ${fmtPct(row.summary.today_probability)}，最佳日期 ${fmtDate(row.summary.best_date)}。`;
  document.getElementById('detail-summary').innerHTML = `
    <h3>${row.location.city}</h3>
    <div class="summary-value ${levelClass(row.summary.today_level || '低概率')}">${fmtPct(row.summary.today_probability)}</div>
    <div class="summary-line">今日等级：${scoreLabel(row.summary.today_level || '低概率')} · 最佳日期：${fmtDate(row.summary.best_date)} · 最佳概率：${fmtPct(row.summary.best_probability)}</div>
    <div class="summary-grid">
      <div class="summary-chip"><span>地区</span>${row.location.group}</div>
      <div class="summary-chip"><span>天然权重</span>${fmtNum(row.geo_prior, 2)}</div>
      <div class="summary-chip"><span>今日日期</span>${fmtDate(todayDate)}</div>
      <div class="summary-chip"><span>今日气温</span>${fmtNum(todayRow?.features?.temperature_2m, 1)} °C</div>
      <div class="summary-chip"><span>今日海温</span>${fmtNum(todayRow?.features?.sea_surface_temperature, 1)} °C</div>
      <div class="summary-chip"><span>今日风速</span>${fmtNum(todayRow?.features?.wind_speed_10m, 1)} m/s</div>
    </div>
    <div class="summary-line">观察建议：${todayRow?.observation || '—'} · 历史段用于升温背景，未来段用于高概率预测。</div>
  `;
  buildProbabilityChart(row);
  buildTemperatureChart(row);
  buildSeaTemperatureChart(row);
  buildWindChart(row);
}

function selectCity(idx, locations) {
  selectedIndex = idx;
  const select = document.getElementById('city-select');
  select.value = String(idx);
  document.querySelectorAll('.city-card').forEach((el) => el.classList.toggle('active', Number(el.dataset.index) === idx));
  renderSelected(locations[idx]);
  refreshMap(locations);
}

async function loadData() {
  let data = null;
  let lastErr = null;
  for (const url of latestUrls) {
    try {
      const response = await fetch(url, { cache: 'no-store' });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const candidate = await response.json();
      const locations = candidate.locations || [];
      const hasData = locations.some((r) => r?.summary?.today_probability != null);
      if (!hasData) throw new Error('no usable location data');
      data = candidate;
      break;
    } catch (err) {
      lastErr = err;
    }
  }
  if (!data) throw lastErr || new Error('unable to load data');
  const locations = data.locations || [];

  document.getElementById('meta-generated').textContent = `更新于 ${data.meta.generated_at}`;
  document.getElementById('meta-model').textContent = `模型：${data.meta.model} · ${data.meta.version}`;
  document.getElementById('meta-source').textContent = `数据源：Open-Meteo Weather / Marine`;
  document.getElementById('stat-date').textContent = data.meta.generated_date;

  const todayHigh = locations.filter((r) => (r.summary.today_probability ?? 0) >= 0.72).length;
  const todayMedium = locations.filter((r) => (r.summary.today_probability ?? 0) >= 0.48 && (r.summary.today_probability ?? 0) < 0.72).length;
  document.getElementById('stat-high').textContent = `${todayHigh}`;
  document.getElementById('stat-medium').textContent = `${todayMedium}`;
  document.getElementById('stat-count').textContent = `${locations.length}`;

  const bestToday = locations.find((r) => r.summary.today_probability != null) || locations[0];
  const bestObserve = [...locations].filter((r) => r.summary.today_probability != null).sort((a, b) => b.summary.today_probability - a.summary.today_probability)[0] || locations[0];

  if (bestToday) {
    document.getElementById('today-top-city').textContent = bestToday.location.city;
    document.getElementById('today-top-score').textContent = fmtPct(bestToday.summary.today_probability);
  }
  if (bestObserve) {
    document.getElementById('today-top-observe').textContent = bestObserve.location.city;
    document.getElementById('today-top-observe-score').textContent = `${fmtPct(bestObserve.summary.today_probability)} · ${scoreLabel(bestObserve.summary.today_level)}`;
  }

  const labels = locations.map((r) => r.location.city);
  const values = locations.map((r) => r.summary.today_probability ?? 0);
  populateSelect(locations);
  createMap(locations);

  const cardsGrid = document.getElementById('cards-grid');
  cardsGrid.innerHTML = locations.map(makeCard).join('');
  document.querySelectorAll('.city-card').forEach((card) => {
    const activate = () => selectCity(Number(card.dataset.index), locations);
    card.addEventListener('click', activate);
    card.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') activate(); });
  });

  const tableBody = document.getElementById('ranking-body');
  tableBody.innerHTML = locations.map((row) => `
    <tr>
      <td>${row.location.city}</td>
      <td>${row.location.group}</td>
      <td>${fmtPct(row.summary.today_probability)}</td>
      <td><span class="badge ${levelClass(row.summary.today_level || '低概率')}">${scoreLabel(row.summary.today_level || '低概率')}</span></td>
      <td>${fmtDate(row.summary.best_date)}</td>
      <td>${fmtPct(row.summary.best_probability)}</td>
      <td><span class="badge ${levelClass(row.summary.best_level || '低概率')}">${scoreLabel(row.summary.best_level || '低概率')}</span></td>
    </tr>
  `).join('');

  renderSelected(locations[selectedIndex]);
  refreshMap(locations);

  document.getElementById('city-select').addEventListener('change', (e) => {
    selectCity(Number(e.target.value), locations);
  });
}

loadData().catch((err) => {
  console.error(err);
  document.body.insertAdjacentHTML('afterbegin', `<div style="padding:16px;color:#fff;background:#8b1f2d">数据加载失败：${err.message}</div>`);
});
