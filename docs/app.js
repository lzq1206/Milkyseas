const latestUrl = 'data/latest.json';

let rankingChart;
let trendChart;

const fmtPct = (v) => (v == null || Number.isNaN(v) ? '—' : `${(v * 100).toFixed(1)}%`);
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

function makeCard(row) {
  const city = row.location.city;
  const prov = row.location.province;
  const today = row.summary.today_probability;
  const best = row.summary.best_probability;
  const bestDate = row.summary.best_date;
  const level = row.summary.today_level || '低';
  const ob = row.daily?.[0]?.observation || '—';
  return `
    <article class="city-card">
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
      <div class="city-card__meta">最佳观测日：<span class="kpi">${fmtDate(bestDate)}</span><br/>最佳概率：<span class="kpi">${fmtPct(best)}</span></div>
      <div class="city-card__foot">建议：${ob}</div>
    </article>
  `;
}

function populateSelect(locations) {
  const select = document.getElementById('city-select');
  select.innerHTML = locations.map((row, idx) => `<option value="${idx}">${row.location.city} · ${row.location.province}</option>`).join('');
}

function buildRankingChart(labels, data) {
  const ctx = document.getElementById('ranking-chart');
  if (rankingChart) rankingChart.destroy();
  rankingChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: '今日概率',
        data,
        borderWidth: 1,
        backgroundColor: data.map((v) => v >= 0.72 ? 'rgba(251, 113, 133, 0.85)' : v >= 0.48 ? 'rgba(251, 191, 36, 0.82)' : 'rgba(52, 211, 153, 0.82)'),
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => `概率：${(ctx.raw * 100).toFixed(1)}%`,
          },
        },
      },
      scales: {
        x: {
          ticks: { color: '#9db1cc' },
          grid: { color: 'rgba(255,255,255,0.05)' },
        },
        y: {
          beginAtZero: true,
          max: 1,
          ticks: {
            color: '#9db1cc',
            callback: (v) => `${Math.round(v * 100)}%`,
          },
          grid: { color: 'rgba(255,255,255,0.05)' },
        },
      },
    },
  });
}

function buildTrendChart(row) {
  const ctx = document.getElementById('trend-chart');
  const labels = row.daily.map((d) => d.date.slice(5));
  const probs = row.daily.map((d) => d.scores.probability);
  if (trendChart) trendChart.destroy();
  trendChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: `${row.location.city} 概率`,
        data: probs,
        borderColor: 'rgba(76, 201, 240, 0.95)',
        backgroundColor: 'rgba(76, 201, 240, 0.15)',
        fill: true,
        tension: 0.35,
        pointRadius: 4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: '#edf4ff' } },
      },
      scales: {
        x: {
          ticks: { color: '#9db1cc' },
          grid: { color: 'rgba(255,255,255,0.05)' },
        },
        y: {
          beginAtZero: true,
          max: 1,
          ticks: {
            color: '#9db1cc',
            callback: (v) => `${Math.round(v * 100)}%`,
          },
          grid: { color: 'rgba(255,255,255,0.05)' },
        },
      },
    },
  });
}

function renderSelected(row) {
  document.getElementById('selected-title').textContent = `${row.location.city} · ${row.location.province}`;
  document.getElementById('selected-caption').textContent = `地区：${row.location.group}，今日概率 ${fmtPct(row.summary.today_probability)}，最佳日期 ${fmtDate(row.summary.best_date)}。`;
  buildTrendChart(row);
}

async function loadData() {
  const response = await fetch(latestUrl, { cache: 'no-store' });
  const data = await response.json();
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

  const cardsGrid = document.getElementById('cards-grid');
  cardsGrid.innerHTML = locations.map(makeCard).join('');

  const tableBody = document.getElementById('ranking-body');
  tableBody.innerHTML = locations.map((row) => `
    <tr>
      <td>${row.location.city}</td>
      <td>${row.location.group}</td>
      <td>${fmtPct(row.summary.today_probability)}</td>
      <td><span class="badge ${levelClass(row.summary.today_level || '低')}">${scoreLabel(row.summary.today_level || '低')}</span></td>
      <td>${fmtDate(row.summary.best_date)}</td>
      <td>${fmtPct(row.summary.best_probability)}</td>
      <td><span class="badge ${levelClass(row.summary.best_level || '低')}">${scoreLabel(row.summary.best_level || '低')}</span></td>
    </tr>
  `).join('');

  const labels = locations.map((r) => r.location.city);
  const values = locations.map((r) => r.summary.today_probability ?? 0);
  buildRankingChart(labels, values);
  populateSelect(locations);
  renderSelected(locations[0]);

  document.getElementById('city-select').addEventListener('change', (e) => {
    const idx = Number(e.target.value);
    renderSelected(locations[idx]);
  });
}

loadData().catch((err) => {
  console.error(err);
  document.body.insertAdjacentHTML('afterbegin', `<div style="padding:16px;color:#fff;background:#8b1f2d">数据加载失败：${err.message}</div>`);
});
