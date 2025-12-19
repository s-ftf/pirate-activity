const chartEl = document.getElementById("swapChart");
const tfPicker = document.getElementById("timeframe-picker");
const summaryTable = document.getElementById("summary-table");
const summaryBadges = document.getElementById("summary-badges");
const pageToggles = document.querySelectorAll(".page-toggle");
const activityChartEl = document.getElementById("activityChart");
const activityPicker = document.getElementById("activity-timeframe");
const activityBadges = document.getElementById("activity-badges");
const activityFilters = document.getElementById("activity-filters");
const activityTable = document.getElementById("activity-table");
const activityCards = document.getElementById("activity-cards");
const minersTable = document.getElementById("miners-table");
const notariesTable = document.getElementById("notaries-table");
let minersData = null;
let notariesData = null;

let swapChart;
let currentRange = "30";
let activityChart;
let activityRange = "30";
let activityDataCache = null;

async function loadData(range) {
  const url = `data/swaps_${range}.json`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to load ${url}`);
  return res.json();
}

function formatNum(n, decimals = 2) {
  return Number(n).toLocaleString(undefined, {
    minimumFractionDigits: n === 0 ? 0 : 0,
    maximumFractionDigits: decimals,
  });
}

function percentile(values, p) {
  if (!values || !values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = (sorted.length - 1) * (p / 100);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  const weight = idx - lo;
  return sorted[lo] * (1 - weight) + sorted[hi] * weight;
}

function updateBadges(meta) {
  if (!summaryBadges) return;
  summaryBadges.innerHTML = "";
  const items = [
    { label: "Swaps", value: meta.total_swaps },
    { label: "ARRR swapped", value: meta.total_amount },
    { label: "Tx fees", value: meta.total_fees, decimals: 6 },
    { label: "Avg/day", value: meta.avg_swaps_per_day },
  ];
  items.forEach((item) => {
    const div = document.createElement("div");
    div.className = "badge";
    div.innerHTML = `<strong>${formatNum(item.value, item.decimals || 2)}</strong> ${item.label}`;
    summaryBadges.appendChild(div);
  });
}

function renderTable(meta) {
  if (!summaryTable) return;
  const rows = [
    { label: "Total swaps", value: meta.total_swaps },
    { label: "Total ARRR swapped", value: meta.total_amount },
    { label: "Total tx fees", value: meta.total_fees, decimals: 6 },
    { label: "Avg swap size", value: meta.avg_swap_amount },
    { label: "Avg swaps/day", value: meta.avg_swaps_per_day },
    { label: "Avg fee/swaps", value: meta.avg_fee_per_swap, decimals: 6 },
    { label: "Max swaps in a day", value: meta.max_swaps_day.count, note: meta.max_swaps_day.date },
    { label: "Max ARRR swapped in a day", value: meta.max_amount_day.amount, note: meta.max_amount_day.date },
    { label: "Max ARRR in a swap", value: meta.max_single_swap },
    { label: "Median swap size", value: meta.median_swap_amount },
  ];
  summaryTable.innerHTML = rows
    .map(
      (r) => `
        <div class="summary-card">
          <div class="label">${r.label}${r.note ? ` (${r.note})` : ""}</div>
          <div class="value">${formatNum(r.value || 0, r.decimals || 2)}</div>
        </div>`
    )
    .join("");
}

function renderChart(data) {
  if (!chartEl) return;
  const labels = data.series.map((p) => p.date);
  const counts = data.series.map((p) => p.swaps);
  const amounts = data.series.map((p) => p.amount);
  const ctx = chartEl.getContext("2d");

  if (swapChart) swapChart.destroy();

  swapChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          type: "bar",
          label: "ARRR swapped",
          data: amounts,
          backgroundColor: "rgba(45, 225, 194, 0.35)",
          borderColor: "rgba(45, 225, 194, 0.8)",
          borderWidth: 1,
          yAxisID: "y1",
        },
        {
          type: "line",
          label: "Swaps",
          data: counts,
          borderColor: "rgba(124, 93, 250, 0.9)",
          backgroundColor: "rgba(124, 93, 250, 0.2)",
          tension: 0.25,
          yAxisID: "y",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          position: "left",
          ticks: { color: "#e8ecf5" },
          grid: { color: "rgba(255,255,255,0.05)" },
        },
        y1: {
          position: "right",
          ticks: { color: "#e8ecf5" },
          grid: { display: false },
        },
        x: {
          ticks: { color: "#e8ecf5", maxRotation: 0, autoSkip: true, maxTicksLimit: 12 },
          grid: { color: "rgba(255,255,255,0.03)" },
        },
      },
      plugins: {
        legend: { labels: { color: "#e8ecf5" } },
        tooltip: {
          callbacks: {
            label: function (ctx) {
              const label = ctx.dataset.label || "";
              const value = ctx.formattedValue;
              return `${label}: ${value}`;
            },
          },
        },
      },
    },
  });
}

async function refresh(range) {
  currentRange = range;
  document.querySelectorAll("#timeframe-picker button").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.range === range);
  });
  try {
    const data = await loadData(range);
    updateBadges(data.meta);
    renderTable(data.meta);
    renderChart(data);
  } catch (err) {
    console.error(err);
    alert("Failed to load data for range " + range);
  }
}

if (chartEl && tfPicker) {
  tfPicker.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-range]");
    if (!btn) return;
    refresh(btn.dataset.range);
  });
}

if (chartEl && tfPicker) {
  refresh(currentRange);
}

// Activity dashboard (index)
async function loadActivity(range) {
  const url = `data/activity_${range}.json`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to load ${url}`);
  return res.json();
}

function buildActivitySeries(data, activeCats) {
  const labels = data.series.map((d) => d.date);
  const counts = [];
  const fees = [];
  data.series.forEach((day) => {
    let dayTx = 0;
    let dayFee = 0;
    activeCats.forEach((c) => {
      const v = (day.categories && day.categories[c]) || { tx: 0, fee: 0 };
      dayTx += v.tx || 0;
      dayFee += v.fee || 0;
    });
    counts.push(dayTx);
    fees.push(dayFee);
  });
  return { labels, counts, fees };
}

function renderActivityChart(data, activeCats) {
  if (!activityChartEl) return;
  const series = buildActivitySeries(data, activeCats);
  const denseSeries = series.labels.length > 400;
  const isAllRange = activityRange === "all";
  const feeCap = isAllRange ? percentile(series.fees, 99) : null;
  const feeMaxForDisplay = feeCap ? feeCap * 1.1 : null;
  const chartFees = feeMaxForDisplay ? series.fees.map((f) => Math.min(f, feeMaxForDisplay)) : series.fees;
  const feeBarOptions = denseSeries
    ? {
        // Force a visible thickness when there are thousands of days (e.g. "All" range)
        barThickness: Math.max(
          2,
          Math.floor((activityChartEl.clientWidth || 0) / series.labels.length)
        ),
        categoryPercentage: 1,
        barPercentage: 1,
      }
    : {};
  const ctx = activityChartEl.getContext("2d");
  if (activityChart) activityChart.destroy();
  activityChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: series.labels,
      datasets: [
        {
          type: "bar",
          label: "Tx fees",
          // Clamp extreme outliers on the "All" view so average bars stay visible
          data: chartFees,
          backgroundColor: "rgba(45, 225, 194, 0.35)",
          borderColor: "rgba(45, 225, 194, 0.8)",
          borderWidth: denseSeries ? 0 : 1,
          yAxisID: "y1",
          order: 0,
          ...feeBarOptions,
        },
        {
          type: "line",
          label: "Transactions",
          data: series.counts,
          borderColor: "rgba(124, 93, 250, 0.9)",
          backgroundColor: "rgba(124, 93, 250, 0.2)",
          tension: 0.25,
          yAxisID: "y",
          order: 1,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          position: "left",
          ticks: { color: "#e8ecf5" },
          grid: { color: "rgba(255,255,255,0.05)" },
        },
        y1: {
          position: "right",
          ticks: { color: "#e8ecf5" },
          grid: { display: false },
          max: feeMaxForDisplay || undefined,
        },
        x: {
          ticks: { color: "#e8ecf5", maxRotation: 0, autoSkip: true, maxTicksLimit: 12 },
          grid: { color: "rgba(255,255,255,0.03)" },
        },
      },
      plugins: {
        legend: { labels: { color: "#e8ecf5" } },
      },
    },
  });
}

function renderActivityBadges(meta) {
  if (!activityBadges) return;
  activityBadges.innerHTML = "";
  const items = [
    { label: "Total tx", value: meta.totals.total_tx },
    { label: "Tx fees", value: meta.totals.total_fees, decimals: 6 },
    { label: "Avg tx/day", value: meta.totals.avg_tx_per_day },
    { label: "Avg fees/day", value: meta.totals.avg_fees_per_day, decimals: 6 },
  ];
  items.forEach((item) => {
    const div = document.createElement("div");
    div.className = "badge";
    div.innerHTML = `<strong>${formatNum(item.value, item.decimals || 2)}</strong> ${item.label}`;
    activityBadges.appendChild(div);
  });
}

function renderActivityCards(meta) {
  if (!activityCards) return;
  activityCards.innerHTML = `
    <div class="summary-card">
      <div class="label">Total transactions</div>
      <div class="value">${formatNum(meta.totals.total_tx)}</div>
    </div>
    <div class="summary-card">
      <div class="label">Total tx fees</div>
      <div class="value">${formatNum(meta.totals.total_fees, 6)}</div>
    </div>
    <div class="summary-card">
      <div class="label">Avg tx per day</div>
      <div class="value">${formatNum(meta.totals.avg_tx_per_day)}</div>
    </div>
    <div class="summary-card">
      <div class="label">Avg fees per day</div>
      <div class="value">${formatNum(meta.totals.avg_fees_per_day, 6)}</div>
    </div>
    <div class="summary-card">
      <div class="label">Max tx in a day (${meta.totals.max_tx_day.date || "-"})</div>
      <div class="value">${formatNum(meta.totals.max_tx_day.count)}</div>
    </div>
    <div class="summary-card">
      <div class="label">Max fees in a day (${meta.totals.max_fee_day.date || "-"})</div>
      <div class="value">${formatNum(meta.totals.max_fee_day.fee, 6)}</div>
    </div>
    <div class="summary-card">
      <div class="label">Median tx/day</div>
      <div class="value">${formatNum(meta.totals.median_tx_per_day || 0)}</div>
    </div>
    <div class="summary-card">
      <div class="label">Days in range</div>
      <div class="value">${formatNum(meta.totals.days || 0)}</div>
    </div>
  `;
}

function renderActivityTable(meta) {
  if (!activityTable) return;
  const categories = Object.keys(meta.per_category || {});
  const tableHeader = `
    <table class="summary-grid">
      <thead>
        <tr>
          <th>Category</th>
          <th>Total tx</th>
          <th>Total fees</th>
          <th>Avg tx/day</th>
          <th>Avg fees/day</th>
          <th>Max tx/day</th>
          <th>Max fees/day</th>
        </tr>
      </thead>
      <tbody>
        ${categories
          .map((cat) => {
            const vals = meta.per_category[cat];
            return `<tr>
              <td>${cat}</td>
              <td>${formatNum(vals.total_tx)}</td>
              <td>${formatNum(vals.total_fees, 6)}</td>
              <td>${formatNum(vals.avg_tx_per_day)}</td>
              <td>${formatNum(vals.avg_fees_per_day, 6)}</td>
              <td>${formatNum(vals.max_tx_day.count)} (${vals.max_tx_day.date || "-"})</td>
              <td>${formatNum(vals.max_fee_day.fee, 6)} (${vals.max_fee_day.date || "-"})</td>
            </tr>`;
          })
          .join("")}
      </tbody>
    </table>
  `;

  activityTable.innerHTML = tableHeader;
}

function buildActivityFilters(categories) {
  if (!activityFilters) return;
  activityFilters.innerHTML = "";
  categories.forEach((cat) => {
    const id = `cat-${cat}`;
    const label = document.createElement("label");
    label.setAttribute("for", id);
    label.innerHTML = `<input type="checkbox" id="${id}" data-cat="${cat}" checked> ${cat}`;
    activityFilters.appendChild(label);
  });
}

function getActiveCategories() {
  if (!activityFilters) return [];
  const boxes = activityFilters.querySelectorAll("input[type='checkbox']");
  const active = [];
  boxes.forEach((b) => {
    if (b.checked) active.push(b.dataset.cat);
  });
  return active;
}

async function refreshActivity(range) {
  activityRange = range;
  if (activityPicker) {
    activityPicker.querySelectorAll("button[data-range]").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.range === range);
    });
  }
  const data = await loadActivity(range);
  activityDataCache = data;
  buildActivityFilters(data.meta.categories || []);
  renderActivityBadges(data.meta);
  renderActivityCards(data.meta);
  renderActivityTable(data.meta);
  const activeCats = getActiveCategories();
  renderActivityChart(data, activeCats);
  renderMiners(range);
  renderNotaries(range);
}

if (activityPicker) {
  activityPicker.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-range]");
    if (!btn) return;
    refreshActivity(btn.dataset.range);
  });
}

if (activityFilters) {
  activityFilters.addEventListener("change", () => {
    if (activityDataCache) {
      const activeCats = getActiveCategories();
      renderActivityChart(activityDataCache, activeCats);
    }
  });
}

if (activityChartEl && activityPicker) {
  refreshActivity(activityRange).catch((err) => {
    console.error(err);
  });
}

// Miners / Notaries tables
async function loadJson(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Failed to load ${path}`);
  return res.json();
}

function daysFromRange(range) {
  if (!range || range === "all") return null;
  const num = Number(range);
  return Number.isFinite(num) ? num : null;
}

function filterByLastSeen(items, range) {
  const days = daysFromRange(range);
  if (days === null) return items;
  const cutoffMs = Date.now() - days * 86400000;
  return items.filter((item) => item.last_seen && item.last_seen * 1000 >= cutoffMs);
}

function daysAgo(ts) {
  if (!ts) return "-";
  const nowMs = Date.now();
  const diffDays = Math.floor((nowMs - ts * 1000) / 86400000);
  return `${diffDays}d ago`;
}

function shortenAddress(addr) {
  if (!addr || addr.length <= 14) return addr || "";
  return `${addr.slice(0, 7)}...${addr.slice(-7)}`;
}

function explorerLink(addr) {
  const url = `https://explorer.piratechain.com/address/${addr}`;
  return `<a href="${url}" target="_blank" rel="noopener noreferrer">${shortenAddress(addr)}</a>`;
}

async function renderMiners(range = activityRange) {
  if (!minersTable) return;
  try {
    if (!minersData) {
      minersData = await loadJson("data/miners.json");
    }
    const miners = filterByLastSeen(minersData, range);
    const rows = miners
      .map(
        (m) => `<tr>
          <td>${m.name}</td>
          <td class="address">${explorerLink(m.address)}</td>
          <td>${formatNum(m.blocks_mined)}</td>
          <td>${formatNum(m.total_arrr, 6)}</td>
          <td>${formatNum(m.avg_arrr_per_block || 0, 6)}</td>
          <td>${daysAgo(m.last_seen)}</td>
        </tr>`
      )
      .join("");
    minersTable.innerHTML = `
      <table class="striped-table">
        <thead>
          <tr>
            <th>Name</th><th>Address</th><th>Blocks mined</th><th>Total ARRR</th><th>Avg ARRR/block</th><th>Last seen</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    `;
  } catch (err) {
    console.error(err);
  }
}

async function renderNotaries(range = activityRange) {
  if (!notariesTable) return;
  try {
    if (!notariesData) {
      notariesData = await loadJson("data/notaries_stats.json");
    }
    const notaries = filterByLastSeen(notariesData, range);
    const rows = notaries
      .map(
        (n) => `<tr>
          <td>${n.name}</td>
          <td class="address">${explorerLink(n.address)}</td>
          <td>${formatNum(n.tx_count)}</td>
          <td>${formatNum(n.total_arrr, 6)}</td>
          <td>${formatNum(n.total_fee, 6)}</td>
          <td>${daysAgo(n.last_seen)}</td>
        </tr>`
      )
      .join("");
    notariesTable.innerHTML = `
      <table class="striped-table">
        <thead>
          <tr>
            <th>Name</th><th>Address</th><th>Tx count</th><th>Total ARRR</th><th>Total fees</th><th>Last seen</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    `;
  } catch (err) {
    console.error(err);
  }
}

renderMiners(activityRange);
renderNotaries(activityRange);
// Page toggle navigation
pageToggles.forEach((toggle) => {
  toggle.addEventListener("click", (e) => {
    const btn = e.target.closest(".toggle-option");
    if (!btn) return;
    const target = btn.dataset.target;
    if (target && !btn.classList.contains("active")) {
      window.location.href = target;
    }
  });
});
