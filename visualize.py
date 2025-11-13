#!/usr/bin/env python3
"""Serve interactive dashboards for every CSV in this folder."""

from __future__ import annotations

import argparse
import csv
import json
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).parent


def coerce(value: str | None) -> Any:
    if value is None:
        return ""
    trimmed = value.strip()
    if not trimmed:
        return ""
    numeric_candidate = trimmed.replace(",", "")
    try:
        if "." in numeric_candidate:
            return float(numeric_candidate)
        return int(numeric_candidate)
    except ValueError:
        return trimmed


def load_csv(path: Path) -> Dict[str, Any]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames[:] if reader.fieldnames else []
        rows: List[Dict[str, Any]] = []
        for row in reader:
            rows.append({key: coerce(value) for key, value in row.items()})
    return {
        "filename": path.name,
        "label": path.stem.replace("_", " "),
        "row_count": len(rows),
        "columns": columns,
        "rows": rows,
    }


def load_datasets() -> List[Dict[str, Any]]:
    return [load_csv(path) for path in sorted(ROOT.glob("*.csv"))]


def build_dashboard_html(datasets_json: str) -> str:
    template = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Player Performance Explorer</title>
  <style>
    :root {
      font-family: system-ui, -apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif;
      background: #0f172a;
      color: #e2e8f0;
    }
    body {
      margin: 0;
      padding: 1.5rem;
      line-height: 1.5;
    }
    h1, h2 { margin: 0 0 0.5rem; }
    select {
      padding: 0.4rem 0.6rem;
      border-radius: 0.4rem;
      border: 1px solid #334155;
      background: #1e293b;
      color: inherit;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
      gap: 1rem;
      margin-block: 1.5rem;
    }
    .card {
      background: rgba(15, 23, 42, 0.8);
      border: 1px solid #1f2937;
      border-radius: 0.8rem;
      padding: 1rem;
      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.45);
    }
    .card ul {
      list-style: none;
      padding: 0;
      margin: 0;
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
      font-size: 0.95rem;
    }
    canvas {
      background: rgba(15, 23, 42, 0.9);
      border-radius: 0.8rem;
      padding: 1rem;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 1rem;
      font-size: 0.93rem;
    }
    table td, table th {
      border-bottom: 1px solid #1f2937;
      padding: 0.4rem 0.6rem;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 0.8rem;
      margin: 1.5rem 0;
    }
    .stat {
      background: #151f32;
      border: 1px solid #1f2937;
      border-radius: 0.8rem;
      padding: 0.8rem 1rem;
      display: flex;
      flex-direction: column;
      gap: 0.2rem;
    }
    .stat .label {
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #94a3b8;
    }
    .stat .value {
      font-size: 1.5rem;
      font-weight: 600;
      color: #f8fafc;
    }
    .stat .sub {
      font-size: 0.85rem;
      color: #cbd5f5;
    }
    .tabs {
      display: flex;
      gap: 0.5rem;
      margin: 1.5rem 0;
      flex-wrap: wrap;
    }
    .tab-button {
      border: 1px solid #1d4ed8;
      background: rgba(37, 99, 235, 0.15);
      color: #e0f2fe;
      padding: 0.5rem 1rem;
      border-radius: 999px;
      cursor: pointer;
      font-weight: 600;
      transition: background 0.2s ease, color 0.2s ease;
    }
    .tab-button.active { background: #1d4ed8; color: #f8fafc; }
    .tab-content.hidden { display: none; }
    .filters {
      background: rgba(15, 23, 42, 0.8);
      border: 1px solid #1f2937;
      border-radius: 0.8rem;
      padding: 1rem;
      margin: 1.5rem 0;
      box-shadow: 0 15px 35px -20px rgba(0, 0, 0, 0.6);
    }
    .filters-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 0.75rem;
    }
    .filters label {
      font-size: 0.85rem;
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
      color: #cbd5f5;
    }
    .filters select,
    .filters input {
      padding: 0.4rem 0.6rem;
      border-radius: 0.4rem;
      border: 1px solid #334155;
      background: #0b1121;
      color: inherit;
    }
    .filters button {
      margin-top: 1rem;
      padding: 0.5rem 1rem;
      border: 1px solid #1d4ed8;
      background: #1d4ed8;
      color: #f8fafc;
      border-radius: 0.4rem;
      cursor: pointer;
      font-weight: 600;
    }
    .filters button:disabled { opacity: 0.5; cursor: not-allowed; }
    .compare-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 0.75rem;
    }
    .compare-summary {
      margin-top: 1rem;
      font-size: 0.95rem;
      color: #cbd5f5;
      line-height: 1.4;
    }
    .compare-search {
      display: flex;
      flex-direction: column;
      gap: 0.35rem;
    }
    .compare-search input {
      padding: 0.4rem 0.6rem;
      border-radius: 0.4rem;
      border: 1px solid #334155;
      background: #0b1121;
      color: inherit;
    }
    .storylines {
      list-style: disc;
      padding-left: 1.25rem;
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
      margin: 0;
      color: #cbd5f5;
      font-size: 0.95rem;
    }
    .chart-section-title {
      margin-top: 2rem;
      font-size: 1.3rem;
    }
    .full-width {
      grid-column: 1 / -1;
    }
    .insights {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 0.8rem;
    }
    .insight {
      background: #1e293b;
      border-radius: 0.8rem;
      padding: 0.8rem 1rem;
      border: 1px solid #334155;
      font-size: 0.9rem;
    }
    a { color: #38bdf8; }
  </style>
  <script src=\"https://cdn.jsdelivr.net/npm/chart.js@4.4.6/dist/chart.umd.min.js\"></script>
</head>
<body>
  <header>
    <h1>Player Performance Explorer</h1>
    <p>Pick a dataset, filter the squad, and reveal patterns, comparisons, and storylines.</p>
    <label>Dataset: <select id=\"datasetSelect\"></select></label>
  </header>

  <nav class=\"tabs\">
    <button class=\"tab-button active\" data-tab=\"overview\">Overview</button>
    <button class=\"tab-button\" data-tab=\"compare\">Compare Players</button>
  </nav>

  <section id=\"overviewTab\" class=\"tab-content\">
    <section class=\"filters card\">
      <h2>Filters</h2>
      <div class=\"filters-grid\">
        <label>Position
          <select id=\"filterPosition\"><option value=\"all\">All positions</option></select>
        </label>
        <label>League
          <select id=\"filterLeague\"><option value=\"all\">All leagues</option></select>
        </label>
        <label>Team
          <select id=\"filterTeam\"><option value=\"all\">All teams</option></select>
        </label>
        <label>Minutes ≥
          <input type=\"number\" id=\"filterMinMinutes\" min=\"0\" step=\"30\" value=\"0\" />
        </label>
        <label>Age ≤
          <input type=\"number\" id=\"filterMaxAge\" min=\"0\" max=\"60\" step=\"1\" placeholder=\"Any\" />
        </label>
      </div>
      <button id=\"resetFilters\" type=\"button\">Reset Filters</button>
    </section>

    <section class=\"stats\" id=\"summaryStats\"></section>

    <section class=\"card\">
      <h2>Highlight Players</h2>
      <p>Type a name to highlight two players across the charts.</p>
      <div class=\"compare-grid\">
        <label>Highlight A
          <div class=\"compare-search\">
            <input type=\"text\" id=\"highlightPlayerA\" placeholder=\"Type to search…\" list=\"highlightListA\" autocomplete=\"off\" />
            <datalist id=\"highlightListA\"></datalist>
          </div>
        </label>
        <label>Highlight B
          <div class=\"compare-search\">
            <input type=\"text\" id=\"highlightPlayerB\" placeholder=\"Type to search…\" list=\"highlightListB\" autocomplete=\"off\" />
            <datalist id=\"highlightListB\"></datalist>
          </div>
        </label>
      </div>
    </section>

    <section class=\"grid\">
      <article class=\"card\"><h2>Finishers • Goals</h2><ul id=\"topScorers\"></ul></article>
      <article class=\"card\"><h2>Playmakers • Assists</h2><ul id=\"topCreators\"></ul></article>
      <article class=\"card\"><h2>Field Tilt • Progression</h2><ul id=\"topProgressors\"></ul></article>
    </section>

    <h2 class=\"chart-section-title\">Output & Efficiency</h2>
    <section class=\"grid\">
      <article class=\"card\"><h2>Goals vs Assists</h2><canvas id=\"goalsAssistsChart\" height=\"280\"></canvas></article>
      <article class=\"card\"><h2>Goals vs xG</h2><canvas id=\"goalsXgChart\" height=\"280\"></canvas></article>
      <article class=\"card\"><h2>xG per 90 Distribution</h2><canvas id=\"xgHistogram\" height=\"280\"></canvas></article>
      <article class=\"card\"><h2>Assists vs xA</h2><canvas id=\"assistsXaChart\" height=\"280\"></canvas></article>
      <article class=\"card\"><h2>G+A per 90 Leaders</h2><canvas id=\"gaRankingChart\" height=\"280\"></canvas></article>
      <article class=\"card\"><h2>Goals per 90 by Age</h2><canvas id=\"goalsAgeCurve\" height=\"280\"></canvas></article>
      <article class=\"card\"><h2>Usage vs Output</h2><canvas id=\"efficiencyChart\" height=\"280\"></canvas></article>
      <article class=\"card\"><h2>xG Chain Map</h2><canvas id=\"xgChainChart\" height=\"280\"></canvas></article>
      <article class=\"card\"><h2>Minutes vs G+A per 90</h2><canvas id=\"minutesGaChart\" height=\"280\"></canvas></article>
    </section>

    <h2 class=\"chart-section-title\">Ball Progression</h2>
    <section class=\"grid\">
      <article class=\"card\"><h2>Progressive Carries vs Passes</h2><canvas id=\"progressionChart\" height=\"280\"></canvas></article>
      <article class=\"card\"><h2>Threat Per 90</h2><canvas id=\"per90Chart\" height=\"280\"></canvas></article>
      <article class=\"card\"><h2>Positional Load</h2><canvas id=\"positionMinutesChart\" height=\"280\"></canvas></article>
      <article class=\"card\"><h2>Finishing Delta</h2><canvas id=\"finishingChart\" height=\"280\"></canvas></article>
      <article class=\"card\"><h2>Team Goal Share</h2><canvas id=\"teamContributionChart\" height=\"280\"></canvas></article>
    </section>

    <section>
      <h2>Storylines & Over/Under-performance</h2>
      <div class=\"insights\" id=\"insights\"></div>
      <table id=\"overUnderTable\">
        <thead>
          <tr><th colspan=\"2\">Clinical Finishers</th><th colspan=\"2\">Unlucky Creators</th></tr>
          <tr><th>Player</th><th>Goals - xG</th><th>Player</th><th>Goals - xG</th></tr>
        </thead>
        <tbody></tbody>
      </table>
      <article class=\"card\">
        <h2>Emerging Storylines</h2>
        <ul id=\"storylines\" class=\"storylines\"></ul>
      </article>
    </section>
  </section>

  <section id=\"compareTab\" class=\"tab-content hidden\">
    <section class=\"card\">
      <h2>Player Comparison Lab</h2>
      <p>Type to search two players and unlock head-to-head insights.</p>
      <div class=\"compare-grid\">
        <label>Player A
          <div class=\"compare-search\">
            <input type=\"text\" id=\"searchPlayerA\" placeholder=\"Type to search…\" list=\"playersListA\" autocomplete=\"off\" />
            <datalist id=\"playersListA\"></datalist>
          </div>
        </label>
        <label>Player B
          <div class=\"compare-search\">
            <input type=\"text\" id=\"searchPlayerB\" placeholder=\"Type to search…\" list=\"playersListB\" autocomplete=\"off\" />
            <datalist id=\"playersListB\"></datalist>
          </div>
        </label>
      </div>
      <p id=\"compareNote\" class=\"compare-summary\">Select two players to see the comparison details.</p>
      <div id=\"compareSummary\" class=\"compare-summary\"></div>
    </section>

    <section class=\"grid\">
      <article class=\"card\"><h3>League Percentile Radar</h3><canvas id=\"comparePercentileRadar\" height=\"320\"></canvas></article>
      <article class=\"card\"><h3>Position Overlay Radar</h3><canvas id=\"comparePositionRadar\" height=\"320\"></canvas></article>
      <article class=\"card\"><h3>Goals vs xG (League)</h3><canvas id=\"compareGoalsXgLeague\" height=\"320\"></canvas></article>
      <article class=\"card\"><h3>Assists vs xA (League)</h3><canvas id=\"compareAssistsXaLeague\" height=\"320\"></canvas></article>
      <article class=\"card\"><h3>Minutes vs G+A per 90</h3><canvas id=\"compareGaMinutes\" height=\"320\"></canvas></article>
    </section>
  </section>

  <script>
    const DATASETS = __DATASETS_JSON__;
    const datasetSelect = document.getElementById("datasetSelect");
    const topScorersEl = document.getElementById("topScorers");
    const topCreatorsEl = document.getElementById("topCreators");
    const topProgressorsEl = document.getElementById("topProgressors");
    const insightsEl = document.getElementById("insights");
    const overUnderBody = document.querySelector("#overUnderTable tbody");
    const summaryStatsEl = document.getElementById("summaryStats");
    const storylinesEl = document.getElementById("storylines");
    const filterControls = {
      position: document.getElementById("filterPosition"),
      league: document.getElementById("filterLeague"),
      team: document.getElementById("filterTeam"),
      minMinutes: document.getElementById("filterMinMinutes"),
      maxAge: document.getElementById("filterMaxAge"),
      reset: document.getElementById("resetFilters"),
    };
    const tabButtons = document.querySelectorAll(".tab-button");
    const tabs = {
      overview: document.getElementById("overviewTab"),
      compare: document.getElementById("compareTab"),
    };
    const compareControls = {
      playerAInput: document.getElementById("searchPlayerA"),
      playerBInput: document.getElementById("searchPlayerB"),
      listA: document.getElementById("playersListA"),
      listB: document.getElementById("playersListB"),
      note: document.getElementById("compareNote"),
      summary: document.getElementById("compareSummary"),
    };
    const highlightControls = {
      playerAInput: document.getElementById("highlightPlayerA"),
      playerBInput: document.getElementById("highlightPlayerB"),
      listA: document.getElementById("highlightListA"),
      listB: document.getElementById("highlightListB"),
    };

    const filterState = { position: "all", league: "all", team: "all", minMinutes: 0, maxAge: "" };
    const compareState = { playerA: null, playerB: null };
    const highlightState = { playerA: null, playerB: null };
    const TOP_N = 10;
    const formatNumber = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });
    const charts = {
      goalsAssists: null,
      goalsXg: null,
      xgHistogram: null,
      assistsXa: null,
      gaRanking: null,
      goalsAgeCurve: null,
      efficiency: null,
      xgChain: null,
      minutesGa: null,
      progression: null,
      per90: null,
      positionMinutes: null,
      finishing: null,
      teamContribution: null,
      comparePercentileRadar: null,
      comparePositionRadar: null,
      compareGoalsXgLeague: null,
      compareAssistsXaLeague: null,
      compareGaMinutes: null,
    };

    DATASETS.forEach((dataset, index) => {
      const option = document.createElement("option");
      option.value = index;
      option.textContent = `${dataset.label} (${dataset.row_count} rows)`;
      datasetSelect.appendChild(option);
    });

    function numeric(value) {
      if (value === null || value === undefined || value === "") return 0;
      return Number(value) || 0;
    }

    function primaryPosition(value) {
      if (!value) return "Unknown";
      const first = value.split(",")[0].trim();
      return first || "Unknown";
    }

    function renderList(element, rows, field) {
      element.innerHTML = rows
        .map((row, idx) => `<li><strong>${idx + 1}.</strong> ${row.player || row.team} — ${formatNumber.format(numeric(row[field]))} ${field}</li>`)
        .join("");
    }

    function aggregateBy(rows, keyField, valueField) {
      const totals = new Map();
      rows.forEach((row) => {
        const key = row[keyField] || "Unknown";
        totals.set(key, (totals.get(key) || 0) + numeric(row[valueField]));
      });
      return [...totals.entries()].sort((a, b) => b[1] - a[1]);
    }

    function upsertChart(key, ctx, config) {
      if (!charts[key]) { charts[key] = new Chart(ctx, config); }
      else { charts[key].data = config.data; charts[key].options = config.options; charts[key].update(); }
    }

    function safePer90(total, minutes) {
      const mins = Math.max(1, numeric(minutes));
      return total / (mins / 90);
    }

    function computeAge(row, currentYear) {
      const birth = numeric(row.birth_year);
      if (!birth) return null;
      const age = currentYear - birth;
      return Number.isFinite(age) ? age : null;
    }

    function getLeagueField(dataset) {
      if (!dataset || !dataset.columns) return null;
      return ["comp_level", "comp", "league"].find((field) => dataset.columns.includes(field)) || null;
    }

    function extractUnique(rows, field) {
      const set = new Set();
      rows.forEach((row) => {
        const value = row[field];
        if (value !== undefined && value !== null && value !== "") set.add(String(value));
      });
      return [...set].sort((a, b) => a.localeCompare(b));
    }

    function extractPositions(rows) {
      const set = new Set();
      rows.forEach((row) => {
        const value = row.position;
        if (!value) return;
        value.split(",").forEach((token) => { const trimmed = token.trim(); if (trimmed) set.add(trimmed); });
      });
      return [...set].sort((a, b) => a.localeCompare(b));
    }

    function populateSelect(select, values, placeholder, enabled) {
      if (!select) return;
      select.innerHTML = `<option value="all">${placeholder}</option>`;
      if (enabled) {
        values.forEach((value) => {
          const option = document.createElement("option");
          option.value = value;
          option.textContent = value;
          select.appendChild(option);
        });
      }
      select.disabled = !enabled;
    }

    function syncFilterControls() {
      if (filterControls.position) filterControls.position.value = filterState.position;
      if (filterControls.league) filterControls.league.value = filterState.league;
      if (filterControls.team) filterControls.team.value = filterState.team;
      if (filterControls.minMinutes) filterControls.minMinutes.value = filterState.minMinutes || 0;
      if (filterControls.maxAge) filterControls.maxAge.value = filterState.maxAge === "" ? "" : filterState.maxAge;
    }

    function resetFilterState() {
      filterState.position = "all";
      filterState.league = "all";
      filterState.team = "all";
      filterState.minMinutes = 0;
      filterState.maxAge = "";
    }

    function populateFilterControls(dataset) {
      if (!dataset) return;
      const rows = dataset.rows || [];
      const positionValues = extractPositions(rows);
      populateSelect(filterControls.position, positionValues, positionValues.length ? "All positions" : "No positions", positionValues.length > 0);

      const leagueField = getLeagueField(dataset);
      const leagueValues = leagueField ? extractUnique(rows, leagueField) : [];
      populateSelect(filterControls.league, leagueValues, leagueField ? "All leagues" : "League unavailable", Boolean(leagueField && leagueValues.length));

      const hasTeam = dataset.columns && dataset.columns.includes("team");
      const teamValues = hasTeam ? extractUnique(rows, "team") : [];
      populateSelect(filterControls.team, teamValues, hasTeam ? "All teams" : "Team unavailable", Boolean(hasTeam && teamValues.length));

      const ageAvailable = dataset.columns && (dataset.columns.includes("birth_year") || dataset.columns.includes("age"));
      if (filterControls.maxAge) {
        filterControls.maxAge.disabled = !ageAvailable;
        filterControls.maxAge.placeholder = ageAvailable ? "Any" : "No age data";
        if (!ageAvailable) filterState.maxAge = "";
      }
      syncFilterControls();
    }

    function applyFilters(rows, dataset, currentYear) {
      const leagueField = getLeagueField(dataset);
      const hasTeam = dataset.columns && dataset.columns.includes("team");
      const hasBirthYear = dataset.columns && dataset.columns.includes("birth_year");
      const hasAgeField = dataset.columns && dataset.columns.includes("age");
      return rows.filter((row) => {
        if (filterState.position !== "all") {
          const tokens = String(row.position || "").split(",").map((token) => token.trim().toLowerCase()).filter(Boolean);
          if (!tokens.includes(filterState.position.toLowerCase())) return false;
        }
        if (filterState.league !== "all" && leagueField && String(row[leagueField] || "") !== filterState.league) return false;
        if (filterState.team !== "all" && hasTeam && String(row.team || "") !== filterState.team) return false;
        if (filterState.minMinutes && numeric(row.minutes) < filterState.minMinutes) return false;
        if (filterState.maxAge) {
          let ageValue = null;
          if (hasBirthYear) ageValue = computeAge(row, currentYear);
          else if (hasAgeField) { const parsedAge = numeric(row.age); ageValue = Number.isFinite(parsedAge) && parsedAge > 0 ? parsedAge : null; }
          if (ageValue !== null && ageValue > filterState.maxAge) return false;
        }
        return true;
      });
    }

    let latestFilteredRows = [];
    let leagueRows = [];
    let latestRowLookup = new Map();
    let latestCompareOptions = [];
    let compareLabelToKey = new Map();
    let compareKeyToLabel = new Map();

    function updateTabs(target) {
      tabButtons.forEach((button) => button.classList.toggle("active", button.dataset.tab === target));
      Object.entries(tabs).forEach(([key, section]) => {
        if (section) section.classList.toggle("hidden", key !== target);
      });
    }
    tabButtons.forEach((button) => button.addEventListener("click", () => updateTabs(button.dataset.tab)));

    function playerLabel(row) {
      return `${row.player || row.team || "Unknown"} (${row.team || "—"})`;
    }

    function goalsPer90(row) {
      return numeric(row.goals_per90) || safePer90(numeric(row.goals), numeric(row.minutes));
    }

    function assistsPer90(row) {
      return numeric(row.assists_per90) || safePer90(numeric(row.assists), numeric(row.minutes));
    }

    function xgPer90(row) {
      return numeric(row.xg_per90) || safePer90(numeric(row.xg), numeric(row.minutes));
    }

    function xgAssistPer90(row) {
      return numeric(row.xg_assist_per90) || safePer90(numeric(row.xg_assist), numeric(row.minutes));
    }

    function gaPer90(row) {
      return goalsPer90(row) + assistsPer90(row);
    }

    function minutesPlayed(row) {
      return Math.max(0, numeric(row.minutes));
    }

    function highlightEntries(rows) {
      const entries = [];
      [
        { key: highlightState.playerA, color: "#2563eb" },
        { key: highlightState.playerB, color: "#dc2626" },
      ].forEach(({ key, color }) => {
        if (!key) return;
        const row = latestRowLookup.get(key);
        if (row && rows.includes(row)) {
          entries.push({ row, color, label: playerLabel(row) });
        }
      });
      return entries;
    }

    function highlightDatasetsFor(rows, mapper) {
      return highlightEntries(rows)
        .map(({ row, color, label }) => {
          const point = mapper(row);
          if (!point || !Number.isFinite(point.x) || !Number.isFinite(point.y)) return null;
          return {
            label,
            data: [{
              ...point,
              label,
              team: row.team || "—",
            }],
            backgroundColor: color,
            borderColor: color,
            pointRadius: 8,
            pointHoverRadius: 10,
          };
        })
        .filter(Boolean);
    }

    function buildCompareOptions(rows) {
      latestRowLookup = new Map();
      latestCompareOptions = [];
      compareLabelToKey = new Map();
      compareKeyToLabel = new Map();
      rows.forEach((row, idx) => {
        if (!row.player && !row.team) return;
        const key = `${idx}-${row.player || row.team || "row"}`;
        const label = playerLabel(row);
        latestRowLookup.set(key, row);
        compareLabelToKey.set(label.toLowerCase(), key);
        compareKeyToLabel.set(key, label);
        latestCompareOptions.push({ key, label });
      });
    }

    function populateCompareOptions(rows) {
      if (!compareControls.playerAInput || !compareControls.playerBInput || !compareControls.listA || !compareControls.listB) return;
      if (!rows.length) {
        compareControls.playerAInput.value = "";
        compareControls.playerBInput.value = "";
        compareControls.playerAInput.disabled = true;
        compareControls.playerBInput.disabled = true;
        compareControls.listA.innerHTML = "";
        compareControls.listB.innerHTML = "";
        compareState.playerA = null;
        compareState.playerB = null;
        latestRowLookup = new Map();
        latestCompareOptions = [];
        renderCompareCharts();
        return;
      }

      const sorted = [...rows].sort((a, b) => minutesPlayed(b) - minutesPlayed(a));
      buildCompareOptions(sorted);
      const optionsMarkup = latestCompareOptions.map((option) => `<option value="${option.label}"></option>`).join("");
      compareControls.listA.innerHTML = optionsMarkup;
      compareControls.listB.innerHTML = optionsMarkup;
      compareControls.playerAInput.disabled = false;
      compareControls.playerBInput.disabled = false;

      if (!compareState.playerA || !latestRowLookup.has(compareState.playerA)) compareState.playerA = null;
      if (!compareState.playerB || !latestRowLookup.has(compareState.playerB)) compareState.playerB = null;
      compareControls.playerAInput.value = compareState.playerA ? compareKeyToLabel.get(compareState.playerA) || "" : "";
      compareControls.playerBInput.value = compareState.playerB ? compareKeyToLabel.get(compareState.playerB) || "" : "";
      renderCompareCharts();
    }

    function populateHighlightOptions(filteredRows) {
      if (!highlightControls.playerAInput || !highlightControls.playerBInput || !highlightControls.listA || !highlightControls.listB) return;
      const options = filteredRows
        .filter((row) => row.player || row.team)
        .map((row) => {
          const label = playerLabel(row);
          const key = compareLabelToKey.get(label.toLowerCase());
          if (!key) return null;
          return { key, label };
        })
        .filter(Boolean);
      const markup = options.map((option) => `<option value="${option.label}"></option>`).join("");
      highlightControls.listA.innerHTML = markup;
      highlightControls.listB.innerHTML = markup;

      const hasOptions = options.length > 0;
      highlightControls.playerAInput.disabled = !hasOptions;
      highlightControls.playerBInput.disabled = !hasOptions;

      if (!hasOptions) {
        highlightState.playerA = null;
        highlightState.playerB = null;
        highlightControls.playerAInput.value = "";
        highlightControls.playerBInput.value = "";
        return;
      }

      ["playerA", "playerB"].forEach((slot) => {
        const key = highlightState[slot];
        const input = slot === "playerA" ? highlightControls.playerAInput : highlightControls.playerBInput;
        if (!key || !latestRowLookup.has(key) || !options.find((option) => option.key === key)) {
          highlightState[slot] = null;
          input.value = "";
        } else {
          input.value = compareKeyToLabel.get(key) || "";
        }
      });
    }

    function handleCompareInput(stateKey) {
      const input = stateKey === "playerA" ? compareControls.playerAInput : compareControls.playerBInput;
      if (!input) return;
      const key = compareLabelToKey.get(input.value.trim().toLowerCase()) || null;
      compareState[stateKey] = key;
      renderCompareCharts();
    }

    if (compareControls.playerAInput) {
      compareControls.playerAInput.addEventListener("input", () => handleCompareInput("playerA"));
      compareControls.playerAInput.addEventListener("change", () => handleCompareInput("playerA"));
    }
    if (compareControls.playerBInput) {
      compareControls.playerBInput.addEventListener("input", () => handleCompareInput("playerB"));
      compareControls.playerBInput.addEventListener("change", () => handleCompareInput("playerB"));
    }

    function handleHighlightInput(stateKey) {
      const input = stateKey === "playerA" ? highlightControls.playerAInput : highlightControls.playerBInput;
      if (!input) return;
      const key = compareLabelToKey.get(input.value.trim().toLowerCase()) || null;
      highlightState[stateKey] = key;
      updateDashboard();
    }

    if (highlightControls.playerAInput) {
      highlightControls.playerAInput.addEventListener("input", () => handleHighlightInput("playerA"));
      highlightControls.playerAInput.addEventListener("change", () => handleHighlightInput("playerA"));
    }
    if (highlightControls.playerBInput) {
      highlightControls.playerBInput.addEventListener("input", () => handleHighlightInput("playerB"));
      highlightControls.playerBInput.addEventListener("change", () => handleHighlightInput("playerB"));
    }

    function renderCompareCharts() {
      const playerA = compareState.playerA ? latestRowLookup.get(compareState.playerA) : null;
      const playerB = compareState.playerB ? latestRowLookup.get(compareState.playerB) : null;
      if (!playerA || !playerB) {
        if (compareControls.note) compareControls.note.style.display = "block";
        if (compareControls.summary) compareControls.summary.innerHTML = "";
        ["comparePercentileRadar","comparePositionRadar","compareGoalsXgLeague","compareAssistsXaLeague","compareGaMinutes"].forEach((key) => {
          if (charts[key]) {
            charts[key].data = { labels: [], datasets: [] };
            charts[key].update();
          }
        });
        return;
      }

      if (compareControls.note) compareControls.note.style.display = "none";

      const metrics = leagueRows.map((row) => ({
        goals90: goalsPer90(row),
        assists90: assistsPer90(row),
        xg90: xgPer90(row),
        xga90: xgAssistPer90(row),
        progression: numeric(row.progressive_carries) + numeric(row.progressive_passes),
        minutes: minutesPlayed(row),
        row,
      }));

      function percentileFn(key) {
        const sorted = metrics.map((m) => m[key]).filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
        return (value) => {
          if (!sorted.length) return 0;
          let low = 0;
          let high = sorted.length;
          while (low < high) {
            const mid = Math.floor((low + high) / 2);
            if (value >= sorted[mid]) low = mid + 1;
            else high = mid;
          }
          return (low / sorted.length) * 100;
        };
      }

      const percentileFns = {
        goals90: percentileFn("goals90"),
        assists90: percentileFn("assists90"),
        xg90: percentileFn("xg90"),
        xga90: percentileFn("xga90"),
        progression: percentileFn("progression"),
        minutes: percentileFn("minutes"),
      };

      function percentileData(row) {
        return [
          percentileFns.goals90(goalsPer90(row)),
          percentileFns.assists90(assistsPer90(row)),
          percentileFns.xg90(xgPer90(row)),
          percentileFns.xga90(xgAssistPer90(row)),
          percentileFns.progression(numeric(row.progressive_carries) + numeric(row.progressive_passes)),
          percentileFns.minutes(minutesPlayed(row)),
        ];
      }

      const percentileLabels = ["Goals/90","Assists/90","xG/90","xGA/90","Progression","Minutes"];
      const percentileCtx = document.getElementById("comparePercentileRadar");
      if (percentileCtx) {
        upsertChart("comparePercentileRadar", percentileCtx, {
          type: "radar",
          data: {
            labels: percentileLabels,
            datasets: [
              { label: playerA.player || "Player A", data: percentileData(playerA), backgroundColor: "rgba(59, 130, 246, 0.2)", borderColor: "#3b82f6" },
              { label: playerB.player || "Player B", data: percentileData(playerB), backgroundColor: "rgba(248, 113, 113, 0.2)", borderColor: "#f87171" },
            ],
          },
          options: { scales: { r: { suggestedMin: 0, suggestedMax: 100 } } },
        });
      }

      function positionKey(row) {
        return primaryPosition(row.position).toUpperCase();
      }

      const positionGroups = new Map();
      leagueRows.forEach((row) => {
        const key = positionKey(row);
        if (!positionGroups.has(key)) {
          positionGroups.set(key, { count: 0, goals: 0, assists: 0, xg: 0, xga: 0, progression: 0, cards: 0 });
        }
        const group = positionGroups.get(key);
        group.count += 1;
        group.goals += goalsPer90(row);
        group.assists += assistsPer90(row);
        group.xg += xgPer90(row);
        group.xga += xgAssistPer90(row);
        group.progression += numeric(row.progressive_carries) + numeric(row.progressive_passes);
        group.cards += numeric(row.cards_yellow) + numeric(row.cards_red);
      });

      function positionAverage(row) {
        const group = positionGroups.get(positionKey(row));
        if (!group || !group.count) return [0, 0, 0, 0, 0, 0];
        return [
          group.goals / group.count,
          group.assists / group.count,
          group.xg / group.count,
          group.xga / group.count,
          group.progression / group.count,
          group.cards / group.count,
        ];
      }

      const positionLabels = ["Goals/90","Assists/90","xG/90","xGA/90","Progression","Cards"];
      const positionCtx = document.getElementById("comparePositionRadar");
      if (positionCtx) {
        upsertChart("comparePositionRadar", positionCtx, {
          type: "radar",
          data: {
            labels: positionLabels,
            datasets: [
              { label: playerA.player || "Player A", data: [
                goalsPer90(playerA),
                assistsPer90(playerA),
                xgPer90(playerA),
                xgAssistPer90(playerA),
                numeric(playerA.progressive_carries) + numeric(playerA.progressive_passes),
                numeric(playerA.cards_yellow) + numeric(playerA.cards_red),
              ], backgroundColor: "rgba(59, 130, 246, 0.15)", borderColor: "#3b82f6" },
              { label: playerB.player || "Player B", data: [
                goalsPer90(playerB),
                assistsPer90(playerB),
                xgPer90(playerB),
                xgAssistPer90(playerB),
                numeric(playerB.progressive_carries) + numeric(playerB.progressive_passes),
                numeric(playerB.cards_yellow) + numeric(playerB.cards_red),
              ], backgroundColor: "rgba(248, 113, 113, 0.15)", borderColor: "#f87171" },
              { label: `${positionKey(playerA)} Avg`, data: positionAverage(playerA), backgroundColor: "rgba(148, 163, 184, 0.1)", borderColor: "#94a3b8" },
            ],
          },
        });
      }

      function leagueScatter(xAccessor, yAccessor) {
        return leagueRows
          .map((row) => ({
            x: xAccessor(row),
            y: yAccessor(row),
            label: playerLabel(row),
            team: row.team || "—",
          }))
          .filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y));
      }

      const goalsXgLeagueCtx = document.getElementById("compareGoalsXgLeague");
      if (goalsXgLeagueCtx) {
        const leagueData = leagueScatter(xgPer90, goalsPer90);
        upsertChart("compareGoalsXgLeague", goalsXgLeagueCtx, {
          type: "scatter",
          data: {
            datasets: [
              { label: "League", data: leagueData, backgroundColor: "#47556955", borderColor: "#475569", pointRadius: 3 },
              {
                label: playerA.player || "Player A",
                data: [{ x: xgPer90(playerA), y: goalsPer90(playerA), label: playerLabel(playerA), team: playerA.team || "—" }],
                backgroundColor: "#3b82f6", borderColor: "#3b82f6", pointRadius: 7,
              },
              {
                label: playerB.player || "Player B",
                data: [{ x: xgPer90(playerB), y: goalsPer90(playerB), label: playerLabel(playerB), team: playerB.team || "—" }],
                backgroundColor: "#f87171", borderColor: "#f87171", pointRadius: 7,
              },
            ],
          },
          options: {
            plugins: { tooltip: { callbacks: { label: (ctx) => {
              const d = ctx.raw;
              return `${d.label} — Goals/90: ${formatNumber.format(d.y)}, xG/90: ${formatNumber.format(d.x)}`;
            } } } },
            scales: { x: { title: { display: true, text: "xG per 90" } }, y: { title: { display: true, text: "Goals per 90" } } },
          },
        });
      }

      const assistsXaLeagueCtx = document.getElementById("compareAssistsXaLeague");
      if (assistsXaLeagueCtx) {
        const leagueData = leagueScatter(xgAssistPer90, assistsPer90);
        upsertChart("compareAssistsXaLeague", assistsXaLeagueCtx, {
          type: "scatter",
          data: {
            datasets: [
              { label: "League", data: leagueData, backgroundColor: "#47556955", borderColor: "#475569", pointRadius: 3 },
              {
                label: playerA.player || "Player A",
                data: [{ x: xgAssistPer90(playerA), y: assistsPer90(playerA), label: playerLabel(playerA), team: playerA.team || "—" }],
                backgroundColor: "#3b82f6", borderColor: "#3b82f6", pointRadius: 7,
              },
              {
                label: playerB.player || "Player B",
                data: [{ x: xgAssistPer90(playerB), y: assistsPer90(playerB), label: playerLabel(playerB), team: playerB.team || "—" }],
                backgroundColor: "#f87171", borderColor: "#f87171", pointRadius: 7,
              },
            ],
          },
          options: {
            plugins: { tooltip: { callbacks: { label: (ctx) => {
              const d = ctx.raw;
              return `${d.label} — Assists/90: ${formatNumber.format(d.y)}, xA/90: ${formatNumber.format(d.x)}`;
            } } } },
            scales: { x: { title: { display: true, text: "xA per 90" } }, y: { title: { display: true, text: "Assists per 90" } } },
          },
        });
      }

      const gaMinutesCtx = document.getElementById("compareGaMinutes");
      if (gaMinutesCtx) {
        const leagueData = leagueScatter(minutesPlayed, gaPer90);
        upsertChart("compareGaMinutes", gaMinutesCtx, {
          type: "scatter",
          data: {
            datasets: [
              { label: "League", data: leagueData, backgroundColor: "#47556955", borderColor: "#475569", pointRadius: 3 },
              {
                label: playerA.player || "Player A",
                data: [{ x: minutesPlayed(playerA), y: gaPer90(playerA), label: playerLabel(playerA), team: playerA.team || "—" }],
                backgroundColor: "#3b82f6", borderColor: "#3b82f6", pointRadius: 7,
              },
              {
                label: playerB.player || "Player B",
                data: [{ x: minutesPlayed(playerB), y: gaPer90(playerB), label: playerLabel(playerB), team: playerB.team || "—" }],
                backgroundColor: "#f87171", borderColor: "#f87171", pointRadius: 7,
              },
            ],
          },
          options: {
            plugins: { tooltip: { callbacks: { label: (ctx) => {
              const d = ctx.raw;
              return `${d.label} — Minutes: ${formatNumber.format(d.x)}, G+A/90: ${formatNumber.format(d.y)}`;
            } } } },
            scales: { x: { title: { display: true, text: "Minutes" } }, y: { title: { display: true, text: "G+A per 90" } } },
          },
        });
      }

      if (compareControls.summary) {
        const diffMinutes = minutesPlayed(playerA) - minutesPlayed(playerB);
        const diffGoals = goalsPer90(playerA) - goalsPer90(playerB);
        const progDiff = (numeric(playerA.progressive_carries) + numeric(playerA.progressive_passes)) -
          (numeric(playerB.progressive_carries) + numeric(playerB.progressive_passes));
        compareControls.summary.innerHTML = `
          <strong>${playerA.player || "Player A"}</strong> logs ${formatNumber.format(diffMinutes)} more minutes and
          ${formatNumber.format(diffGoals)} more goals/90, while contributing ${formatNumber.format(progDiff)} more progressive actions than
          <strong>${playerB.player || "Player B"}</strong>.
        `;
      }
    }

    function updateDashboard() {
      const dataset = DATASETS[datasetSelect.value];
      if (!dataset) return;
      const currentYear = new Date().getFullYear();
      leagueRows = dataset.rows || [];
      populateCompareOptions(leagueRows);
      const rows = applyFilters(leagueRows, dataset, currentYear);
      latestFilteredRows = rows;
      populateHighlightOptions(rows);

      const withGoals = rows.filter((row) => numeric(row.goals) > 0);
      const withAssists = rows.filter((row) => numeric(row.assists) > 0);
      const withProgression = rows.filter((row) => numeric(row.progressive_carries) + numeric(row.progressive_passes) > 0);

      const topScorers = [...rows].sort((a, b) => numeric(b.goals) - numeric(a.goals)).slice(0, TOP_N);
      const topCreators = [...rows].sort((a, b) => numeric(b.assists) - numeric(a.assists)).slice(0, TOP_N);
      const topProgressors = [...rows]
        .sort((a, b) => (numeric(b.progressive_carries) + numeric(b.progressive_passes)) - (numeric(a.progressive_carries) + numeric(a.progressive_passes)))
        .slice(0, TOP_N)
        .map((row) => ({
          player: row.player,
          progress: numeric(row.progressive_carries) + numeric(row.progressive_passes),
        }));

      renderList(topScorersEl, topScorers, "goals");
      renderList(topCreatorsEl, topCreators, "assists");
      topProgressorsEl.innerHTML = topProgressors
        .map((row, idx) => `<li><strong>${idx + 1}.</strong> ${row.player} — ${formatNumber.format(row.progress)} prog. actions</li>`)
        .join("");

      const totalGoals = rows.reduce((sum, row) => sum + numeric(row.goals), 0);
      const totalXg = rows.reduce((sum, row) => sum + numeric(row.xg), 0);
      const totalAssists = rows.reduce((sum, row) => sum + numeric(row.assists), 0);
      const totalXa = rows.reduce((sum, row) => sum + numeric(row.xg_assist), 0);
      const totalMinutes = rows.reduce((sum, row) => sum + numeric(row.minutes), 0);
      const matchesEquivalent = totalMinutes ? totalMinutes / 90 : 0;
      const ageValues = rows.map((row) => computeAge(row, currentYear)).filter((value) => value !== null);
      const avgAge = ageValues.length ? ageValues.reduce((sum, value) => sum + value, 0) / ageValues.length : 0;
      const avgGoalsPer90 = rows.length
        ? rows.reduce((sum, row) => sum + (numeric(row.goals_per90) || safePer90(numeric(row.goals), numeric(row.minutes))), 0) / rows.length
        : 0;

      if (summaryStatsEl) {
        if (!rows.length) {
          summaryStatsEl.innerHTML = `
            <div class="stat">
              <span class="label">No rows match</span>
              <span class="value">0</span>
              <span class="sub">Adjust filters above.</span>
            </div>
          `;
        } else {
          summaryStatsEl.innerHTML = `
            <div class="stat">
              <span class="label">Goals</span>
              <span class="value">${formatNumber.format(totalGoals)}</span>
              <span class="sub">${formatNumber.format(totalXg)} expected</span>
            </div>
            <div class="stat">
              <span class="label">Assists</span>
              <span class="value">${formatNumber.format(totalAssists)}</span>
              <span class="sub">${formatNumber.format(totalXa)} expected</span>
            </div>
            <div class="stat">
              <span class="label">Minutes Logged</span>
              <span class="value">${formatNumber.format(totalMinutes)}</span>
              <span class="sub">≈ ${formatNumber.format(matchesEquivalent)} matches</span>
            </div>
            <div class="stat">
              <span class="label">Squad Avg Age</span>
              <span class="value">${formatNumber.format(avgAge)} yrs</span>
              <span class="sub">Avg goals/90: ${formatNumber.format(avgGoalsPer90)}</span>
            </div>
          `;
        }
      }

      const goalsAssistsData = rows
        .map((row) => ({
          x: numeric(row.goals),
          y: numeric(row.assists),
          label: row.player,
          team: row.team || "—",
        }))
        .filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y));
      const goalsAssistsHighlights = highlightDatasetsFor(rows, (row) => ({
        x: numeric(row.goals),
        y: numeric(row.assists),
      }));
      upsertChart("goalsAssists", document.getElementById("goalsAssistsChart"), {
        type: "scatter",
        data: {
          datasets: [
            {
              label: "Goals vs Assists",
              data: goalsAssistsData,
              backgroundColor: "#38bdf8",
              borderColor: "#38bdf8",
              pointRadius: 4,
              pointHoverRadius: 6,
            },
            ...goalsAssistsHighlights,
          ],
        },
        options: {
          responsive: true,
          plugins: {
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const d = ctx.raw;
                  return `${d.label} (${d.team}) — ${d.x} G / ${d.y} A`;
                },
              },
            },
          },
          scales: {
            x: { title: { display: true, text: "Goals" } },
            y: { title: { display: true, text: "Assists" } },
          },
        },
      });

      const goalsXgData = rows
        .map((row) => ({
          x: xgPer90(row),
          y: goalsPer90(row),
          label: row.player,
          team: row.team || "—",
        }))
        .filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y));
      const goalsXgHighlights = highlightDatasetsFor(rows, (row) => ({
        x: xgPer90(row),
        y: goalsPer90(row),
      }));
      upsertChart("goalsXg", document.getElementById("goalsXgChart"), {
        type: "scatter",
        data: {
          datasets: [
            {
              label: "Goals vs xG per 90",
              data: goalsXgData,
              backgroundColor: "#f97316",
              borderColor: "#f97316",
              pointRadius: 4,
              pointHoverRadius: 6,
            },
            ...goalsXgHighlights,
          ],
        },
        options: {
          plugins: {
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const d = ctx.raw;
                  return `${d.label} (${d.team}) — Goals/90: ${formatNumber.format(d.y)}, xG/90: ${formatNumber.format(d.x)}`;
                },
              },
            },
          },
          scales: {
            x: { title: { display: true, text: "xG per 90" } },
            y: { title: { display: true, text: "Goals per 90" } },
          },
        },
      });

      const xgValues = rows.map((row) => xgPer90(row)).filter((value) => Number.isFinite(value) && value >= 0);
      const histogramCtx = document.getElementById("xgHistogram");
      if (histogramCtx) {
        if (!xgValues.length) {
          upsertChart("xgHistogram", histogramCtx, {
            type: "bar",
            data: { labels: ["No data"], datasets: [{ label: "xG per 90", data: [0], backgroundColor: "#94a3b8" }] },
          });
        } else {
          const binCount = 12;
          const maxXg = Math.max(...xgValues);
          const binSize = Math.max(maxXg / binCount, 0.05);
          const bins = new Array(binCount).fill(0);
          xgValues.forEach((value) => {
            const index = Math.min(binCount - 1, Math.floor(value / binSize));
            bins[index] += 1;
          });
          const labels = bins.map((_, idx) => `${formatNumber.format(idx * binSize)}-${formatNumber.format((idx + 1) * binSize)}`);
          upsertChart("xgHistogram", histogramCtx, {
            type: "bar",
            data: {
              labels,
              datasets: [{ label: "Players", data: bins, backgroundColor: "#c084fc" }],
            },
            options: {
              scales: {
                x: { title: { display: true, text: "xG per 90 bin" } },
                y: { title: { display: true, text: "Players" } },
              },
            },
          });
        }
      }

      const assistsXaData = rows
        .map((row) => ({
          x: xgAssistPer90(row),
          y: assistsPer90(row),
          label: row.player,
          team: row.team || "—",
        }))
        .filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y));
      const assistsXaHighlights = highlightDatasetsFor(rows, (row) => ({
        x: xgAssistPer90(row),
        y: assistsPer90(row),
      }));
      upsertChart("assistsXaChart", document.getElementById("assistsXaChart"), {
        type: "scatter",
        data: {
          datasets: [
            {
              label: "Assists vs xA per 90",
              data: assistsXaData,
              backgroundColor: "#34d399",
              borderColor: "#34d399",
              pointRadius: 4,
              pointHoverRadius: 6,
            },
            ...assistsXaHighlights,
          ],
        },
        options: {
          plugins: {
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const d = ctx.raw;
                  return `${d.label} (${d.team}) — Assists/90: ${formatNumber.format(d.y)}, xA/90: ${formatNumber.format(d.x)}`;
                },
              },
            },
          },
          scales: {
            x: { title: { display: true, text: "xA per 90" } },
            y: { title: { display: true, text: "Assists per 90" } },
          },
        },
      });

      const gaRanking = [...rows]
        .map((row) => ({
          label: row.player || row.team || "Unknown",
          value: gaPer90(row),
        }))
        .filter((item) => Number.isFinite(item.value))
        .sort((a, b) => b.value - a.value)
        .slice(0, 15);
      upsertChart("gaRanking", document.getElementById("gaRankingChart"), {
        type: "bar",
        data: {
          labels: gaRanking.map((item) => item.label),
          datasets: [{
            label: "G+A per 90",
            data: gaRanking.map((item) => item.value),
            backgroundColor: "#fbbf24",
          }],
        },
        options: {
          indexAxis: "y",
          scales: { x: { title: { display: true, text: "G+A per 90" } } },
        },
      });

      const ageBuckets = new Map();
      rows.forEach((row) => {
        let age = numeric(row.age);
        if (!age) {
          const computed = computeAge(row, currentYear);
          age = computed === null ? null : computed;
        }
        if (age === null || !Number.isFinite(age) || age <= 0) return;
        const bucket = ageBuckets.get(age) || { total: 0, count: 0 };
        bucket.total += goalsPer90(row);
        bucket.count += 1;
        ageBuckets.set(age, bucket);
      });
      const sortedAges = [...ageBuckets.entries()].sort((a, b) => a[0] - b[0]);
      upsertChart("goalsAgeCurve", document.getElementById("goalsAgeCurve"), {
        type: "line",
        data: {
          labels: sortedAges.map(([age]) => age),
          datasets: [{
            label: "Goals per 90",
            data: sortedAges.map(([, info]) => info.total / info.count),
            borderColor: "#38bdf8",
            backgroundColor: "rgba(56, 189, 248, 0.2)",
            tension: 0.3,
          }],
        },
        options: {
          scales: {
            x: { title: { display: true, text: "Age" } },
            y: { title: { display: true, text: "Goals per 90" } },
          },
        },
      });

      const minutesGaData = rows
        .map((row) => ({
          x: minutesPlayed(row),
          y: gaPer90(row),
          label: row.player,
          team: row.team || "—",
        }))
        .filter((point) => Number.isFinite(point.x) && Number.isFinite(point.y));
      const minutesGaHighlights = highlightDatasetsFor(rows, (row) => ({
        x: minutesPlayed(row),
        y: gaPer90(row),
      }));
      upsertChart("minutesGa", document.getElementById("minutesGaChart"), {
        type: "scatter",
        data: {
          datasets: [
            {
              label: "Minutes vs G+A/90",
              data: minutesGaData,
              backgroundColor: "#f472b6",
              borderColor: "#f472b6",
              pointRadius: 4,
              pointHoverRadius: 6,
            },
            ...minutesGaHighlights,
          ],
        },
        options: {
          plugins: {
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const d = ctx.raw;
                  return `${d.label} (${d.team}) — Minutes: ${formatNumber.format(d.x)}, G+A/90: ${formatNumber.format(d.y)}`;
                },
              },
            },
          },
          scales: {
            x: { title: { display: true, text: "Minutes played" } },
            y: { title: { display: true, text: "G+A per 90" } },
          },
        },
      });

      const teamGoals = aggregateBy(rows, "team", "goals").slice(0, 12);
      upsertChart("teamContribution", document.getElementById("teamContributionChart"), {
        type: "bar",
        data: {
          labels: teamGoals.map((item) => item[0]),
          datasets: [{
            label: "Goals",
            data: teamGoals.map((item) => item[1]),
            backgroundColor: "#34d399aa",
            borderColor: "#34d399",
          }],
        },
        options: {
          indexAxis: "y",
          scales: { x: { title: { display: true, text: "Goals" } } },
        },
      });

      const progressionData = rows
        .map((row) => ({
          x: numeric(row.progressive_carries),
          y: numeric(row.progressive_passes),
          player: row.player,
          team: row.team || "—",
        }))
        .filter((point) => Number.isFinite(point.x) || Number.isFinite(point.y));
      const progressionHighlights = highlightDatasetsFor(rows, (row) => ({
        x: numeric(row.progressive_carries),
        y: numeric(row.progressive_passes),
      }));
      upsertChart("progression", document.getElementById("progressionChart"), {
        type: "scatter",
        data: {
          datasets: [
            {
              label: "Progression Mix",
              data: progressionData,
              backgroundColor: "#f472b6aa",
              borderColor: "#f472b6",
              pointRadius: 4,
            },
            ...progressionHighlights,
          ],
        },
        options: {
          plugins: {
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const d = ctx.raw;
                  return `${d.player} (${d.team}) — Carries: ${formatNumber.format(d.x)}, Passes: ${formatNumber.format(d.y)}`;
                },
              },
            },
          },
          scales: {
            x: { title: { display: true, text: "Progressive Carries" } },
            y: { title: { display: true, text: "Progressive Passes" } },
          },
        },
      });

      const rowsWithMinutes = rows.filter((row) => numeric(row.minutes) > 0);
      const efficiencyData = rowsWithMinutes
        .map((row) => ({
          x: numeric(row.minutes),
          y: safePer90(numeric(row.goals) + numeric(row.assists), numeric(row.minutes)),
          player: row.player,
          team: row.team || "—",
        }));
      const efficiencyHighlights = highlightDatasetsFor(rowsWithMinutes, (row) => ({
        x: numeric(row.minutes),
        y: safePer90(numeric(row.goals) + numeric(row.assists), numeric(row.minutes)),
      }));
      upsertChart("efficiency", document.getElementById("efficiencyChart"), {
        type: "scatter",
        data: {
          datasets: [
            {
              label: "Minutes vs Goal Contributions per 90",
              data: efficiencyData,
              backgroundColor: "#facc15",
              borderColor: "#facc15",
              pointRadius: 4,
            },
            ...efficiencyHighlights,
          ],
        },
        options: {
          plugins: {
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const d = ctx.raw;
                  return `${d.player} (${d.team}) — ${formatNumber.format(d.y)} G+A/90 at ${formatNumber.format(d.x)} mins`;
                },
              },
            },
          },
          scales: {
            x: { title: { display: true, text: "Minutes played" } },
            y: { title: { display: true, text: "Goals + Assists per 90" } },
          },
        },
      });

      const xgChainData = rows
        .map((row) => ({
          x: numeric(row.xg_per90) || safePer90(numeric(row.xg), numeric(row.minutes)),
          y: numeric(row.xg_assist_per90) || safePer90(numeric(row.xg_assist), numeric(row.minutes)),
          player: row.player,
          team: row.team || "—",
        }));
      const xgChainHighlights = highlightDatasetsFor(rows, (row) => ({
        x: numeric(row.xg_per90) || safePer90(numeric(row.xg), numeric(row.minutes)),
        y: numeric(row.xg_assist_per90) || safePer90(numeric(row.xg_assist), numeric(row.minutes)),
      }));
      upsertChart("xgChain", document.getElementById("xgChainChart"), {
        type: "scatter",
        data: {
          datasets: [
            {
              label: "xG vs xGA per 90",
              data: xgChainData,
              backgroundColor: "#a855f7",
              borderColor: "#a855f7",
              pointRadius: 4,
            },
            ...xgChainHighlights,
          ],
        },
        options: {
          plugins: {
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const d = ctx.raw;
                  return `${d.player} (${d.team}) — xG/90: ${formatNumber.format(d.x)}, xGA/90: ${formatNumber.format(d.y)}`;
                },
              },
            },
          },
          scales: {
            x: { title: { display: true, text: "xG per 90" } },
            y: { title: { display: true, text: "xG Assist per 90" } },
          },
        },
      });

      const deltas = rows
        .map((row) => ({
          player: row.player,
          team: row.team,
          delta: numeric(row.goals) - numeric(row.xg),
        }))
        .filter((row) => row.player)
        .sort((a, b) => b.delta - a.delta);

      const clinical = deltas.slice(0, TOP_N);
      const unlucky = deltas.slice(-TOP_N).reverse();
      overUnderBody.innerHTML = clinical
        .map((row, index) => {
          const unluckyRow = unlucky[index] || { player: "—", delta: 0 };
          return `
            <tr>
              <td>${row.player}</td>
              <td>${formatNumber.format(row.delta)}</td>
              <td>${unluckyRow.player}</td>
              <td>${formatNumber.format(unluckyRow.delta)}</td>
            </tr>
          `;
        })
        .join("");

      const per90Data = rows
        .map((row) => {
          const mins = Math.max(1, numeric(row.minutes));
          const goals90 = numeric(row.goals_per90) || safePer90(numeric(row.goals), mins);
          const assists90 = numeric(row.assists_per90) || safePer90(numeric(row.assists), mins);
          return {
            x: goals90,
            y: assists90,
            minutes: mins,
            player: row.player,
            team: row.team || "—",
          };
        })
        .filter((entry) => entry.player && (entry.x > 0 || entry.y > 0))
        .sort((a, b) => (b.x + b.y) - (a.x + a.y))
        .slice(0, 120);
      upsertChart("per90", document.getElementById("per90Chart"), {
        type: "scatter",
        data: {
          datasets: [{
            label: "Goals/90 vs Assists/90",
            data: per90Data,
            backgroundColor: "#c084fcaa",
            borderColor: "#c084fc",
            pointRadius: (ctx) => Math.min(8, Math.max(3, (ctx.raw?.minutes ?? 0) / 400)),
          }],
        },
        options: {
          plugins: {
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const d = ctx.raw;
                  return `${d.player} (${d.team}) — G/90: ${formatNumber.format(d.x)}, A/90: ${formatNumber.format(d.y)}`;
                },
              },
            },
          },
          scales: {
            x: { title: { display: true, text: "Goals per 90" } },
            y: { title: { display: true, text: "Assists per 90" } },
          },
        },
      });

      const positionTotals = rows.reduce((map, row) => {
        const pos = primaryPosition(row.position);
        map.set(pos, (map.get(pos) || 0) + numeric(row.minutes));
        return map;
      }, new Map());
      const positionMinutes = [...positionTotals.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8);
      const positionColors = ["#60a5fa", "#34d399", "#fbbf24", "#f87171", "#a78bfa", "#38bdf8", "#f472b6", "#cbd5f5"];
      upsertChart("positionMinutes", document.getElementById("positionMinutesChart"), {
        type: "doughnut",
        data: {
          labels: positionMinutes.map((item) => item[0]),
          datasets: [{
            data: positionMinutes.map((item) => item[1]),
            backgroundColor: positionMinutes.map((_, idx) => positionColors[idx % positionColors.length]),
          }],
        },
        options: {
          plugins: { legend: { position: "bottom" } },
        },
      });

      insightsEl.innerHTML = "";
      const avgGoals = rows.reduce((sum, row) => sum + (numeric(row.goals_per90) || numeric(row.goals)), 0) / (rows.length || 1);
      const avgAssists = rows.reduce((sum, row) => sum + (numeric(row.assists_per90) || numeric(row.assists)), 0) / (rows.length || 1);
      const topMinutes = [...rows].sort((a, b) => numeric(b.minutes) - numeric(a.minutes)).slice(0, 1)[0];
      const progressionLeaders = aggregateBy(rows, "player", "progressive_carries")
        .slice(0, 1)
        .map(([player, carries]) => `${player} leads with ${formatNumber.format(carries)} carries.`)
        .join("");

      const insightMessages = [
        `Average output: ${formatNumber.format(avgGoals)} goals & ${formatNumber.format(avgAssists)} assists per row.`,
        topMinutes ? `${topMinutes.player} logs the heaviest load at ${formatNumber.format(numeric(topMinutes.minutes))} minutes.` : "",
        progressionLeaders,
      ].filter(Boolean);
      insightMessages.forEach((text) => {
        const div = document.createElement("div");
        div.className = "insight";
        div.textContent = text;
        insightsEl.appendChild(div);
      });

      if (storylinesEl) {
        const storylineItems = [];
        if (clinical[0]) {
          storylineItems.push(`${clinical[0].player} (${clinical[0].team || "—"}) is beating xG by ${formatNumber.format(clinical[0].delta)} goals.`);
        }
        if (unlucky[0]) {
          storylineItems.push(`${unlucky[0].player} (${unlucky[0].team || "—"}) is due with ${formatNumber.format(unlucky[0].delta)} goals below xG.`);
        }
        const mileageLeader = [...rows].sort((a, b) => numeric(b.minutes) - numeric(a.minutes))[0];
        if (mileageLeader) {
          storylineItems.push(`${mileageLeader.player} logs ${formatNumber.format(numeric(mileageLeader.minutes))} minutes so far.`);
        }
        const bestCombo = rows
          .filter((row) => numeric(row.assists) > 0 && numeric(row.progressive_passes) > 0)
          .sort((a, b) => (numeric(b.assists) + numeric(b.progressive_passes)) - (numeric(a.assists) + numeric(a.progressive_passes)))[0];
        if (bestCombo) {
          storylineItems.push(`Playmaking engine: ${bestCombo.player} pairs ${formatNumber.format(numeric(bestCombo.assists))} assists with ${formatNumber.format(numeric(bestCombo.progressive_passes))} progressive passes.`);
        }
        storylinesEl.innerHTML = storylineItems.length ? storylineItems.map((text) => `<li>${text}</li>`).join("") : "<li>No storyline data for this filter.</li>";
      }

      const finishingMix = [...deltas.slice(0, TOP_N), ...deltas.slice(-TOP_N).reverse()].filter((row) => row);
      upsertChart("finishing", document.getElementById("finishingChart"), {
        type: "bar",
        data: {
          labels: finishingMix.map((row) => `${row.player} (${row.team || "—"})`),
          datasets: [{
            label: "Goals - xG",
            data: finishingMix.map((row) => row.delta),
            backgroundColor: finishingMix.map((row) => (row.delta >= 0 ? "#86efac" : "#fca5a5")),
            borderColor: finishingMix.map((row) => (row.delta >= 0 ? "#22c55e" : "#f87171")),
          }],
        },
        options: {
          indexAxis: "y",
          scales: { x: { title: { display: true, text: "Goals above expectation" } } },
        },
      });
    }

    if (filterControls.position) {
      filterControls.position.addEventListener("change", (event) => {
        filterState.position = event.target.value || "all";
        updateDashboard();
      });
    }
    if (filterControls.league) {
      filterControls.league.addEventListener("change", (event) => {
        filterState.league = event.target.value || "all";
        updateDashboard();
      });
    }
    if (filterControls.team) {
      filterControls.team.addEventListener("change", (event) => {
        filterState.team = event.target.value || "all";
        updateDashboard();
      });
    }
    if (filterControls.minMinutes) {
      filterControls.minMinutes.addEventListener("input", (event) => {
        const value = Number(event.target.value);
        filterState.minMinutes = Number.isFinite(value) && value > 0 ? value : 0;
        updateDashboard();
      });
    }
    if (filterControls.maxAge) {
      filterControls.maxAge.addEventListener("input", (event) => {
        if (event.target.disabled) return;
        const value = Number(event.target.value);
        filterState.maxAge = Number.isFinite(value) && value > 0 ? value : "";
        updateDashboard();
      });
    }
    if (filterControls.reset) {
      filterControls.reset.addEventListener("click", () => {
        resetFilterState();
        syncFilterControls();
        updateDashboard();
      });
    }
    datasetSelect.addEventListener("change", () => {
      resetFilterState();
      compareState.playerA = null;
      compareState.playerB = null;
      populateFilterControls(DATASETS[datasetSelect.value]);
      updateDashboard();
    });

    datasetSelect.value = "0";
    updateTabs("overview");
    populateFilterControls(DATASETS[0]);
    updateDashboard();
  </script>
</body>
</html>
"""
    return template.replace("__DATASETS_JSON__", datasets_json)


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, dashboard_html: str, datasets_json: str, **kwargs):
        self._dashboard_html = dashboard_html
        self._datasets_json = datasets_json
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def do_GET(self) -> None:  # noqa: D401
        if self.path in ("/", "/index.html"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(self._dashboard_html.encode("utf-8"))
            return
        if self.path == "/data.json":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(self._datasets_json.encode("utf-8"))
            return
        super().do_GET()


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve an interactive dashboard for local CSV files.")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")
    args = parser.parse_args()

    datasets = load_datasets()
    if not datasets:
        raise SystemExit("No CSV files found. Drop a CSV next to visualize.py and try again.")

    datasets_json = json.dumps(datasets, ensure_ascii=False)
    dashboard_html = build_dashboard_html(datasets_json)

    handler = partial(DashboardHandler, dashboard_html=dashboard_html, datasets_json=datasets_json)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), handler)
    print(f"Dashboard available at http://127.0.0.1:{args.port} (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down…")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
