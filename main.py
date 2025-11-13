#!/usr/bin/env python3
import sys
from bs4 import BeautifulSoup
from pathlib import Path
import csv

# --- usage check ---
if len(sys.argv) < 2:
    print("Usage: python scrape.py <file.html>")
    sys.exit(1)

html_path = Path(sys.argv[1])
if not html_path.exists():
    print(f"Error: File '{html_path}' not found.")
    sys.exit(1)

# --- read html file ---
html = html_path.read_text(encoding="utf-8", errors="ignore")

# --- parse with BeautifulSoup ---
soup = BeautifulSoup(html, "lxml")

# find all player rows (FBref-style example)
rows = []
for tr in soup.select("tr[data-row]"):
    row = {}
    for cell in tr.find_all(["th", "td"], recursive=False):
        stat = cell.get("data-stat")
        if stat:
            text = cell.get_text(" ", strip=True)
            row[stat] = text
    if row:
        rows.append(row)

# --- output ---
if rows:
    # collect all field names in order of first appearance
    fieldnames = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)

    out_file = html_path.with_suffix(".csv")
    with out_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"✅ Extracted {len(rows)} rows → {out_file}")
else:
    print("No data-row elements found.")
