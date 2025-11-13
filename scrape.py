#!/usr/bin/env python3
import sys
from pathlib import Path
import csv
import argparse

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:
    BeautifulSoup = None

# ---------- helpers ----------

def normalize(s: str) -> str:
    if not s:
        return ""
    return " ".join(s.strip().lower().split())  # lowercase + collapse spaces


def make_key(name: str, birth_year: str | int | None, team: str | None) -> str:
    return "|".join([
        normalize(name),
        str(birth_year or "").strip(),
        normalize(team or ""),
    ])


def load_index(index_path: Path):
    """
    Load an existing player index if it exists.
    Returns: (dict key -> player_id, max_id)
    """
    mapping = {}
    max_id = 0

    if index_path.exists():
        with index_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pid = int(row["player_id"])
                key = row["key"]
                mapping[key] = pid
                if pid > max_id:
                    max_id = pid

    return mapping, max_id


def save_index(mapping, index_path: Path):
    """
    Save key -> id mapping back to disk.
    We also store name / birth_year / team for debugging.
    """
    # Rebuild rows from keys
    # key format: norm_name|birth_year|norm_team
    rows = []
    for key, pid in sorted(mapping.items(), key=lambda x: x[1]):
        norm_name, birth_year, norm_team = key.split("|", 2)
        rows.append({
            "player_id": pid,
            "key": key,
            "norm_name": norm_name,
            "birth_year": birth_year,
            "norm_team": norm_team,
        })

    with index_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["player_id", "key", "norm_name", "birth_year", "norm_team"],
        )
        writer.writeheader()
        writer.writerows(rows)


def get_or_create_player_id(row, mapping, max_id_ref):
    """
    Given a scraped row dict, find or assign a player_id using (name, birth_year, team).
    max_id_ref is a list [max_id] so we can update it inside the function.
    """
    name = row.get("player") or row.get("player_name")
    birth_year = row.get("birth_year")
    team = row.get("team")

    if not name:
        return None  # can't index without at least a name

    key = make_key(name, birth_year, team)

    if key in mapping:
        return mapping[key]

    # assign new id
    max_id_ref[0] += 1
    pid = max_id_ref[0]
    mapping[key] = pid
    return pid

# ---------- data loading utilities ----------

def rows_from_html(html_path: Path):
    html = html_path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "lxml")
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
    return rows


def rows_from_csv(csv_path: Path):
    with csv_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames[:] if reader.fieldnames else []
        rows = [dict(row) for row in reader]
    return rows, fieldnames


def build_fieldnames(existing_fieldnames, rows):
    if existing_fieldnames:
        fieldnames = list(existing_fieldnames)
    else:
        fieldnames = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    if "player_id" not in fieldnames:
        fieldnames.insert(0, "player_id")
    return fieldnames


def write_csv(rows, fieldnames, out_path: Path):
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ---------- database helpers ----------

def load_database(db_path: Path):
    if not db_path.exists():
        return {}, ["player_id"]

    with db_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames[:] if reader.fieldnames else ["player_id"]
        rows = {}
        for row in reader:
            pid = row.get("player_id")
            if not pid:
                continue
            rows[str(pid)] = row
    return rows, fieldnames


def merge_fieldnames(existing, incoming):
    merged = []
    for field_list in (existing or [], incoming or []):
        for field in field_list:
            if field not in merged:
                merged.append(field)
    if "player_id" not in merged:
        merged.insert(0, "player_id")
    else:
        merged = ["player_id"] + [f for f in merged if f != "player_id"]
    return merged


def update_database(db_path: Path, incoming_rows, incoming_fields):
    db_rows, db_fields = load_database(db_path)
    fieldnames = merge_fieldnames(db_fields, incoming_fields)

    for row in incoming_rows:
        pid = row.get("player_id")
        if pid is None:
            continue
        pid = str(pid)
        existing = db_rows.get(pid, {"player_id": pid})
        existing.update(row)
        db_rows[pid] = existing

    # fill missing columns with empty string placeholder
    ordered_rows = []
    for pid in sorted(db_rows.keys(), key=lambda x: int(x) if str(x).isdigit() else x):
        row = db_rows[pid]
        for field in fieldnames:
            row.setdefault(field, "")
        ordered_rows.append(row)

    with db_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(ordered_rows)

    return fieldnames


# ---------- main scraping ----------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Assign stable player IDs and sync a master database."
    )
    parser.add_argument("index_csv", help="Path to the player index CSV (id lookup table).")
    parser.add_argument("database_csv", help="Path to the master database CSV to update.")
    parser.add_argument("source_path", help="HTML or CSV file containing new stats.")
    parser.add_argument(
        "--skip-source-export",
        action="store_true",
        help="Update the database only (no per-source CSV is written).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    index_path = Path(args.index_csv)
    database_path = Path(args.database_csv)
    data_path = Path(args.source_path)

    if not data_path.exists():
        print(f"Error: File '{data_path}' not found.")
        sys.exit(1)

    ext = data_path.suffix.lower()
    if ext in {".html", ".htm"}:
        if BeautifulSoup is None:
            print("Error: BeautifulSoup (bs4) is required for HTML inputs. Install with 'pip install beautifulsoup4 lxml'.")
            sys.exit(1)
        rows = rows_from_html(data_path)
        base_headers = []
    elif ext == ".csv":
        rows, base_headers = rows_from_csv(data_path)
    else:
        print("Error: Supported input extensions are .html, .htm, or .csv")
        sys.exit(1)

    if not rows:
        print("No data rows found.")
        return

    mapping, max_id = load_index(index_path)
    max_id_ref = [max_id]

    for row in rows:
        pid = get_or_create_player_id(row, mapping, max_id_ref)
        if pid is not None:
            row["player_id"] = pid

    fieldnames = build_fieldnames(base_headers, rows)

    if args.skip_source_export:
        print(f"ℹ️ Source export skipped (database-only mode).")
    else:
        out_path = data_path.with_suffix(".csv") if ext != ".csv" else data_path
        write_csv(rows, fieldnames, out_path)
        print(f"✅ Updated {out_path} with player_id column ({len(rows)} rows)")

    save_index(mapping, index_path)
    print(f"🆔 Player index updated → {index_path} (max id = {max_id_ref[0]})")

    db_fields = update_database(database_path, rows, fieldnames)
    print(f"🗄️ Database synchronized → {database_path} ({len(db_fields)} columns)")

if __name__ == "__main__":
    main()
