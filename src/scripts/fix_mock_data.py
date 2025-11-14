import csv
import re
from pathlib import Path
from collections import defaultdict


def extract_id_number(id_str: str) -> int:
    """Extract number from ID string like 'CUS_001' -> 1, 'ORG_002' -> 2"""
    if not id_str or id_str.strip() == "":
        return None
    match = re.search(r'_(\d+)$', id_str.strip())
    if match:
        return int(match.group(1))
    return None


def fix_csv_file(csv_path: Path):
    """Fix CSV file by converting string IDs to numeric IDs"""
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        header = reader.fieldnames

    if not rows:
        return

    # Identify ID columns (FID and foreign key columns ending with ID)
    id_columns = [col for col in header if col.upper() == "FID" or col.upper().endswith("ID")]

    # Convert string IDs to numeric IDs
    fixed_rows = []
    for row in rows:
        fixed_row = row.copy()
        for col in id_columns:
            value = row.get(col, "").strip()
            if value:
                # Extract numeric part from ID string
                num = extract_id_number(value)
                if num is not None:
                    fixed_row[col] = str(num)
                else:
                    # If it's already a number, keep it
                    try:
                        int(value)
                        fixed_row[col] = value
                    except ValueError:
                        # If it's not a valid ID format, set to None
                        fixed_row[col] = ""
        fixed_rows.append(fixed_row)

    # Write back to file (without BOM)
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(fixed_rows)

    return len(fixed_rows)


def fix_all_mock_data(data_dir: str):
    """Fix all CSV files in the mock data directory"""
    data_path = Path(data_dir)
    if not data_path.exists():
        raise ValueError(f"Data directory does not exist: {data_dir}")

    csv_files = list(data_path.glob("*_虚拟数据.csv"))
    if not csv_files:
        print(f"No CSV files found in {data_dir}")
        return

    print(f"Found {len(csv_files)} CSV files to fix")
    for csv_file in csv_files:
        try:
            count = fix_csv_file(csv_file)
            print(f"✓ Fixed {csv_file.name}: {count} rows")
        except Exception as e:
            print(f"✗ Error fixing {csv_file.name}: {e}")

    print("\nAll CSV files have been fixed!")


if __name__ == "__main__":
    data_dir = "./data/mock_data"
    fix_all_mock_data(data_dir)

