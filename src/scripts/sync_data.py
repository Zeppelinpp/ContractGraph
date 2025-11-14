import csv
import os
import psycopg2
from pathlib import Path
from tqdm import tqdm
from src.settings import settings


def get_table_name_from_filename(filename: str) -> str:
    """Extract table name from filename like 't_xxx_虚拟数据.csv'"""
    name = filename.replace("_虚拟数据.csv", "").replace(".csv", "")
    return name


def import_csv_to_table(
    csv_path: Path, table_name: str, db_config: dict, dbname: str = "postgres"
):
    """Import CSV data to PostgreSQL table"""
    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        user=db_config["user"],
        password=db_config["password"],
        dbname=dbname,
    )
    cur = conn.cursor()

    try:
        # Read CSV file (handle BOM)
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader)

        # Get column count from table
        cur.execute(
            f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        table_columns = [row[0] for row in cur.fetchall()]

        if not table_columns:
            raise ValueError(f"Table {table_name} does not exist or has no columns")

        # Get column types and nullable info from table
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        table_col_info = {row[0]: {"type": row[1], "nullable": row[2] == "YES"} for row in cur.fetchall()}

        # Map CSV columns to table columns (case-insensitive)
        column_mapping = []
        for csv_col in header:
            csv_col_upper = csv_col.upper()
            matched = False
            for table_col in table_columns:
                if table_col.upper() == csv_col_upper:
                    # Check if column type matches (skip bigint columns with non-numeric values)
                    col_info = table_col_info.get(table_col, {})
                    col_type = col_info.get("type", "")
                    if col_type == "bigint" and csv_col_upper not in ["FID", "FPARTAID", "FPARTBID", "FPARTCID", "FPARTDID", "FOPERATORID", "FRELATECONTRACTID", "FCASETYPEID"]:
                        # Skip non-ID bigint columns that might have string values
                        column_mapping.append(None)
                    else:
                        column_mapping.append(table_col)
                    matched = True
                    break
            if not matched:
                column_mapping.append(None)

        # Read and insert data (handle BOM)
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            return (table_name, True, None, 0)

        # Build insert statement
        valid_columns = [col for col in column_mapping if col is not None]
        placeholders = ", ".join(["%s"] * len(valid_columns))
        columns_str = ", ".join(valid_columns)
        insert_sql = f"INSERT INTO public.{table_name} ({columns_str}) VALUES ({placeholders})"

        # Clear existing data and reset sequences
        cur.execute(f"TRUNCATE TABLE public.{table_name} CASCADE")
        
        # Reset sequences for FID column if it exists
        try:
            cur.execute(
                f"""
                SELECT setval(pg_get_serial_sequence('public.{table_name}', 'fid'), 1, false)
                WHERE EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = %s 
                    AND column_name = 'fid'
                    AND data_type IN ('bigint', 'integer', 'smallint')
                )
                """,
                (table_name,),
            )
        except Exception:
            pass

        # Insert data
        inserted_count = 0
        for row in rows:
            values = []
            for csv_col, table_col in zip(header, column_mapping):
                if table_col is not None:
                    value = row.get(csv_col, "").strip()
                    col_info = table_col_info.get(table_col, {})
                    is_nullable = col_info.get("nullable", True)
                    col_type = col_info.get("type", "")
                    
                    if value == "":
                        if not is_nullable:
                            # Provide default value for non-nullable columns
                            if col_type in ["bigint", "integer", "smallint"]:
                                values.append(0)
                            elif col_type in ["numeric", "double precision", "real"]:
                                values.append(0)
                            else:
                                values.append("")
                        else:
                            values.append(None)
                    else:
                        values.append(value)
            cur.execute(insert_sql, values)
            inserted_count += 1

        conn.commit()
        return (table_name, True, None, inserted_count)

    except Exception as e:
        conn.rollback()
        return (table_name, False, str(e), 0)
    finally:
        cur.close()
        conn.close()


def sync_data(data_dir: str, dbname: str = "postgres", max_workers: int = None):
    """Import all CSV files from data directory to PostgreSQL"""
    data_path = Path(data_dir)
    if not data_path.exists():
        raise ValueError(f"Data directory does not exist: {data_dir}")

    # Find all CSV files
    csv_files = list(data_path.glob("*_虚拟数据.csv"))
    if not csv_files:
        print(f"No CSV files found in {data_dir}")
        return

    total_files = len(csv_files)
    db_config = settings.pg_config["local"]

    results = []
    with tqdm(total=total_files, desc="Importing data") as pbar:
        for csv_file in csv_files:
            table_name = get_table_name_from_filename(csv_file.name)
            result = import_csv_to_table(csv_file, table_name, db_config, dbname)
            results.append(result)
            table_name, success, error, count = result
            if success:
                pbar.set_postfix_str(f"✓ {table_name} ({count} rows)")
            else:
                pbar.set_postfix_str(f"✗ {table_name}: {error}")
            pbar.update(1)

    # Print summary
    success_count = sum(1 for _, success, _, _ in results if success)
    failed_count = total_files - success_count
    total_rows = sum(count for _, _, _, count in results)
    print(
        f"\nCompleted: {success_count}/{total_files} succeeded, {failed_count} failed"
    )
    print(f"Total rows imported: {total_rows}")

    if failed_count > 0:
        print("\nFailed files:")
        for table_name, success, error, _ in results:
            if not success:
                print(f"  {table_name}: {error}")


if __name__ == "__main__":
    # Get data directory relative to project root
    data_dir = "./data/mock_data"

    sync_data(str(data_dir))

