import psycopg2
from psycopg2.extras import DictCursor
import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.settings import settings


class DataPipeline:
    def __init__(self):
        pass

    def _sync_data(self, db_name: str, table_name: str, local_dir: str):
        # Create a new connection for each task to avoid sharing issues in concurrent execution
        conn = psycopg2.connect(
            host=settings.pg_config["remote"]["host"],
            port=settings.pg_config["remote"]["port"],
            user=settings.pg_config["remote"]["user"],
            password=settings.pg_config["remote"]["password"],
            dbname=db_name,
        )
        cur = conn.cursor(cursor_factory=DictCursor)

        try:
            cur.execute(f"SELECT * FROM {table_name}")
            rows = cur.fetchall()

            if not rows:
                return (db_name, table_name, 0, "No data found")

            if not os.path.exists(local_dir):
                os.makedirs(local_dir)

            local_path = os.path.join(local_dir, f"{table_name}.csv")

            # Get column names from cursor description
            column_names = [desc[0] for desc in cur.description]

            with open(local_path, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=column_names)
                writer.writeheader()
                for row in rows:
                    writer.writerow(dict(row))

            return (db_name, table_name, len(rows), None)
        except Exception as e:
            return (db_name, table_name, 0, str(e))
        finally:
            cur.close()
            conn.close()

    def sync(self, sync_config: dict, local_dir: str, max_workers: int = 8):
        # Prepare all tasks
        tasks = []
        for db_name, tables in sync_config.items():
            if isinstance(tables, str):
                tables = [tables]
            for table_name in tables:
                tasks.append((db_name, table_name, local_dir))

        # Execute tasks concurrently with progress bar
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self._sync_data, db_name, table_name, local_dir): (
                    db_name,
                    table_name,
                )
                for db_name, table_name, _ in tasks
            }

            # Process completed tasks with progress bar
            with tqdm(total=len(tasks), desc="Syncing data") as pbar:
                for future in as_completed(future_to_task):
                    db_name, table_name = future_to_task[future]
                    try:
                        result = future.result()
                        db_name, table_name, row_count, error = result
                        if error:
                            pbar.set_postfix_str(f"✗ {db_name}.{table_name}: {error}")
                        else:
                            pbar.set_postfix_str(
                                f"✓ {db_name}.{table_name} ({row_count} rows)"
                            )
                        results.append(result)
                    except Exception as e:
                        pbar.set_postfix_str(f"✗ {db_name}.{table_name}: {str(e)}")
                        results.append((db_name, table_name, 0, str(e)))
                    pbar.update(1)

        # Print summary
        success_count = sum(1 for _, _, _, error in results if error is None)
        failed_count = len(results) - success_count
        total_rows = sum(row_count for _, _, row_count, _ in results)

        print(
            f"\nCompleted: {success_count}/{len(results)} succeeded, {failed_count} failed"
        )
        print(f"Total rows exported: {total_rows}")

        if failed_count > 0:
            print("\nFailed tasks:")
            for db_name, table_name, _, error in results:
                if error:
                    print(f"  {db_name}.{table_name}: {error}")

        return results


if __name__ == "__main__":
    data_pipeline = DataPipeline()
    sync_config = {
        "psdd_test_pg1_sys": [
            "t_sec_user",
            "t_org_org",
            "t_bd_supplier",
            "t_bd_customer",
        ],
        "psdd_test_pg1_scm": [
            "t_mscon_contract",
            "t_mscon_counterpart",
            "t_mscon_performplanin",
            "t_mscon_performplanout",
            "t_conl_case",
            "t_conl_disputeregist",
        ],
    }
    data_pipeline.sync(sync_config, "./data/local_data", max_workers=8)
