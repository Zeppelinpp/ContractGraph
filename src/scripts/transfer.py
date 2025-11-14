import psycopg2
import subprocess
import os
from multiprocessing import Pool
from tqdm import tqdm
from src.settings import settings


def _copy_table_worker(args):
    db_name, table_name, source_config, target_config = args
    try:
        transfer = DataTransfer(source_config, target_config)
        transfer.copy_table(db_name, table_name)
        return (db_name, table_name, True, None)
    except Exception as e:
        return (db_name, table_name, False, str(e))


class DataTransfer:
    def __init__(self, source_config: dict, target_config: dict):
        self.source_config = source_config
        self.target_config = target_config

    def get_table_ddl(self, db_name: str, table_name: str):
        cmd = [
            "pg_dump",
            "-h",
            self.source_config["host"],
            "-p",
            self.source_config["port"],
            "-U",
            self.source_config["user"],
            "-d",
            db_name,
            "-t",
            table_name,
            "-s",
            "--no-owner",
            "--no-privileges",
        ]
        ddl = subprocess.check_output(
            cmd,
            text=True,
            env={**os.environ, "PGPASSWORD": self.source_config["password"]},
        )

        # Filter out psql meta-commands (lines starting with \) and SET statements
        lines = []
        for line in ddl.split("\n"):
            stripped = line.strip()
            # Skip psql meta-commands (lines starting with \)
            if stripped.startswith("\\"):
                continue
            # Skip SET statements that are not SQL DDL
            if (
                stripped.startswith("SET ")
                and "=" in stripped
                and not stripped.startswith("SET search_path")
            ):
                continue
            lines.append(line)

        return "\n".join(lines)

    def copy_table(self, db_name: str, table_name: str):
        source_ddl = self.get_table_ddl(db_name, table_name)

        # Create table in target database
        target_conn = psycopg2.connect(
            host=self.target_config["host"],
            port=self.target_config["port"],
            user=self.target_config["user"],
            password=self.target_config["password"],
            dbname="postgres",
        )
        # 可先 drop
        target_cur = target_conn.cursor()
        target_cur.execute(f"DROP TABLE IF EXISTS public.{table_name} CASCADE;")
        target_cur.execute(source_ddl)
        target_conn.commit()
        target_cur.close()
        target_conn.close()

    def sync(self, sync_config: dict, max_workers: int = None):
        # Calculate total tasks
        tasks = []
        for db_name, tables in sync_config.items():
            if isinstance(tables, list):
                for table_name in tables:
                    tasks.append(
                        (db_name, table_name, self.source_config, self.target_config)
                    )
            elif isinstance(tables, str):
                tasks.append((db_name, tables, self.source_config, self.target_config))

        total_tasks = len(tasks)
        if total_tasks == 0:
            return

        # Use multiprocessing with tqdm
        with Pool(processes=max_workers) as pool:
            results = []
            with tqdm(total=total_tasks, desc="Syncing tables") as pbar:
                for result in pool.imap(_copy_table_worker, tasks):
                    results.append(result)
                    db_name, table_name, success, error = result
                    if success:
                        pbar.set_postfix_str(f"✓ {db_name}.{table_name}")
                    else:
                        pbar.set_postfix_str(f"✗ {db_name}.{table_name}: {error}")
                    pbar.update(1)

        # Print summary
        success_count = sum(1 for _, _, success, _ in results if success)
        failed_count = total_tasks - success_count
        print(
            f"\nCompleted: {success_count}/{total_tasks} succeeded, {failed_count} failed"
        )

        if failed_count > 0:
            print("\nFailed tasks:")
            for db_name, table_name, success, error in results:
                if not success:
                    print(f"  {db_name}.{table_name}: {error}")


if __name__ == "__main__":
    transfer = DataTransfer(settings.pg_config["remote"], settings.pg_config["local"])
    sync_config = {
        "psdd_test_pg1_sys": [
            "t_sec_user",
            "t_org_org",
            "t_bd_supplier",
            "t_bd_customer",
        ],
        "psdd_test_pg1_scm": [
            "t_mscon_contract",
            "t_conl_disputeregist",
            "t_conl_otherparty",
            "t_conl_ourparty",
            "t_conl_case",
            "t_conl_processdefendant",
            "t_conl_processplaintiff",
            "t_conl_party",
            "t_mscon_counterpart",
            "t_mscon_type",
        ],
    }
    transfer.sync(sync_config, max_workers=10)
