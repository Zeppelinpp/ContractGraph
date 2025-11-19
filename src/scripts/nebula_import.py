"""
Nebula Graph data importer.

将 graph_data 目录中的 CSV 节点/边数据导入到 Nebula Graph。
运行前确保：
  pip install nebula3-python
  graph_data 目录与本脚本位于同一层级

使用方式：
  # 使用默认 graph_data 目录
  python nebula_import.py
  
  # 使用指定数据目录
  python nebula_import.py --data-dir /path/to/your/graph_data
  
  # 使用 enhanced_graph_data
  python nebula_import.py --data-dir enhanced_graph_data
"""

import argparse
import csv
import os
import time
from pathlib import Path
from typing import Iterable, List, Dict

from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool
from src.settings import settings

# ============================================================================
# 基础配置
# ============================================================================
NEBULA_HOST = settings.nebula_config["host"]
NEBULA_PORT = settings.nebula_config["port"]
NEBULA_USERNAME = settings.nebula_config["user"]
NEBULA_PASSWORD = settings.nebula_config["password"]
NEBULA_SPACE = settings.nebula_config["space"]

# 默认数据目录
DEFAULT_GRAPH_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "graph_data")
GRAPH_DATA_DIR = DEFAULT_GRAPH_DATA_DIR

# 需要预先创建的边类型
EDGE_TYPES = [
    "LEGAL_PERSON",
    "CONTROLS",
    "PARTY_A",
    "PARTY_B",
    "PARTY_C",
    "PARTY_D",
    "TRADES_WITH",
    "INVOLVED_IN",
    "RELATED_TO",
    "IS_SUPPLIER",
    "IS_CUSTOMER",
    "PAYS",
    "RECEIVES",
]

TAG_TYPES = ["Person", "Company", "Contract", "LegalEvent", "Transaction"]


# ============================================================================
# 工具函数
# ============================================================================
def escape(value: str) -> str:
    """转义字符串，兼容 Nebula 的双引号表示。"""
    if value is None:
        return ""
    return str(value).replace("\\", "\\\\").replace('"', r"\"")


def parse_float(value: str) -> float:
    if value is None:
        return 0.0
    text = str(value).strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def read_csv_rows(filename: str) -> List[Dict[str, str]]:
    path = os.path.join(GRAPH_DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"  ! 文件不存在，跳过: {filename}")
        return []
    with open(path, "r", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def connect_nebula() -> ConnectionPool:
    config = Config()
    config.max_connection_pool_size = 10
    pool = ConnectionPool()
    ok = pool.init([(NEBULA_HOST, NEBULA_PORT)], config)
    if not ok:
        raise RuntimeError("无法连接到 Nebula Graph")
    return pool


def execute(session, query: str):
    result = session.execute(query)
    if not result.is_succeeded():
        raise RuntimeError(f"执行失败: {result.error_msg()}\nQuery: {query}")
    return result


def wait_for_schema_ready(session, retries: int = 10, interval: float = 1.0):
    """等待所有 Tag 和 Edge 元数据可用。"""
    execute(session, f"USE {NEBULA_SPACE};")

    def _wait(cmd: str, name: str):
        for _ in range(retries):
            result = session.execute(cmd)
            if result.is_succeeded():
                return
            time.sleep(interval)
        raise RuntimeError(f"{name} 元数据未就绪，请稍后重试。")

    for tag in TAG_TYPES:
        _wait(f"DESCRIBE TAG {tag};", f"Tag {tag}")
    for edge in EDGE_TYPES:
        _wait(f"DESCRIBE EDGE {edge};", f"Edge {edge}")


# ============================================================================
# Schema 创建
# ============================================================================
def create_space_and_schema(session):
    print("\n=== 创建图空间及 Schema ===")
    execute(
        session,
        f"CREATE SPACE IF NOT EXISTS {NEBULA_SPACE} "
        "(vid_type = FIXED_STRING(64));",
    )
    # 等待空间创建完成，新创建的空间需要更多时间
    print("  等待图空间创建完成...")
    time.sleep(3)
    execute(session, f"USE {NEBULA_SPACE};")

    execute(
        session,
        """
        CREATE TAG IF NOT EXISTS Person (
            name string,
            number string,
            id_card string,
            gender string,
            birthday string,
            status string
        );
        """,
    )

    execute(
        session,
        """
        CREATE TAG IF NOT EXISTS Company (
            name string,
            number string,
            legal_person string,
            credit_code string,
            establish_date string,
            status string,
            description string
        );
        """,
    )

    execute(
        session,
        """
        CREATE TAG IF NOT EXISTS Contract (
            contract_no string,
            contract_name string,
            amount double,
            sign_date string,
            status string,
            description string
        );
        """,
    )

    execute(
        session,
        """
        CREATE TAG IF NOT EXISTS LegalEvent (
            event_type string,
            event_no string,
            event_name string,
            amount double,
            status string,
            register_date string,
            description string
        );
        """,
    )

    execute(
        session,
        """
        CREATE TAG IF NOT EXISTS Transaction (
            transaction_type string,
            transaction_no string,
            contract_no string,
            amount double,
            transaction_date string,
            status string,
            description string
        );
        """,
    )

    for edge_type in EDGE_TYPES:
        execute(
            session,
            f"""
            CREATE EDGE IF NOT EXISTS {edge_type} (
                properties string
            );
            """,
        )

    wait_for_schema_ready(session)


# ============================================================================
# 索引创建
# ============================================================================
def create_tag_indexes(session):
    """为所有 Tag 的属性创建索引。"""
    print("\n=== 创建节点属性索引 ===")
    execute(session, f"USE {NEBULA_SPACE};")
    
    # Person Tag 索引
    person_indexes = [
        ("person_name", "Person", "name"),
        ("person_number", "Person", "number"),
        ("person_id_card", "Person", "id_card"),
        ("person_gender", "Person", "gender"),
        ("person_birthday", "Person", "birthday"),
        ("person_status", "Person", "status"),
    ]
    
    # Company Tag 索引
    company_indexes = [
        ("company_name", "Company", "name"),
        ("company_number", "Company", "number"),
        ("company_legal_person", "Company", "legal_person"),
        ("company_credit_code", "Company", "credit_code"),
        ("company_establish_date", "Company", "establish_date"),
        ("company_status", "Company", "status"),
        ("company_description", "Company", "description"),
    ]
    
    # Contract Tag 索引
    contract_indexes = [
        ("contract_contract_no", "Contract", "contract_no"),
        ("contract_contract_name", "Contract", "contract_name"),
        ("contract_amount", "Contract", "amount"),
        ("contract_sign_date", "Contract", "sign_date"),
        ("contract_status", "Contract", "status"),
        ("contract_description", "Contract", "description"),
    ]
    
    # LegalEvent Tag 索引
    legal_event_indexes = [
        ("legal_event_event_type", "LegalEvent", "event_type"),
        ("legal_event_event_no", "LegalEvent", "event_no"),
        ("legal_event_event_name", "LegalEvent", "event_name"),
        ("legal_event_amount", "LegalEvent", "amount"),
        ("legal_event_status", "LegalEvent", "status"),
        ("legal_event_register_date", "LegalEvent", "register_date"),
        ("legal_event_description", "LegalEvent", "description"),
    ]
    
    # Transaction Tag 索引
    transaction_indexes = [
        ("transaction_transaction_type", "Transaction", "transaction_type"),
        ("transaction_transaction_no", "Transaction", "transaction_no"),
        ("transaction_contract_no", "Transaction", "contract_no"),
        ("transaction_amount", "Transaction", "amount"),
        ("transaction_transaction_date", "Transaction", "transaction_date"),
        ("transaction_status", "Transaction", "status"),
        ("transaction_description", "Transaction", "description"),
    ]
    
    all_indexes = (
        person_indexes
        + company_indexes
        + contract_indexes
        + legal_event_indexes
        + transaction_indexes
    )
    
    # 字符串属性的索引长度（字节），考虑中文字符使用 128
    string_props = {
        "name", "number", "id_card", "gender", "birthday", "status",
        "legal_person", "credit_code", "establish_date", "description",
        "contract_no", "contract_name", "sign_date",
        "event_type", "event_no", "event_name", "register_date",
        "transaction_type", "transaction_no", "transaction_date",
    }
    
    # double 类型的属性不需要指定长度
    double_props = {"amount"}
    
    for index_name, tag_name, prop_name in all_indexes:
        if prop_name in string_props:
            query = (
                f"CREATE TAG INDEX IF NOT EXISTS {index_name} "
                f"ON {tag_name}({prop_name}(128));"
            )
        elif prop_name in double_props:
            query = (
                f"CREATE TAG INDEX IF NOT EXISTS {index_name} "
                f"ON {tag_name}({prop_name});"
            )
        else:
            query = (
                f"CREATE TAG INDEX IF NOT EXISTS {index_name} "
                f"ON {tag_name}({prop_name}(128));"
            )
        
        try:
            execute(session, query)
            print(f"  创建索引: {index_name}")
        except RuntimeError as e:
            print(f"  ! 创建索引失败 {index_name}: {e}")
    
    # 等待索引创建完成
    print("\n等待索引创建完成...")
    time.sleep(2)
    
    # 检查索引是否存在，等待所有索引创建完成
    def wait_for_indexes_ready(index_names, retries: int = 10, interval: float = 1.0):
        """等待所有索引创建完成。"""
        for _ in range(retries):
            try:
                result = execute(session, "SHOW TAG INDEXES;")
                existing_indexes = set()
                if result.row_size() > 0:
                    for i in range(result.row_size()):
                        row = result.row_values(i)
                        # 兼容不同版本的返回值：可能是list或Row对象
                        if isinstance(row, list):
                            if len(row) > 0:
                                existing_indexes.add(str(row[0]))
                        else:
                            if hasattr(row, 'values') and len(row.values) > 0:
                                existing_indexes.add(str(row.values[0]))
                
                missing = [name for name in index_names if name not in existing_indexes]
                if not missing:
                    return True
            except RuntimeError:
                pass
            time.sleep(interval)
        return False
    
    index_names = [name for name, _, _ in all_indexes]
    if wait_for_indexes_ready(index_names):
        print("  所有索引创建完成")
    else:
        print("  警告: 部分索引可能尚未创建完成")
    
    # 重建所有索引（仅重建已存在的索引）
    print("\n=== 重建索引 ===")
    for index_name, _, _ in all_indexes:
        try:
            execute(session, f"REBUILD TAG INDEX {index_name};")
            print(f"  重建索引: {index_name}")
        except RuntimeError as e:
            # 如果索引不存在，跳过重建（可能是新创建的索引，不需要重建）
            if "not found" not in str(e).lower():
                print(f"  ! 重建索引失败 {index_name}: {e}")
    
    # 等待索引重建完成
    print("\n等待索引重建完成...")
    time.sleep(3)
    
    # 检查索引状态
    try:
        result = execute(session, "SHOW TAG INDEX STATUS;")
        print("  索引状态:")
        if result.row_size() > 0:
            for i in range(result.row_size()):
                row = result.row_values(i)
                print(f"    {row}")
    except RuntimeError:
        pass


# ============================================================================
# 数据导入
# ============================================================================
def import_person_nodes(session):
    rows = read_csv_rows("nodes_person.csv")
    if not rows:
        return 0
    count = 0
    for row in rows:
        query = (
            "INSERT VERTEX Person(name, number, id_card, gender, birthday, status) "
            f"VALUES \"{escape(row.get('node_id'))}\": ("
            f"\"{escape(row.get('name'))}\", "
            f"\"{escape(row.get('number'))}\", "
            f"\"{escape(row.get('id_card'))}\", "
            f"\"{escape(row.get('gender'))}\", "
            f"\"{escape(row.get('birthday'))}\", "
            f"\"{escape(row.get('status'))}\");"
        )
        execute(session, query)
        count += 1
    print(f"  Person 节点导入完成: {count}")
    return count


def import_company_nodes(session):
    rows = read_csv_rows("nodes_company.csv")
    if not rows:
        return 0
    count = 0
    for row in rows:
        query = (
            "INSERT VERTEX Company("
            "name, number, legal_person, credit_code, "
            "establish_date, status, description) "
            f"VALUES \"{escape(row.get('node_id'))}\": ("
            f"\"{escape(row.get('name'))}\", "
            f"\"{escape(row.get('number'))}\", "
            f"\"{escape(row.get('legal_person'))}\", "
            f"\"{escape(row.get('credit_code'))}\", "
            f"\"{escape(row.get('establish_date'))}\", "
            f"\"{escape(row.get('status'))}\", "
            f"\"{escape(row.get('description'))}\");"
        )
        execute(session, query)
        count += 1
    print(f"  Company 节点导入完成: {count}")
    return count


def import_contract_nodes(session):
    rows = read_csv_rows("nodes_contract.csv")
    if not rows:
        return 0
    count = 0
    for row in rows:
        amount = parse_float(row.get("amount"))
        query = (
            "INSERT VERTEX Contract(contract_no, contract_name, amount, "
            "sign_date, status, description) "
            f"VALUES \"{escape(row.get('node_id'))}\": ("
            f"\"{escape(row.get('contract_no'))}\", "
            f"\"{escape(row.get('contract_name'))}\", "
            f"{amount}, "
            f"\"{escape(row.get('sign_date'))}\", "
            f"\"{escape(row.get('status'))}\", "
            f"\"{escape(row.get('description'))}\");"
        )
        execute(session, query)
        count += 1
    print(f"  Contract 节点导入完成: {count}")
    return count


def import_legal_event_nodes(session):
    rows = read_csv_rows("nodes_legal_event.csv")
    if not rows:
        return 0
    count = 0
    for row in rows:
        amount = parse_float(row.get("amount"))
        query = (
            "INSERT VERTEX LegalEvent("
            "event_type, event_no, event_name, amount, status, register_date, description) "
            f"VALUES \"{escape(row.get('node_id'))}\": ("
            f"\"{escape(row.get('event_type'))}\", "
            f"\"{escape(row.get('event_no'))}\", "
            f"\"{escape(row.get('event_name'))}\", "
            f"{amount}, "
            f"\"{escape(row.get('status'))}\", "
            f"\"{escape(row.get('register_date'))}\", "
            f"\"{escape(row.get('description'))}\");"
        )
        execute(session, query)
        count += 1
    print(f"  LegalEvent 节点导入完成: {count}")
    return count


def import_transaction_nodes(session):
    rows = read_csv_rows("nodes_transaction.csv")
    if not rows:
        return 0
    count = 0
    for row in rows:
        amount = parse_float(row.get("amount"))
        query = (
            "INSERT VERTEX Transaction("
            "transaction_type, transaction_no, contract_no, amount, "
            "transaction_date, status, description) "
            f"VALUES \"{escape(row.get('node_id'))}\": ("
            f"\"{escape(row.get('transaction_type'))}\", "
            f"\"{escape(row.get('transaction_no'))}\", "
            f"\"{escape(row.get('contract_no'))}\", "
            f"{amount}, "
            f"\"{escape(row.get('transaction_date'))}\", "
            f"\"{escape(row.get('status'))}\", "
            f"\"{escape(row.get('description'))}\");"
        )
        execute(session, query)
        count += 1
    print(f"  Transaction 节点导入完成: {count}")
    return count


def import_edges_from_file(session, filename: str, fixed_type: str = None):
    rows = read_csv_rows(filename)
    if not rows:
        return 0
    count = 0
    for row in rows:
        edge_type = fixed_type or row.get("edge_type")
        if not edge_type:
            continue
        query = (
            f"INSERT EDGE {edge_type}(properties) "
            f"VALUES \"{escape(row.get('from_node'))}\" -> "
            f"\"{escape(row.get('to_node'))}\": "
            f"(\"{escape(row.get('properties'))}\");"
        )
        execute(session, query)
        count += 1
    print(f"  {filename} 导入完成: {count}")
    return count


def import_nodes(session):
    print("\n=== 导入节点 ===")
    execute(session, f"USE {NEBULA_SPACE};")
    import_person_nodes(session)
    import_company_nodes(session)
    import_contract_nodes(session)
    import_legal_event_nodes(session)
    import_transaction_nodes(session)


def import_edges(session):
    print("\n=== 导入边 ===")
    execute(session, f"USE {NEBULA_SPACE};")
    import_edges_from_file(session, "edges_legal_person.csv", "LEGAL_PERSON")
    import_edges_from_file(session, "edges_controls.csv", "CONTROLS")
    import_edges_from_file(session, "edges_party.csv")
    import_edges_from_file(session, "edges_trades_with.csv", "TRADES_WITH")
    import_edges_from_file(session, "edges_case_person.csv", "INVOLVED_IN")
    import_edges_from_file(session, "edges_case_contract.csv", "RELATED_TO")
    import_edges_from_file(session, "edges_dispute_contract.csv", "RELATED_TO")
    import_edges_from_file(session, "edges_is_supplier.csv", "IS_SUPPLIER")
    import_edges_from_file(session, "edges_is_customer.csv", "IS_CUSTOMER")
    import_edges_from_file(session, "edges_company_transaction.csv")


# ============================================================================
# 主流程
# ============================================================================
def set_data_directory(data_dir: str):
    """Set the graph data directory."""
    global GRAPH_DATA_DIR
    
    # Handle relative paths
    if not os.path.isabs(data_dir):
        # Try relative to project root first
        base_dir = Path(__file__).resolve().parents[2]
        data_path = base_dir / "data" / data_dir
        if data_path.exists():
            GRAPH_DATA_DIR = str(data_path)
        else:
            # Try as direct relative path
            abs_path = os.path.abspath(data_dir)
            if os.path.exists(abs_path):
                GRAPH_DATA_DIR = abs_path
            else:
                raise FileNotFoundError(f"Data directory not found: {data_dir}")
    else:
        if os.path.exists(data_dir):
            GRAPH_DATA_DIR = data_dir
        else:
            raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    print(f"Using data directory: {GRAPH_DATA_DIR}")


def main():
    parser = argparse.ArgumentParser(
        description="Import graph data into Nebula Graph",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default graph_data directory
  python nebula_import.py
  
  # Use enhanced_graph_data directory
  python nebula_import.py --data-dir enhanced_graph_data
  
  # Use absolute path
  python nebula_import.py --data-dir /path/to/your/graph_data
        """
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Path to graph data directory (relative to project data/ or absolute path)"
    )
    
    args = parser.parse_args()
    
    # Set data directory if specified
    if args.data_dir:
        set_data_directory(args.data_dir)
    
    print("=" * 80)
    print("Nebula Graph 数据导入工具")
    print("=" * 80)
    print(f"地址: {NEBULA_HOST}:{NEBULA_PORT}")
    print(f"图空间: {NEBULA_SPACE}")
    print(f"数据目录: {GRAPH_DATA_DIR}")

    pool = connect_nebula()
    session = pool.get_session(NEBULA_USERNAME, NEBULA_PASSWORD)
    print("已连接到 Nebula Graph")

    try:
        create_space_and_schema(session)
        create_tag_indexes(session)
        import_nodes(session)
        import_edges(session)
        print("\n全部导入完成。")
    finally:
        session.release()
        pool.close()
        print("连接已关闭")


if __name__ == "__main__":
    main()
