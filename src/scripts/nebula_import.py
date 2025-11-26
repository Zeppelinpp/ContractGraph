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

NEBULA_HOST = settings.nebula_config["host"]
NEBULA_PORT = settings.nebula_config["port"]
NEBULA_USERNAME = settings.nebula_config["user"]
NEBULA_PASSWORD = settings.nebula_config["password"]
NEBULA_SPACE = settings.nebula_config["space"]

# 默认数据目录
DEFAULT_GRAPH_DATA_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "graph_data"
)
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
    "ADMIN_PENALTY_OF",
    "BUSINESS_ABNORMAL_OF",
    "EMPLOYED_BY",
]

TAG_TYPES = [
    "Person",
    "Company",
    "Contract",
    "LegalEvent",
    "Transaction",
    "AdminPenalty",
    "BusinessAbnormal",
]


# ============================================================================
# 工具函数
# ============================================================================
def escape(value: str) -> str:
    """转义字符串，兼容 Nebula 的双引号表示。"""
    if value is None:
        return ""
    s = str(value)
    # 转义反斜杠和双引号
    s = s.replace("\\", "\\\\").replace('"', r"\"")
    # 转义换行符和制表符
    s = s.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    return s


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


def execute(session, query: str, retry: int = 3, retry_delay: float = 0.5):
    """执行查询，带重试机制"""
    for attempt in range(retry):
        result = session.execute(query)
        if result.is_succeeded():
            return result

        # 安全地获取错误消息，处理编码问题
        error_msg = None
        try:
            error_msg = result.error_msg()
        except (UnicodeDecodeError, UnicodeError) as e:
            # 如果 UTF-8 解码失败，尝试直接访问原始字节
            try:
                raw_msg = result._resp.error_msg
                if raw_msg:
                    # 尝试用 latin-1 解码，然后转回 UTF-8
                    error_msg = raw_msg.decode("latin-1")
                else:
                    error_msg = f"无法解码错误消息（编码问题: {type(e).__name__}）"
            except Exception:
                # 尝试获取错误代码
                try:
                    error_code = result.error_code()
                    error_msg = f"错误代码: {error_code} (无法解码错误消息)"
                except:
                    error_msg = f"无法解码错误消息（编码问题: {type(e).__name__}）"

        # 如果是并发冲突错误，等待后重试
        if error_msg:
            try:
                error_msg_lower = error_msg.lower()
                if (
                    "more than one request" in error_msg_lower
                    or "concurrent" in error_msg_lower
                ):
                    if attempt < retry - 1:
                        time.sleep(retry_delay * (attempt + 1))
                        continue
            except (UnicodeDecodeError, UnicodeError, AttributeError):
                pass

        # 其他错误或重试次数用完，抛出异常
        if error_msg:
            raise RuntimeError(f"执行失败: {error_msg}\nQuery: {query[:200]}...")
        else:
            raise RuntimeError(f"执行失败（无法获取错误消息）\nQuery: {query[:200]}...")
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
        f"CREATE SPACE IF NOT EXISTS {NEBULA_SPACE} (vid_type = FIXED_STRING(64));",
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
            status string,
            email string,
            phone string
        );
        """,
    )

    # Update existing Person tag if it doesn't have new fields
    try:
        execute(session, "ALTER TAG Person ADD (email string);")
        print("  更新 Person tag: 添加 email 字段")
    except RuntimeError:
        pass  # Field may already exist

    try:
        execute(session, "ALTER TAG Person ADD (phone string);")
        print("  更新 Person tag: 添加 phone 字段")
    except RuntimeError:
        pass  # Field may already exist

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
            description string,
            fpaidamount double,
            ftotalamount double,
            fbiztimeend string,
            fperformstatus string
        );
        """,
    )

    # Update existing Transaction tag if it doesn't have new fields
    try:
        execute(session, "ALTER TAG Transaction ADD (fpaidamount double);")
        print("  更新 Transaction tag: 添加 fpaidamount 字段")
    except RuntimeError:
        pass

    try:
        execute(session, "ALTER TAG Transaction ADD (ftotalamount double);")
        print("  更新 Transaction tag: 添加 ftotalamount 字段")
    except RuntimeError:
        pass

    try:
        execute(session, "ALTER TAG Transaction ADD (fbiztimeend string);")
        print("  更新 Transaction tag: 添加 fbiztimeend 字段")
    except RuntimeError:
        pass

    try:
        execute(session, "ALTER TAG Transaction ADD (fperformstatus string);")
        print("  更新 Transaction tag: 添加 fperformstatus 字段")
    except RuntimeError:
        pass

    execute(
        session,
        """
        CREATE TAG IF NOT EXISTS AdminPenalty (
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
        CREATE TAG IF NOT EXISTS BusinessAbnormal (
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

    # Create edges with unified schema (all edges use properties string)
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
        ("person_email", "Person", "email"),
        ("person_phone", "Person", "phone"),
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
        ("transaction_fpaidamount", "Transaction", "fpaidamount"),
        ("transaction_ftotalamount", "Transaction", "ftotalamount"),
        ("transaction_fbiztimeend", "Transaction", "fbiztimeend"),
        ("transaction_fperformstatus", "Transaction", "fperformstatus"),
    ]

    # AdminPenalty Tag 索引
    admin_penalty_indexes = [
        ("admin_penalty_event_type", "AdminPenalty", "event_type"),
        ("admin_penalty_event_no", "AdminPenalty", "event_no"),
        ("admin_penalty_event_name", "AdminPenalty", "event_name"),
        ("admin_penalty_amount", "AdminPenalty", "amount"),
        ("admin_penalty_status", "AdminPenalty", "status"),
        ("admin_penalty_register_date", "AdminPenalty", "register_date"),
        ("admin_penalty_description", "AdminPenalty", "description"),
    ]

    # BusinessAbnormal Tag 索引
    business_abnormal_indexes = [
        ("business_abnormal_event_type", "BusinessAbnormal", "event_type"),
        ("business_abnormal_event_no", "BusinessAbnormal", "event_no"),
        ("business_abnormal_event_name", "BusinessAbnormal", "event_name"),
        ("business_abnormal_amount", "BusinessAbnormal", "amount"),
        ("business_abnormal_status", "BusinessAbnormal", "status"),
        ("business_abnormal_register_date", "BusinessAbnormal", "register_date"),
        ("business_abnormal_description", "BusinessAbnormal", "description"),
    ]

    all_indexes = (
        person_indexes
        + company_indexes
        + contract_indexes
        + legal_event_indexes
        + transaction_indexes
        + admin_penalty_indexes
        + business_abnormal_indexes
    )

    # 字符串属性的索引长度（字节），考虑中文字符使用 128
    string_props = {
        "name",
        "number",
        "id_card",
        "gender",
        "birthday",
        "status",
        "email",
        "phone",
        "legal_person",
        "credit_code",
        "establish_date",
        "description",
        "contract_no",
        "contract_name",
        "sign_date",
        "event_type",
        "event_no",
        "event_name",
        "register_date",
        "transaction_type",
        "transaction_no",
        "transaction_date",
        "fbiztimeend",
        "fperformstatus",
    }

    # double 类型的属性不需要指定长度
    double_props = {"amount", "fpaidamount", "ftotalamount"}

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
                            if hasattr(row, "values") and len(row.values) > 0:
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
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values = []
        for row in batch:
            node_id = escape(row.get("node_id"))
            value = (
                f'"{node_id}": ('
                f'"{escape(row.get("name"))}", '
                f'"{escape(row.get("number"))}", '
                f'"{escape(row.get("id_card"))}", '
                f'"{escape(row.get("gender"))}", '
                f'"{escape(row.get("birthday"))}", '
                f'"{escape(row.get("status"))}", '
                f'"{escape(row.get("email", ""))}", '
                f'"{escape(row.get("phone", ""))}")'
            )
            values.append(value)

        query = (
            "INSERT VERTEX Person(name, number, id_card, gender, birthday, status, email, phone) "
            f"VALUES {', '.join(values)};"
        )
        try:
            execute(session, query)
            count += len(batch)
        except RuntimeError as e:
            # 如果批量插入失败，尝试逐条插入
            error_msg = str(e)
            if "More than one request" in error_msg:
                print(f"  批量插入遇到并发冲突，改为逐条插入...")
                for row in batch:
                    node_id = escape(row.get("node_id"))
                    single_query = (
                        "INSERT VERTEX Person(name, number, id_card, gender, birthday, status, email, phone) "
                        f'VALUES "{node_id}": ('
                        f'"{escape(row.get("name"))}", '
                        f'"{escape(row.get("number"))}", '
                        f'"{escape(row.get("id_card"))}", '
                        f'"{escape(row.get("gender"))}", '
                        f'"{escape(row.get("birthday"))}", '
                        f'"{escape(row.get("status"))}", '
                        f'"{escape(row.get("email", ""))}", '
                        f'"{escape(row.get("phone", ""))}");'
                    )
                    try:
                        execute(session, single_query)
                        count += 1
                    except RuntimeError as e2:
                        # 如果是重复顶点错误，跳过
                        if (
                            "existed" in str(e2).lower()
                            or "duplicate" in str(e2).lower()
                        ):
                            continue
                        print(f"  ! 跳过节点 {node_id}: {e2}")
            else:
                raise
    print(f"  Person 节点导入完成: {count}")
    return count


def import_company_nodes(session):
    rows = read_csv_rows("nodes_company.csv")
    if not rows:
        return 0
    count = 0
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values = []
        for row in batch:
            node_id = escape(row.get("node_id"))
            value = (
                f'"{node_id}": ('
                f'"{escape(row.get("name"))}", '
                f'"{escape(row.get("number"))}", '
                f'"{escape(row.get("legal_person"))}", '
                f'"{escape(row.get("credit_code"))}", '
                f'"{escape(row.get("establish_date"))}", '
                f'"{escape(row.get("status"))}", '
                f'"{escape(row.get("description"))}")'
            )
            values.append(value)

        query = (
            "INSERT VERTEX Company("
            "name, number, legal_person, credit_code, "
            "establish_date, status, description) "
            f"VALUES {', '.join(values)};"
        )
        try:
            execute(session, query)
            count += len(batch)
        except RuntimeError as e:
            error_msg = str(e)
            if "More than one request" in error_msg:
                print(f"  批量插入遇到并发冲突，改为逐条插入...")
                for row in batch:
                    node_id = escape(row.get("node_id"))
                    single_query = (
                        "INSERT VERTEX Company("
                        "name, number, legal_person, credit_code, "
                        "establish_date, status, description) "
                        f'VALUES "{node_id}": ('
                        f'"{escape(row.get("name"))}", '
                        f'"{escape(row.get("number"))}", '
                        f'"{escape(row.get("legal_person"))}", '
                        f'"{escape(row.get("credit_code"))}", '
                        f'"{escape(row.get("establish_date"))}", '
                        f'"{escape(row.get("status"))}", '
                        f'"{escape(row.get("description"))}");'
                    )
                    try:
                        execute(session, single_query)
                        count += 1
                    except RuntimeError as e2:
                        if (
                            "existed" in str(e2).lower()
                            or "duplicate" in str(e2).lower()
                        ):
                            continue
                        print(f"  ! 跳过节点 {node_id}: {e2}")
            else:
                raise
    print(f"  Company 节点导入完成: {count}")
    return count


def import_contract_nodes(session):
    rows = read_csv_rows("nodes_contract.csv")
    if not rows:
        return 0
    count = 0
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values = []
        for row in batch:
            node_id = escape(row.get("node_id"))
            amount = parse_float(row.get("amount"))
            value = (
                f'"{node_id}": ('
                f'"{escape(row.get("contract_no"))}", '
                f'"{escape(row.get("contract_name"))}", '
                f"{amount}, "
                f'"{escape(row.get("sign_date"))}", '
                f'"{escape(row.get("status"))}", '
                f'"{escape(row.get("description"))}")'
            )
            values.append(value)

        query = (
            "INSERT VERTEX Contract(contract_no, contract_name, amount, "
            "sign_date, status, description) "
            f"VALUES {', '.join(values)};"
        )
        try:
            execute(session, query)
            count += len(batch)
        except RuntimeError as e:
            error_msg = str(e)
            if "More than one request" in error_msg:
                print(f"  批量插入遇到并发冲突，改为逐条插入...")
                for row in batch:
                    node_id = escape(row.get("node_id"))
                    amount = parse_float(row.get("amount"))
                    single_query = (
                        "INSERT VERTEX Contract(contract_no, contract_name, amount, "
                        "sign_date, status, description) "
                        f'VALUES "{node_id}": ('
                        f'"{escape(row.get("contract_no"))}", '
                        f'"{escape(row.get("contract_name"))}", '
                        f"{amount}, "
                        f'"{escape(row.get("sign_date"))}", '
                        f'"{escape(row.get("status"))}", '
                        f'"{escape(row.get("description"))}");'
                    )
                    try:
                        execute(session, single_query)
                        count += 1
                    except RuntimeError as e2:
                        if (
                            "existed" in str(e2).lower()
                            or "duplicate" in str(e2).lower()
                        ):
                            continue
                        print(f"  ! 跳过节点 {node_id}: {e2}")
            else:
                raise
    print(f"  Contract 节点导入完成: {count}")
    return count


def import_legal_event_nodes(session):
    rows = read_csv_rows("nodes_legal_event.csv")
    if not rows:
        return 0
    count = 0
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values = []
        for row in batch:
            node_id = escape(row.get("node_id"))
            amount = parse_float(row.get("amount"))
            value = (
                f'"{node_id}": ('
                f'"{escape(row.get("event_type"))}", '
                f'"{escape(row.get("event_no"))}", '
                f'"{escape(row.get("event_name"))}", '
                f"{amount}, "
                f'"{escape(row.get("status"))}", '
                f'"{escape(row.get("register_date"))}", '
                f'"{escape(row.get("description"))}")'
            )
            values.append(value)

        query = (
            "INSERT VERTEX LegalEvent("
            "event_type, event_no, event_name, amount, status, register_date, description) "
            f"VALUES {', '.join(values)};"
        )
        try:
            execute(session, query)
            count += len(batch)
        except RuntimeError as e:
            error_msg = str(e)
            if "More than one request" in error_msg:
                print(f"  批量插入遇到并发冲突，改为逐条插入...")
                for row in batch:
                    node_id = escape(row.get("node_id"))
                    amount = parse_float(row.get("amount"))
                    single_query = (
                        "INSERT VERTEX LegalEvent("
                        "event_type, event_no, event_name, amount, status, register_date, description) "
                        f'VALUES "{node_id}": ('
                        f'"{escape(row.get("event_type"))}", '
                        f'"{escape(row.get("event_no"))}", '
                        f'"{escape(row.get("event_name"))}", '
                        f"{amount}, "
                        f'"{escape(row.get("status"))}", '
                        f'"{escape(row.get("register_date"))}", '
                        f'"{escape(row.get("description"))}");'
                    )
                    try:
                        execute(session, single_query)
                        count += 1
                    except RuntimeError as e2:
                        if (
                            "existed" in str(e2).lower()
                            or "duplicate" in str(e2).lower()
                        ):
                            continue
                        print(f"  ! 跳过节点 {node_id}: {e2}")
            else:
                raise
    print(f"  LegalEvent 节点导入完成: {count}")
    return count


def import_transaction_nodes(session):
    rows = read_csv_rows("nodes_transaction.csv")
    if not rows:
        return 0
    count = 0
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values = []
        for row in batch:
            node_id = escape(row.get("node_id"))
            amount = parse_float(row.get("amount"))
            fpaidamount = parse_float(
                row.get("fpaidamount") or row.get("fpaidallamount") or "0"
            )
            ftotalamount = parse_float(row.get("ftotalamount", "0"))
            value = (
                f'"{node_id}": ('
                f'"{escape(row.get("transaction_type"))}", '
                f'"{escape(row.get("transaction_no"))}", '
                f'"{escape(row.get("contract_no"))}", '
                f"{amount}, "
                f'"{escape(row.get("transaction_date"))}", '
                f'"{escape(row.get("status"))}", '
                f'"{escape(row.get("description"))}", '
                f"{fpaidamount}, "
                f"{ftotalamount}, "
                f'"{escape(row.get("fbiztimeend", ""))}", '
                f'"{escape(row.get("fperformstatus", ""))}")'
            )
            values.append(value)

        query = (
            "INSERT VERTEX Transaction("
            "transaction_type, transaction_no, contract_no, amount, "
            "transaction_date, status, description, "
            "fpaidamount, ftotalamount, fbiztimeend, fperformstatus) "
            f"VALUES {', '.join(values)};"
        )
        try:
            execute(session, query)
            count += len(batch)
        except RuntimeError as e:
            error_msg = str(e)
            if "More than one request" in error_msg:
                print(f"  批量插入遇到并发冲突，改为逐条插入...")
                for row in batch:
                    node_id = escape(row.get("node_id"))
                    amount = parse_float(row.get("amount"))
                    fpaidamount = parse_float(
                        row.get("fpaidamount") or row.get("fpaidallamount") or "0"
                    )
                    ftotalamount = parse_float(row.get("ftotalamount", "0"))
                    single_query = (
                        "INSERT VERTEX Transaction("
                        "transaction_type, transaction_no, contract_no, amount, "
                        "transaction_date, status, description, "
                        "fpaidamount, ftotalamount, fbiztimeend, fperformstatus) "
                        f'VALUES "{node_id}": ('
                        f'"{escape(row.get("transaction_type"))}", '
                        f'"{escape(row.get("transaction_no"))}", '
                        f'"{escape(row.get("contract_no"))}", '
                        f"{amount}, "
                        f'"{escape(row.get("transaction_date"))}", '
                        f'"{escape(row.get("status"))}", '
                        f'"{escape(row.get("description"))}", '
                        f"{fpaidamount}, "
                        f"{ftotalamount}, "
                        f'"{escape(row.get("fbiztimeend", ""))}", '
                        f'"{escape(row.get("fperformstatus", ""))}");'
                    )
                    try:
                        execute(session, single_query)
                        count += 1
                    except RuntimeError as e2:
                        if (
                            "existed" in str(e2).lower()
                            or "duplicate" in str(e2).lower()
                        ):
                            continue
                        print(f"  ! 跳过节点 {node_id}: {e2}")
            else:
                raise
    print(f"  Transaction 节点导入完成: {count}")
    return count


def import_admin_penalty_nodes(session):
    rows = read_csv_rows("nodes_admin_penalty.csv")
    if not rows:
        return 0
    count = 0
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values = []
        for row in batch:
            node_id = escape(row.get("node_id"))
            amount = parse_float(row.get("amount"))
            value = (
                f'"{node_id}": ('
                f'"{escape(row.get("event_type"))}", '
                f'"{escape(row.get("event_no"))}", '
                f'"{escape(row.get("event_name"))}", '
                f"{amount}, "
                f'"{escape(row.get("status"))}", '
                f'"{escape(row.get("register_date"))}", '
                f'"{escape(row.get("description"))}")'
            )
            values.append(value)

        query = (
            "INSERT VERTEX AdminPenalty("
            "event_type, event_no, event_name, amount, status, register_date, description) "
            f"VALUES {', '.join(values)};"
        )
        try:
            execute(session, query)
            count += len(batch)
        except RuntimeError as e:
            error_msg = str(e)
            if "More than one request" in error_msg:
                print(f"  批量插入遇到并发冲突，改为逐条插入...")
                for row in batch:
                    node_id = escape(row.get("node_id"))
                    amount = parse_float(row.get("amount"))
                    single_query = (
                        "INSERT VERTEX AdminPenalty("
                        "event_type, event_no, event_name, amount, status, register_date, description) "
                        f'VALUES "{node_id}": ('
                        f'"{escape(row.get("event_type"))}", '
                        f'"{escape(row.get("event_no"))}", '
                        f'"{escape(row.get("event_name"))}", '
                        f"{amount}, "
                        f'"{escape(row.get("status"))}", '
                        f'"{escape(row.get("register_date"))}", '
                        f'"{escape(row.get("description"))}");'
                    )
                    try:
                        execute(session, single_query)
                        count += 1
                    except RuntimeError as e2:
                        if (
                            "existed" in str(e2).lower()
                            or "duplicate" in str(e2).lower()
                        ):
                            continue
                        print(f"  ! 跳过节点 {node_id}: {e2}")
            else:
                raise
    print(f"  AdminPenalty 节点导入完成: {count}")
    return count


def import_business_abnormal_nodes(session):
    rows = read_csv_rows("nodes_business_abnormal.csv")
    if not rows:
        return 0
    count = 0
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values = []
        for row in batch:
            node_id = escape(row.get("node_id"))
            amount = parse_float(row.get("amount"))
            value = (
                f'"{node_id}": ('
                f'"{escape(row.get("event_type"))}", '
                f'"{escape(row.get("event_no"))}", '
                f'"{escape(row.get("event_name"))}", '
                f"{amount}, "
                f'"{escape(row.get("status"))}", '
                f'"{escape(row.get("register_date"))}", '
                f'"{escape(row.get("description"))}")'
            )
            values.append(value)

        query = (
            "INSERT VERTEX BusinessAbnormal("
            "event_type, event_no, event_name, amount, status, register_date, description) "
            f"VALUES {', '.join(values)};"
        )
        try:
            execute(session, query)
            count += len(batch)
        except RuntimeError as e:
            error_msg = str(e)
            if "More than one request" in error_msg:
                print(f"  批量插入遇到并发冲突，改为逐条插入...")
                for row in batch:
                    node_id = escape(row.get("node_id"))
                    amount = parse_float(row.get("amount"))
                    single_query = (
                        "INSERT VERTEX BusinessAbnormal("
                        "event_type, event_no, event_name, amount, status, register_date, description) "
                        f'VALUES "{node_id}": ('
                        f'"{escape(row.get("event_type"))}", '
                        f'"{escape(row.get("event_no"))}", '
                        f'"{escape(row.get("event_name"))}", '
                        f"{amount}, "
                        f'"{escape(row.get("status"))}", '
                        f'"{escape(row.get("register_date"))}", '
                        f'"{escape(row.get("description"))}");'
                    )
                    try:
                        execute(session, single_query)
                        count += 1
                    except RuntimeError as e2:
                        if (
                            "existed" in str(e2).lower()
                            or "duplicate" in str(e2).lower()
                        ):
                            continue
                        print(f"  ! 跳过节点 {node_id}: {e2}")
            else:
                raise
    print(f"  BusinessAbnormal 节点导入完成: {count}")
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
            f'VALUES "{escape(row.get("from_node"))}" -> '
            f'"{escape(row.get("to_node"))}": '
            f'("{escape(row.get("properties"))}");'
        )
        execute(session, query)
        count += 1
    print(f"  {filename} 导入完成: {count}")
    return count


def import_employment_edges(session):
    """Import employment edges, converting position and tenure_start to properties string."""
    rows = read_csv_rows("edges_employment.csv")
    if not rows:
        return 0
    count = 0
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        values = []
        for row in batch:
            from_node = escape(row.get("from_node"))
            to_node = escape(row.get("to_node"))
            position = escape(row.get("position", ""))
            tenure_start = escape(row.get("tenure_start", ""))
            # Build properties string from position and tenure_start
            properties = f"position={position}; tenure_start={tenure_start}"
            value = f'"{from_node}" -> "{to_node}": ("{escape(properties)}")'
            values.append(value)

        query = f"INSERT EDGE EMPLOYED_BY(properties) VALUES {', '.join(values)};"
        try:
            execute(session, query)
            count += len(batch)
        except RuntimeError as e:
            error_msg = str(e)
            if "More than one request" in error_msg:
                print(f"  批量插入遇到并发冲突，改为逐条插入...")
                for row in batch:
                    from_node = escape(row.get("from_node"))
                    to_node = escape(row.get("to_node"))
                    position = escape(row.get("position", ""))
                    tenure_start = escape(row.get("tenure_start", ""))
                    # Build properties string from position and tenure_start
                    properties = f"position={position}; tenure_start={tenure_start}"
                    single_query = (
                        "INSERT EDGE EMPLOYED_BY(properties) "
                        f'VALUES "{from_node}" -> "{to_node}": '
                        f'("{escape(properties)}");'
                    )
                    try:
                        execute(session, single_query)
                        count += 1
                    except RuntimeError as e2:
                        if (
                            "existed" in str(e2).lower()
                            or "duplicate" in str(e2).lower()
                        ):
                            continue
                        print(f"  ! 跳过边 {from_node} -> {to_node}: {e2}")
            else:
                raise
    print(f"  edges_employment.csv 导入完成: {count}")
    return count


def import_nodes(session):
    print("\n=== 导入节点 ===")
    execute(session, f"USE {NEBULA_SPACE};")
    import_person_nodes(session)
    import_company_nodes(session)
    import_contract_nodes(session)
    import_legal_event_nodes(session)
    import_transaction_nodes(session)
    import_admin_penalty_nodes(session)
    import_business_abnormal_nodes(session)


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
    import_edges_from_file(
        session, "edges_admin_penalty_company.csv", "ADMIN_PENALTY_OF"
    )
    import_edges_from_file(
        session, "edges_business_abnormal_company.csv", "BUSINESS_ABNORMAL_OF"
    )
    import_employment_edges(session)


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
        """,
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Path to graph data directory (relative to project data/ or absolute path)",
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
