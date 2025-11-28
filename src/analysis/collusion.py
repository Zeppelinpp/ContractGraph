"""
场景四：关联方串通网络分析

检测关联方串通网络，包括轮流中标、围标等模式
"""

import os
import pandas as pd
import json
from collections import defaultdict, Counter
from typing import List, Optional, Dict
from src.utils.nebula_utils import get_nebula_session, execute_query
from src.config.models import CollusionConfig

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

# Default configuration
DEFAULT_CONFIG = CollusionConfig()


def calculate_rotation_score(win_sequence):
    """
    计算轮换分数：检测是否存在规律的轮流中标
    完美轮换 = 1.0，完全随机 = 0.0
    """
    if len(win_sequence) < 3:
        return 0.0

    counter = Counter(win_sequence)
    counts = list(counter.values())
    if len(counts) < 2:
        return 0.0

    mean_count = sum(counts) / len(counts)
    variance = sum((c - mean_count) ** 2 for c in counts) / len(counts)

    # 归一化：方差为 0 时分数为 1
    max_variance = mean_count**2
    rotation_score = 1 - min(variance / max_variance, 1.0) if max_variance > 0 else 0

    return rotation_score


def is_near_threshold(amount, thresholds, margin=0.05):
    """
    检测金额是否刻意卡在审批阈值附近
    """
    for threshold in thresholds:
        lower = threshold * (1 - margin)
        upper = threshold
        if lower <= amount <= upper:
            return True
    return False


def get_contract_info(session, contract_id: str) -> Dict:
    """获取合同信息"""
    query = f"""
    MATCH (con:Contract)
    WHERE id(con) == "{contract_id}"
    RETURN id(con) as contract_id,
           con.Contract.contract_no as contract_no,
           con.Contract.contract_name as contract_name,
           con.Contract.amount as amount,
           con.Contract.sign_date as sign_date
    """
    rows = execute_query(session, query)
    if rows:
        row = rows[0]
        return {
            "contract_id": row.get("contract_id", contract_id),
            "contract_no": row.get("contract_no", ""),
            "contract_name": row.get("contract_name", contract_id),
            "amount": row.get("amount", 0),
            "sign_date": row.get("sign_date", ""),
        }
    return {"contract_id": contract_id, "contract_name": contract_id}


def get_contract_party_relations(session, contract_id: str) -> List[Dict]:
    """获取合同与公司的甲/乙方关系"""
    query = f"""
    MATCH (c:Company)-[e:PARTY_A|PARTY_B]->(con:Contract)
    WHERE id(con) == "{contract_id}"
    RETURN id(c) as company_id, c.Company.name as company_name, type(e) as party_type
    """
    rows = execute_query(session, query)
    return [
        {
            "company_id": row.get("company_id", ""),
            "company_name": row.get("company_name", ""),
            "party_type": row.get("party_type", ""),
        }
        for row in rows
        if row.get("company_id")
    ]


def get_contract_parties(session, contract_id: str) -> List[str]:
    """
    获取合同的甲方和乙方公司ID
    """
    query = f"""
    MATCH (c:Company)-[:PARTY_A|PARTY_B]->(con:Contract)
    WHERE id(con) == "{contract_id}"
    RETURN DISTINCT id(c) as company_id
    """
    rows = execute_query(session, query)
    return [row.get("company_id", "") for row in rows if row.get("company_id")]


def get_contracts_from_companies(session, company_ids: List[str]) -> List[str]:
    """
    从公司ID列表获取关联的合同ID
    """
    if not company_ids:
        return []
    ids_str = ", ".join([f'"{cid}"' for cid in company_ids])
    query = f"""
    MATCH (c:Company)-[:PARTY_A|PARTY_B]->(con:Contract)
    WHERE id(c) IN [{ids_str}]
    RETURN DISTINCT id(con) as contract_id
    """
    rows = execute_query(session, query)
    return [row.get("contract_id", "") for row in rows if row.get("contract_id")]


def analyze_collusion_patterns(
    company_cluster,
    session,
    periods: Optional[List[str]] = None,
    config: CollusionConfig = DEFAULT_CONFIG,
):
    """
    分析公司集群的串通模式
    
    Args:
        company_cluster: 公司集群列表
        session: Nebula session
        periods: 时间段列表（单值或[start, end]范围）
        config: 串通分析配置
    """
    # Build time filter
    periods_filter = ""
    if periods:
        if len(periods) == 1:
            periods_filter = f"AND con.Contract.sign_date == '{periods[0]}'"
        elif len(periods) == 2:
            periods_filter = f"AND con.Contract.sign_date >= '{periods[0]}' AND con.Contract.sign_date <= '{periods[1]}'"
    
    company_ids_str = ", ".join([f'"{c}"' for c in company_cluster])
    contract_query = f"""
    MATCH (c:Company)-[:PARTY_B]->(con:Contract)
    WHERE id(c) IN [{company_ids_str}] {periods_filter}
    RETURN id(c) as company_id, id(con) as contract_id,
           con.Contract.sign_date as sign_date,
           con.Contract.amount as amount
    ORDER BY sign_date
    """
    rows = execute_query(session, contract_query)

    if len(rows) == 0:
        return {"risk_score": 0.0, "contract_ids": []}

    # 转换为 DataFrame
    contracts_data = []
    for row in rows:
        contracts_data.append(
            {
                "company_id": row.get("company_id", ""),
                "contract_id": row.get("contract_id", ""),
                "sign_date": row.get("sign_date", ""),
                "amount": float(row.get("amount", 0) or 0),
            }
        )

    cluster_contracts = pd.DataFrame(contracts_data)
    cluster_contracts["sign_date"] = pd.to_datetime(cluster_contracts["sign_date"])
    cluster_contracts = cluster_contracts.sort_values("sign_date")

    # Collect contract IDs
    contract_ids = cluster_contracts["contract_id"].unique().tolist()

    # 计算中标轮换度
    win_companies = cluster_contracts["company_id"].tolist()
    rotation_score = calculate_rotation_score(win_companies)

    # 特征 2: 合同金额相似度
    amounts = cluster_contracts["amount"].dropna()
    if len(amounts) >= 2:
        amount_std = amounts.std()
        amount_mean = amounts.mean()
        amount_cv = amount_std / amount_mean if amount_mean > 0 else 0
        amount_similarity = 1 - min(amount_cv, 1.0)
    else:
        amount_similarity = 0

    # 特征 3: 合同金额卡阈值检测
    threshold_count = sum(
        1 for amt in amounts 
        if is_near_threshold(amt, config.approval_thresholds, config.threshold_margin)
    )
    threshold_ratio = threshold_count / len(amounts) if len(amounts) > 0 else 0

    # 特征 4: 网络密度（关联关系的紧密程度）
    relation_query = f"""
    MATCH (c1:Company)-[e:LEGAL_PERSON|CONTROLS]-(c2:Company)
    WHERE id(c1) IN [{company_ids_str}] AND id(c2) IN [{company_ids_str}]
    RETURN count(e) as relation_count
    """
    relation_rows = execute_query(session, relation_query)
    internal_relations = (
        relation_rows[0].get("relation_count", 0) if relation_rows else 0
    )

    max_possible_relations = len(company_cluster) * (len(company_cluster) - 1) / 2
    density = (
        internal_relations / max_possible_relations if max_possible_relations > 0 else 0
    )

    # 特征 5: 关联类型强度
    has_strong_relation = len(company_cluster) >= 2

    # 综合风险分数（使用配置的权重）
    weights = config.feature_weights
    risk_score = (
        rotation_score * weights.get("rotation", 0.3)
        + amount_similarity * weights.get("amount_similarity", 0.2)
        + threshold_ratio * weights.get("threshold_ratio", 0.2)
        + density * weights.get("network_density", 0.2)
        + (weights.get("strong_relation", 0.1) if has_strong_relation else 0)
    )

    return {
        "risk_score": risk_score,
        "rotation_score": rotation_score,
        "amount_similarity": amount_similarity,
        "threshold_ratio": threshold_ratio,
        "network_density": density,
        "contract_count": len(cluster_contracts),
        "total_amount": amounts.sum(),
        "avg_amount": amounts.mean(),
        "contract_ids": contract_ids,
    }


def detect_collusion_network(
    session,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
    config: CollusionConfig = DEFAULT_CONFIG,
):
    """
    检测关联方串通网络

    Args:
        session: Nebula session
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）
        config: 串通分析配置

    Returns:
        list: 可疑串通网络列表
    """
    # Build company filter
    company_filter = ""
    if company_ids:
        ids_str = ", ".join([f"'{cid}'" for cid in company_ids])
        company_filter = f"WHERE c.Company.number IN [{ids_str}]"
    
    company_query = f"""
    MATCH (c:Company)
    {company_filter}
    RETURN id(c) as company_id
    """
    companies = execute_query(session, company_query)
    all_companies = {
        row.get("company_id", "") for row in companies if row.get("company_id", "")
    }

    # 构建关联关系图（字典形式）
    relation_graph = defaultdict(set)

    # 添加共享法人的边
    legal_person_query = """
    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
    WITH p, collect(id(c)) as companies
    WHERE size(companies) >= 2
    RETURN companies
    """
    rows = execute_query(session, legal_person_query)
    for row in rows:
        companies = row.get("companies", [])
        for i, c1 in enumerate(companies):
            for c2 in companies[i + 1 :]:
                if c1 and c2:
                    relation_graph[c1].add(c2)
                    relation_graph[c2].add(c1)

    # 添加控股关系的边
    controls_query = """
    MATCH (c1:Company)-[:CONTROLS]-(c2:Company)
    RETURN id(c1) as c1, id(c2) as c2
    """
    rows = execute_query(session, controls_query)
    for row in rows:
        c1 = row.get("c1", "")
        c2 = row.get("c2", "")
        if c1 and c2:
            relation_graph[c1].add(c2)
            relation_graph[c2].add(c1)

    # 社区检测：找出连通的公司集群（简化版BFS）
    visited = set()
    communities = []

    for node in all_companies:
        if node not in visited:
            queue = [node]
            community = set()

            while queue:
                current = queue.pop(0)
                if current in visited:
                    continue

                visited.add(current)
                community.add(current)

                for neighbor in relation_graph.get(current, []):
                    if neighbor not in visited:
                        queue.append(neighbor)

            if len(community) >= config.min_cluster_size:
                communities.append(list(community))

    suspicious_networks = []

    for comm_idx, comm in enumerate(communities):
        collusion_features = analyze_collusion_patterns(
            comm, session, periods=periods, config=config
        )

        if collusion_features["risk_score"] >= config.risk_score_threshold:
            suspicious_networks.append(
                {
                    "network_id": f"NETWORK_{comm_idx + 1}",
                    "companies": comm,
                    "size": len(comm),
                    **collusion_features,
                }
            )

    return suspicious_networks


def generate_collusion_html(
    network: Dict,
    session,
    contract_id: str = None,
    output_filename: str = None,
) -> str:
    """
    生成串通网络的交互式HTML页面

    Args:
        network: 串通网络数据
        session: Nebula session
        contract_id: 入口合同ID（用于展示合同节点和PARTY关系）
        output_filename: 输出文件名

    Returns:
        str: 生成的HTML文件路径
    """
    if output_filename is None:
        safe_id = network["network_id"].replace('"', "").replace("'", "").replace("/", "_")
        output_filename = f"collusion_network_{safe_id}.html"

    os.makedirs(REPORTS_DIR, exist_ok=True)
    output_path = os.path.join(REPORTS_DIR, output_filename)

    # Build nodes and edges for visualization
    nodes = []
    edges = []
    node_ids = set()

    # Query company names
    company_ids_str = ", ".join([f'"{c}"' for c in network["companies"]])
    company_query = f"""
    MATCH (c:Company)
    WHERE id(c) IN [{company_ids_str}]
    RETURN id(c) as company_id, c.Company.name as name
    """
    company_rows = execute_query(session, company_query)
    company_names = {row.get("company_id", ""): row.get("name", "") for row in company_rows}

    # Add company nodes
    for idx, comp_id in enumerate(network["companies"]):
        comp_name = company_names.get(comp_id, comp_id)
        node_type = "CoreCompany" if idx == 0 else "RelatedCompany"
        nodes.append(
            {
                "id": comp_id,
                "type": node_type,
                "label": comp_name,
                "properties": {"role": "核心公司" if idx == 0 else "关联公司"},
            }
        )
        node_ids.add(comp_id)

    # Query relations between companies (LEGAL_PERSON and CONTROLS)
    relation_query = f"""
    MATCH (c1:Company)-[e:LEGAL_PERSON|CONTROLS]-(c2:Company)
    WHERE id(c1) IN [{company_ids_str}] AND id(c2) IN [{company_ids_str}]
    RETURN id(c1) as c1, id(c2) as c2, type(e) as rel_type
    """
    relation_rows = execute_query(session, relation_query)
    seen_edges = set()
    for row in relation_rows:
        c1 = row.get("c1", "")
        c2 = row.get("c2", "")
        rel_type = row.get("rel_type", "RELATED")
        if c1 and c2 and c1 in node_ids and c2 in node_ids:
            edge_key = tuple(sorted([c1, c2])) + (rel_type,)
            if edge_key not in seen_edges:
                seen_edges.add(edge_key)
                edges.append(
                    {
                        "source": c1,
                        "target": c2,
                        "type": rel_type,
                        "properties": {"relation": rel_type},
                    }
                )

    # Add the entry contract node and PARTY edges
    if contract_id:
        contract_info = get_contract_info(session, contract_id)
        party_relations = get_contract_party_relations(session, contract_id)
        
        if contract_id not in node_ids:
            nodes.append(
                {
                    "id": contract_id,
                    "type": "Contract",
                    "label": contract_info.get("contract_name", contract_id),
                    "properties": {
                        "role": "入口合同",
                        "contract_no": contract_info.get("contract_no", ""),
                        "amount": f"¥{contract_info.get('amount', 0):,.2f}" if contract_info.get('amount') else "",
                        "sign_date": contract_info.get("sign_date", ""),
                    },
                }
            )
            node_ids.add(contract_id)
        
        for rel in party_relations:
            company_id = rel["company_id"]
            party_type = rel["party_type"]
            if company_id in node_ids:
                edges.append(
                    {
                        "source": company_id,
                        "target": contract_id,
                        "type": party_type,
                        "properties": {
                            "role": "甲方" if party_type == "PARTY_A" else "乙方",
                        },
                    }
                )

    # Add related contracts as nodes
    contract_ids = network.get("contract_ids", [])
    for cid in contract_ids[:10]:  # Limit to 10 contracts
        if cid not in node_ids:
            cinfo = get_contract_info(session, cid)
            nodes.append(
                {
                    "id": cid,
                    "type": "RelatedContract",
                    "label": cinfo.get("contract_name", cid),
                    "properties": {
                        "role": "关联合同",
                        "contract_no": cinfo.get("contract_no", ""),
                        "amount": f"¥{cinfo.get('amount', 0):,.2f}" if cinfo.get('amount') else "",
                        "sign_date": cinfo.get("sign_date", ""),
                    },
                }
            )
            node_ids.add(cid)
            
            # Add PARTY edges for related contracts
            cparty_relations = get_contract_party_relations(session, cid)
            for rel in cparty_relations:
                company_id = rel["company_id"]
                party_type = rel["party_type"]
                if company_id in node_ids:
                    edges.append(
                        {
                            "source": company_id,
                            "target": cid,
                            "type": party_type,
                            "properties": {
                                "role": "甲方" if party_type == "PARTY_A" else "乙方",
                            },
                        }
                    )

    network_name = network.get("network_id", "Unknown")
    nodes_json = json.dumps(nodes, ensure_ascii=False)
    edges_json = json.dumps(edges, ensure_ascii=False)

    html_content = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>串通网络分析 - {network_name}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 
                         'Hiragino Sans GB', 'Microsoft YaHei', sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            min-height: 100vh;
            color: #e8e8e8;
        }}
        
        .container {{
            max-width: 1600px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        header {{
            text-align: center;
            padding: 30px 0;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
            margin-bottom: 20px;
        }}
        
        header h1 {{
            font-size: 2.2em;
            font-weight: 600;
            background: linear-gradient(135deg, #e74c3c 0%, #f39c12 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 10px;
        }}
        
        header p {{
            color: #8892b0;
            font-size: 1.1em;
        }}
        
        .stats-bar {{
            display: flex;
            justify-content: center;
            gap: 40px;
            margin: 20px 0;
            flex-wrap: wrap;
        }}
        
        .stat-item {{
            text-align: center;
        }}
        
        .stat-value {{
            font-size: 1.8em;
            font-weight: 700;
            color: #e74c3c;
        }}
        
        .stat-value.warning {{
            color: #f39c12;
        }}
        
        .stat-value.info {{
            color: #3498db;
        }}
        
        .stat-label {{
            font-size: 0.9em;
            color: #8892b0;
            margin-top: 5px;
        }}
        
        .main-content {{
            display: grid;
            grid-template-columns: 300px 1fr;
            gap: 20px;
        }}
        
        .sidebar {{
            background: rgba(255, 255, 255, 0.03);
            border-radius: 16px;
            padding: 20px;
            border: 1px solid rgba(255, 255, 255, 0.08);
            backdrop-filter: blur(10px);
        }}
        
        .sidebar h3 {{
            font-size: 1.1em;
            color: #e74c3c;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        }}
        
        .legend {{
            margin-bottom: 25px;
        }}
        
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 10px;
            font-size: 0.9em;
        }}
        
        .legend-dot {{
            width: 16px;
            height: 16px;
            border-radius: 50%;
            flex-shrink: 0;
        }}
        
        .node-list {{
            max-height: 400px;
            overflow-y: auto;
        }}
        
        .node-item {{
            padding: 10px 12px;
            margin-bottom: 8px;
            background: rgba(255, 255, 255, 0.03);
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s;
            border-left: 3px solid transparent;
        }}
        
        .node-item:hover {{
            background: rgba(255, 255, 255, 0.08);
            transform: translateX(3px);
        }}
        
        .node-item.active {{
            background: rgba(231, 76, 60, 0.1);
            border-left-color: #e74c3c;
        }}
        
        .node-item-type {{
            font-size: 0.75em;
            color: #8892b0;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .node-item-label {{
            font-size: 0.95em;
            color: #e8e8e8;
            margin-top: 3px;
            word-break: break-word;
        }}
        
        .graph-panel {{
            background: rgba(255, 255, 255, 0.03);
            border-radius: 16px;
            border: 1px solid rgba(255, 255, 255, 0.08);
            overflow: hidden;
        }}
        
        .graph-toolbar {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 15px 20px;
            background: rgba(0, 0, 0, 0.2);
            border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        }}
        
        .graph-toolbar h3 {{
            color: #e8e8e8;
            font-size: 1em;
        }}
        
        .toolbar-buttons {{
            display: flex;
            gap: 10px;
        }}
        
        .btn {{
            padding: 8px 16px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 0.9em;
            transition: all 0.2s;
            background: rgba(255, 255, 255, 0.1);
            color: #e8e8e8;
        }}
        
        .btn:hover {{
            background: rgba(255, 255, 255, 0.2);
        }}
        
        .btn-primary {{
            background: linear-gradient(135deg, #e74c3c 0%, #f39c12 100%);
            color: #1a1a2e;
            font-weight: 600;
        }}
        
        .btn-primary:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 20px rgba(231, 76, 60, 0.3);
        }}
        
        #graph-svg {{
            width: 100%;
            height: 700px;
            background: radial-gradient(circle at center, rgba(231, 76, 60, 0.03) 0%, transparent 70%);
        }}
        
        .node circle {{
            stroke-width: 3px;
            filter: drop-shadow(0 2px 8px rgba(0, 0, 0, 0.3));
        }}
        
        .node text {{
            font-size: 11px;
            fill: #e8e8e8;
            pointer-events: none;
            text-shadow: 0 1px 3px rgba(0, 0, 0, 0.8);
        }}
        
        .link {{
            stroke-opacity: 0.6;
        }}
        
        .link-label {{
            font-size: 9px;
            fill: #8892b0;
            text-shadow: 0 1px 2px rgba(0, 0, 0, 0.8);
        }}
        
        .tooltip {{
            position: absolute;
            background: rgba(26, 26, 46, 0.95);
            border: 1px solid rgba(231, 76, 60, 0.3);
            border-radius: 12px;
            padding: 15px;
            font-size: 0.9em;
            pointer-events: none;
            z-index: 1000;
            max-width: 350px;
            backdrop-filter: blur(10px);
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
        }}
        
        .tooltip h4 {{
            color: #e74c3c;
            margin-bottom: 10px;
            font-size: 1.1em;
        }}
        
        .tooltip-row {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 6px;
        }}
        
        .tooltip-key {{
            color: #8892b0;
        }}
        
        .tooltip-value {{
            color: #e8e8e8;
            text-align: right;
            max-width: 200px;
            word-break: break-word;
        }}
        
        .detail-panel {{
            position: fixed;
            right: 20px;
            top: 100px;
            width: 350px;
            background: rgba(26, 26, 46, 0.95);
            border: 1px solid rgba(231, 76, 60, 0.2);
            border-radius: 16px;
            padding: 20px;
            display: none;
            z-index: 100;
            backdrop-filter: blur(10px);
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
        }}
        
        .detail-panel.show {{
            display: block;
        }}
        
        .detail-panel h4 {{
            color: #e74c3c;
            margin-bottom: 15px;
            font-size: 1.1em;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .detail-panel .close-btn {{
            cursor: pointer;
            color: #8892b0;
            font-size: 1.5em;
            line-height: 1;
        }}
        
        .detail-panel .close-btn:hover {{
            color: #e8e8e8;
        }}
        
        .detail-content {{
            max-height: 400px;
            overflow-y: auto;
        }}
        
        .detail-row {{
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }}
        
        .detail-row:last-child {{
            border-bottom: none;
        }}
        
        ::-webkit-scrollbar {{
            width: 6px;
        }}
        
        ::-webkit-scrollbar-track {{
            background: rgba(255, 255, 255, 0.05);
            border-radius: 3px;
        }}
        
        ::-webkit-scrollbar-thumb {{
            background: rgba(231, 76, 60, 0.3);
            border-radius: 3px;
        }}
        
        ::-webkit-scrollbar-thumb:hover {{
            background: rgba(231, 76, 60, 0.5);
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔗 关联方串通网络分析</h1>
            <p>网络ID: {network_name} | 公司数量: {network["size"]}</p>
        </header>
        
        <div class="stats-bar">
            <div class="stat-item">
                <div class="stat-value">{network["risk_score"]:.2%}</div>
                <div class="stat-label">风险分数</div>
            </div>
            <div class="stat-item">
                <div class="stat-value warning">{network.get("rotation_score", 0):.2%}</div>
                <div class="stat-label">轮换分数</div>
            </div>
            <div class="stat-item">
                <div class="stat-value warning">{network.get("amount_similarity", 0):.2%}</div>
                <div class="stat-label">金额相似度</div>
            </div>
            <div class="stat-item">
                <div class="stat-value info">{network.get("threshold_ratio", 0):.2%}</div>
                <div class="stat-label">卡阈值比例</div>
            </div>
            <div class="stat-item">
                <div class="stat-value info">{network.get("network_density", 0):.2%}</div>
                <div class="stat-label">网络密度</div>
            </div>
            <div class="stat-item">
                <div class="stat-value info">{network.get("contract_count", 0)}</div>
                <div class="stat-label">合同数量</div>
            </div>
        </div>
        
        <div class="main-content">
            <div class="sidebar">
                <div class="legend">
                    <h3>图例说明</h3>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #e74c3c;"></div>
                        <span>核心公司 (Core)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #f39c12;"></div>
                        <span>关联公司 (Related)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #2ecc71;"></div>
                        <span>入口合同 (Contract)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #3498db;"></div>
                        <span>关联合同 (Related Contract)</span>
                    </div>
                    <div class="legend-item" style="margin-top: 15px; border-top: 1px solid rgba(255,255,255,0.1); padding-top: 10px;">
                        <span style="font-size: 0.85em; color: #8892b0;">边类型：</span>
                    </div>
                    <div class="legend-item">
                        <div style="width: 30px; height: 2px; background: #9b59b6;"></div>
                        <span>法人关系 (LEGAL_PERSON)</span>
                    </div>
                    <div class="legend-item">
                        <div style="width: 30px; height: 2px; background: #e74c3c;"></div>
                        <span>控股关系 (CONTROLS)</span>
                    </div>
                    <div class="legend-item">
                        <div style="width: 30px; height: 2px; background: #3498db;"></div>
                        <span>合同关系 (PARTY)</span>
                    </div>
                </div>
                
                <h3>节点列表</h3>
                <div class="node-list" id="node-list"></div>
            </div>
            
            <div class="graph-panel">
                <div class="graph-toolbar">
                    <h3>串通网络图谱</h3>
                    <div class="toolbar-buttons">
                        <button class="btn" onclick="zoomIn()">🔍 放大</button>
                        <button class="btn" onclick="zoomOut()">🔍 缩小</button>
                        <button class="btn" onclick="resetView()">↺ 重置</button>
                        <button class="btn btn-primary" onclick="exportData()">📥 导出数据</button>
                    </div>
                </div>
                <svg id="graph-svg"></svg>
            </div>
        </div>
    </div>
    
    <div class="detail-panel" id="detail-panel">
        <h4>
            <span id="detail-title">节点详情</span>
            <span class="close-btn" onclick="closeDetailPanel()">×</span>
        </h4>
        <div class="detail-content" id="detail-content"></div>
    </div>
    
    <div class="tooltip" id="tooltip" style="display: none;"></div>

    <script>
        const graphData = {{
            nodes: {nodes_json},
            edges: {edges_json}
        }};
        
        const colorMap = {{
            'CoreCompany': '#e74c3c',
            'RelatedCompany': '#f39c12',
            'Contract': '#2ecc71',
            'RelatedContract': '#3498db'
        }};
        
        const edgeColorMap = {{
            'LEGAL_PERSON': '#9b59b6',
            'CONTROLS': '#e74c3c',
            'PARTY_A': '#3498db',
            'PARTY_B': '#3498db'
        }};
        
        function renderNodeList() {{
            const listEl = document.getElementById('node-list');
            const grouped = {{}};
            
            graphData.nodes.forEach(node => {{
                if (!grouped[node.type]) grouped[node.type] = [];
                grouped[node.type].push(node);
            }});
            
            let html = '';
            for (const [type, nodes] of Object.entries(grouped)) {{
                nodes.forEach(node => {{
                    html += `
                        <div class="node-item" data-id="${{node.id}}" onclick="focusNode('${{node.id}}')">
                            <div class="node-item-type" style="color: ${{colorMap[type]}}">${{type}}</div>
                            <div class="node-item-label">${{node.label}}</div>
                        </div>
                    `;
                }});
            }}
            
            listEl.innerHTML = html;
        }}
        
        renderNodeList();
        
        const svg = d3.select('#graph-svg');
        const width = svg.node().getBoundingClientRect().width;
        const height = 700;
        
        svg.attr('viewBox', [0, 0, width, height]);
        
        const g = svg.append('g');
        
        const zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', (event) => {{
                g.attr('transform', event.transform);
            }});
        
        svg.call(zoom);
        
        const nodes = graphData.nodes.map(n => ({{...n}}));
        const links = graphData.edges.map(e => ({{
            source: e.source,
            target: e.target,
            type: e.type,
            properties: e.properties
        }}));
        
        const simulation = d3.forceSimulation(nodes)
            .force('link', d3.forceLink(links).id(d => d.id).distance(150))
            .force('charge', d3.forceManyBody().strength(-500))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(50));
        
        const defs = svg.append('defs');
        ['LEGAL_PERSON', 'CONTROLS', 'PARTY_A', 'PARTY_B'].forEach(type => {{
            defs.append('marker')
                .attr('id', `arrow-${{type}}`)
                .attr('viewBox', '0 -5 10 10')
                .attr('refX', 28)
                .attr('refY', 0)
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .attr('orient', 'auto')
                .append('path')
                .attr('fill', edgeColorMap[type] || '#4a5568')
                .attr('d', 'M0,-5L10,0L0,5');
        }});
        
        const link = g.append('g')
            .selectAll('line')
            .data(links)
            .join('line')
            .attr('class', 'link')
            .attr('stroke', d => edgeColorMap[d.type] || '#4a5568')
            .attr('stroke-width', 2)
            .attr('marker-end', d => `url(#arrow-${{d.type}})`);
        
        const linkLabel = g.append('g')
            .selectAll('text')
            .data(links)
            .join('text')
            .attr('class', 'link-label')
            .text(d => d.type);
        
        const node = g.append('g')
            .selectAll('g')
            .data(nodes)
            .join('g')
            .attr('class', 'node')
            .call(d3.drag()
                .on('start', dragstarted)
                .on('drag', dragged)
                .on('end', dragended));
        
        node.append('circle')
            .attr('r', d => d.type === 'CoreCompany' ? 28 : 20)
            .attr('fill', d => colorMap[d.type] || '#999')
            .attr('stroke', d => d.type === 'CoreCompany' ? '#fff' : 'rgba(255,255,255,0.3)')
            .attr('stroke-width', d => d.type === 'CoreCompany' ? 4 : 2);
        
        node.append('text')
            .attr('dy', 40)
            .attr('text-anchor', 'middle')
            .text(d => d.label.length > 12 ? d.label.substring(0, 12) + '...' : d.label);
        
        const tooltip = d3.select('#tooltip');
        
        node.on('mouseover', (event, d) => {{
            let html = `<h4>${{d.label}}</h4>`;
            html += `<div class="tooltip-row"><span class="tooltip-key">类型</span><span class="tooltip-value">${{d.type}}</span></div>`;
            html += `<div class="tooltip-row"><span class="tooltip-key">ID</span><span class="tooltip-value">${{d.id}}</span></div>`;
            
            if (d.properties) {{
                for (const [key, value] of Object.entries(d.properties)) {{
                    if (value) {{
                        html += `<div class="tooltip-row"><span class="tooltip-key">${{key}}</span><span class="tooltip-value">${{value}}</span></div>`;
                    }}
                }}
            }}
            
            tooltip.html(html)
                .style('display', 'block')
                .style('left', (event.pageX + 15) + 'px')
                .style('top', (event.pageY - 10) + 'px');
        }})
        .on('mouseout', () => {{
            tooltip.style('display', 'none');
        }})
        .on('click', (event, d) => {{
            showDetailPanel(d);
        }});
        
        link.on('mouseover', (event, d) => {{
            let html = `<h4>${{d.type}}</h4>`;
            if (d.properties) {{
                for (const [key, value] of Object.entries(d.properties)) {{
                    if (value) {{
                        html += `<div class="tooltip-row"><span class="tooltip-key">${{key}}</span><span class="tooltip-value">${{value}}</span></div>`;
                    }}
                }}
            }}
            
            tooltip.html(html)
                .style('display', 'block')
                .style('left', (event.pageX + 15) + 'px')
                .style('top', (event.pageY - 10) + 'px');
        }})
        .on('mouseout', () => {{
            tooltip.style('display', 'none');
        }});
        
        simulation.on('tick', () => {{
            link
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);
            
            linkLabel
                .attr('x', d => (d.source.x + d.target.x) / 2)
                .attr('y', d => (d.source.y + d.target.y) / 2);
            
            node.attr('transform', d => `translate(${{d.x}},${{d.y}})`);
        }});
        
        function dragstarted(event, d) {{
            if (!event.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
        }}
        
        function dragged(event, d) {{
            d.fx = event.x;
            d.fy = event.y;
        }}
        
        function dragended(event, d) {{
            if (!event.active) simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
        }}
        
        function zoomIn() {{
            svg.transition().call(zoom.scaleBy, 1.3);
        }}
        
        function zoomOut() {{
            svg.transition().call(zoom.scaleBy, 0.7);
        }}
        
        function resetView() {{
            svg.transition().call(zoom.transform, d3.zoomIdentity);
        }}
        
        function focusNode(nodeId) {{
            const targetNode = nodes.find(n => n.id === nodeId);
            if (targetNode) {{
                const transform = d3.zoomIdentity
                    .translate(width / 2 - targetNode.x, height / 2 - targetNode.y);
                svg.transition().duration(500).call(zoom.transform, transform);
                
                document.querySelectorAll('.node-item').forEach(el => el.classList.remove('active'));
                document.querySelector(`.node-item[data-id="${{nodeId}}"]`)?.classList.add('active');
                
                showDetailPanel(targetNode);
            }}
        }}
        
        function showDetailPanel(node) {{
            const panel = document.getElementById('detail-panel');
            const title = document.getElementById('detail-title');
            const content = document.getElementById('detail-content');
            
            title.textContent = node.label;
            
            let html = `
                <div class="detail-row">
                    <span class="tooltip-key">类型</span>
                    <span class="tooltip-value">${{node.type}}</span>
                </div>
                <div class="detail-row">
                    <span class="tooltip-key">ID</span>
                    <span class="tooltip-value">${{node.id}}</span>
                </div>
            `;
            
            if (node.properties) {{
                for (const [key, value] of Object.entries(node.properties)) {{
                    if (value) {{
                        html += `
                            <div class="detail-row">
                                <span class="tooltip-key">${{key}}</span>
                                <span class="tooltip-value">${{value}}</span>
                            </div>
                        `;
                    }}
                }}
            }}
            
            content.innerHTML = html;
            panel.classList.add('show');
        }}
        
        function closeDetailPanel() {{
            document.getElementById('detail-panel').classList.remove('show');
        }}
        
        function exportData() {{
            const data = JSON.stringify(graphData, null, 2);
            const blob = new Blob([data], {{ type: 'application/json' }});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'collusion_network.json';
            a.click();
            URL.revokeObjectURL(url);
        }}
    </script>
</body>
</html>
'''

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    return output_path


def detect_collusion_by_contract(
    session,
    contract_id: str,
    config: CollusionConfig = DEFAULT_CONFIG,
    periods: Optional[List[str]] = None,
) -> Dict:
    """
    以合同为入口检测串通网络
    
    找到合同的甲/乙方公司，以这些公司为核心检测串通网络
    
    Args:
        session: Nebula session
        contract_id: 合同ID
        config: 串通分析配置
        periods: 时间段列表
    
    Returns:
        dict: {
            "contract_id": str,
            "parties": list,
            "networks": list,
            "html_url": str,
        }
    """
    # Step 1: 获取合同的甲/乙方
    parties = get_contract_parties(session, contract_id)
    
    if not parties:
        return {
            "contract_id": contract_id,
            "parties": [],
            "networks": [],
            "html_url": None,
            "message": "未找到合同相关方"
        }
    
    # Step 2: 检测串通网络
    all_networks = detect_collusion_network(
        session=session,
        company_ids=None,  # 检测全部
        periods=periods,
        config=config,
    )
    
    # Step 3: 筛选出包含合同相关方的网络
    relevant_networks = [
        n for n in all_networks
        if any(p in n["companies"] for p in parties)
    ]
    
    if not relevant_networks:
        return {
            "contract_id": contract_id,
            "parties": parties,
            "networks": [],
            "html_url": None,
            "message": "未检测到与该合同相关的串通网络"
        }
    
    # Step 4: 按风险分数排序
    sorted_networks = sorted(
        relevant_networks, key=lambda x: x["risk_score"], reverse=True
    )
    top_network = sorted_networks[0]
    
    # Step 5: 生成 HTML
    html_path = generate_collusion_html(top_network, session, contract_id=contract_id)
    
    # Collect all contract IDs
    all_contract_ids = list(set(
        cid for n in sorted_networks for cid in n.get("contract_ids", [])
    ))
    
    return {
        "contract_id": contract_id,
        "parties": parties,
        "networks": sorted_networks,
        "html_url": html_path,
        "contract_ids": all_contract_ids,
        "message": f"检测到 {len(sorted_networks)} 个相关串通网络"
    }


def main(
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
    config: CollusionConfig = DEFAULT_CONFIG,
):
    """
    Main function for collusion network analysis
    
    Args:
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）
        config: 串通分析配置
    """
    print("=" * 70)
    print("关联方串通网络分析")
    print("=" * 70)
    
    if company_ids:
        print(f"  过滤公司: {len(company_ids)} 家")
    if periods:
        print(f"  时间范围: {periods}")

    session = None
    try:
        session = get_nebula_session()

        print("\n[1/3] 构建关联关系图...")
        suspicious_networks = detect_collusion_network(
            session,
            company_ids=company_ids,
            periods=periods,
            config=config,
        )

        print(f"  发现可疑串通网络数: {len(suspicious_networks)}")

        if len(suspicious_networks) == 0:
            print("\n未发现可疑的串通网络")
            return

        print("\n[2/3] 分析串通模式...")

        # 查询公司信息用于展示
        company_filter = ""
        if company_ids:
            ids_str = ", ".join([f"'{cid}'" for cid in company_ids])
            company_filter = f"WHERE c.Company.number IN [{ids_str}]"
        
        company_query = f"""
        MATCH (c:Company)
        {company_filter}
        RETURN id(c) as company_id, c.Company.name as name
        """
        companies = execute_query(session, company_query)
        company_names = {
            row.get("company_id", ""): row.get("name", "") for row in companies
        }

        # 生成详细报告
        report_data = []
        for network in suspicious_networks:
            company_list = network["companies"][:5]
            company_names_str = ", ".join(
                [company_names.get(c, str(c)) for c in company_list]
            ) + ("..." if len(network["companies"]) > 5 else "")

            report_data.append(
                {
                    "network_id": network["network_id"],
                    "company_count": network["size"],
                    "risk_score": network["risk_score"],
                    "rotation_score": network.get("rotation_score", 0),
                    "amount_similarity": network.get("amount_similarity", 0),
                    "threshold_ratio": network.get("threshold_ratio", 0),
                    "network_density": network.get("network_density", 0),
                    "contract_count": network.get("contract_count", 0),
                    "total_amount": network.get("total_amount", 0),
                    "companies": company_names_str,
                }
            )

        report_df = pd.DataFrame(report_data)
        report_df = report_df.sort_values("risk_score", ascending=False)

        print("\n[3/3] 生成报告...")

        os.makedirs(REPORTS_DIR, exist_ok=True)

        output_file = os.path.join(REPORTS_DIR, "collusion_network_report.csv")
        report_df.to_csv(output_file, index=False, encoding="utf-8-sig")

        print("\n前 5 高风险串通网络：\n")
        for idx, row in report_df.head(5).iterrows():
            print(f"{row['network_id']}:")
            print(f"  公司数量: {row['company_count']}")
            print(f"  风险分数: {row['risk_score']:.4f}")
            print(f"  轮换分数: {row['rotation_score']:.4f}")
            print(f"  金额相似度: {row['amount_similarity']:.4f}")
            print(f"  卡阈值比例: {row['threshold_ratio']:.2%}")
            print(f"  网络密度: {row['network_density']:.4f}")
            print(f"  合同总数: {row['contract_count']}")
            print(f"  涉及金额: ¥{row['total_amount']:,.2f}")
            print(f"  公司列表: {row['companies']}")
            print()

        print(f"完整报告已保存至: reports/collusion_network_report.csv")

    finally:
        if session:
            session.release()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="关联方串通网络分析")
    parser.add_argument(
        "--company-ids",
        type=str,
        default=None,
        help="公司编号列表，逗号分隔",
    )
    parser.add_argument(
        "--periods",
        type=str,
        default=None,
        help="时间范围，格式：YYYY-MM-DD 或 YYYY-MM-DD,YYYY-MM-DD",
    )
    parser.add_argument(
        "--min-cluster-size",
        type=int,
        default=3,
        help="最小集群大小",
    )
    parser.add_argument(
        "--risk-threshold",
        type=float,
        default=0.5,
        help="风险分数阈值",
    )
    args = parser.parse_args()

    company_ids = args.company_ids.split(",") if args.company_ids else None
    periods = args.periods.split(",") if args.periods else None
    
    config = CollusionConfig(
        min_cluster_size=args.min_cluster_size,
        risk_score_threshold=args.risk_threshold,
    )

    main(company_ids=company_ids, periods=periods, config=config)
