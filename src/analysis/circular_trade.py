"""
场景二：高级循环交易检测（分散-汇聚模式）

检测复杂的循环交易模式，包括分散-汇聚模式
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Optional, Dict, Set
from dataclasses import dataclass
from src.utils.nebula_utils import get_nebula_session, execute_query

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")


def build_company_filter(company_ids: Optional[List[str]] = None) -> str:
    """Build company ID filter clause for nGQL"""
    if not company_ids:
        return ""
    ids_str = ", ".join([f"'{cid}'" for cid in company_ids])
    return f"WHERE c.Company.number IN [{ids_str}]"


def build_periods_filter(
    periods: Optional[List[str]] = None,
    date_field: str = "t.Transaction.transaction_date",
) -> str:
    """Build time period filter clause for nGQL"""
    if not periods:
        return ""
    if len(periods) == 1:
        return f"WHERE {date_field} == '{periods[0]}'"
    elif len(periods) == 2:
        return (
            f"WHERE {date_field} >= '{periods[0]}' AND {date_field} <= '{periods[1]}'"
        )
    else:
        raise ValueError("periods list must have 1 or 2 elements")


def get_related_companies(company_id, session):
    """
    获取公司的关联方：
    1. 共同法人的公司
    2. 控股/被控股的公司
    """
    related = {company_id}

    # 1. 通过法人关系
    legal_person_query = f"""
    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
    WHERE id(c) == "{company_id}"
    WITH p
    MATCH (p)-[:LEGAL_PERSON]->(c2:Company)
    RETURN id(c2) as company_id
    """
    rows = execute_query(session, legal_person_query)
    for row in rows:
        comp_id = row.get("company_id", "")
        if comp_id:
            related.add(comp_id)

    # 2. 通过控股关系
    controls_query = f"""
    MATCH (c1:Company)-[:CONTROLS*0..2]-(c2:Company)
    WHERE id(c1) == "{company_id}"
    RETURN DISTINCT id(c2) as company_id
    """
    rows = execute_query(session, controls_query)
    for row in rows:
        comp_id = row.get("company_id", "")
        if comp_id:
            related.add(comp_id)

    return related


def calculate_circular_trade_risk(similarity, num_dispersed, num_inter_trades):
    """
    计算循环交易风险分数
    """
    # 金额相似度权重 40%
    similarity_score = similarity * 0.4

    # 分散节点数量权重 30% (越多越可疑)
    dispersed_score = min(num_dispersed / 10, 1.0) * 0.3

    # 中间交易密度权重 30%
    inter_trade_score = min(num_inter_trades / 20, 1.0) * 0.3

    return similarity_score + dispersed_score + inter_trade_score


def get_contracts_from_transactions(session, transaction_ids: List[str]) -> List[str]:
    """
    从交易ID列表中获取关联的合同ID

    Transaction 通过 contract_no 属性关联 Contract（而非边关系）

    Args:
        session: Nebula session
        transaction_ids: 交易ID列表

    Returns:
        list: 合同ID列表
    """
    if not transaction_ids:
        return []

    ids_str = ", ".join([f'"{tid}"' for tid in transaction_ids])
    # Transaction.contract_no 对应 Contract.contract_no
    query = f"""
    MATCH (t:Transaction)
    WHERE id(t) IN [{ids_str}]
    WITH t.Transaction.contract_no AS contract_no
    WHERE contract_no IS NOT NULL AND contract_no != ""
    MATCH (con:Contract)
    WHERE con.Contract.contract_no == contract_no
    RETURN DISTINCT id(con) as contract_id
    """
    rows = execute_query(session, query)
    return [row.get("contract_id", "") for row in rows if row.get("contract_id")]


def detect_fan_out_fan_in(
    session,
    time_window_days=180,
    amount_threshold=1000000,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
):
    """
    检测分散-汇聚模式的循环交易

    Args:
        session: Nebula session
        time_window_days: 时间窗口（天）
        amount_threshold: 金额阈值
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）

    Returns:
        list: 可疑模式列表（包含 transaction_ids 和 contract_ids）
    """
    # Build filter conditions
    where_clauses = []
    if company_ids:
        ids_str = ", ".join([f"'{cid}'" for cid in company_ids])
        where_clauses.append(
            f"(payer.Company.number IN [{ids_str}] OR receiver.Company.number IN [{ids_str}])"
        )
    if periods:
        if len(periods) == 1:
            where_clauses.append(f"t.Transaction.transaction_date == '{periods[0]}'")
        elif len(periods) == 2:
            where_clauses.append(
                f"t.Transaction.transaction_date >= '{periods[0]}' AND t.Transaction.transaction_date <= '{periods[1]}'"
            )

    where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    money_flow_query = f"""
    MATCH (payer:Company)-[:PAYS]->(t:Transaction)-[:RECEIVES]->(receiver:Company)
    {where_clause}
    RETURN id(payer) as payer_company,
           payer.Company.name as payer_name,
           id(receiver) as receiver_company,
           receiver.Company.name as receiver_name,
           id(t) as transaction_id,
           t.Transaction.amount as transaction_amount,
           t.Transaction.transaction_date as transaction_date
    """
    rows = execute_query(session, money_flow_query)

    # 转换为 DataFrame
    money_flows_data = []
    for row in rows:
        money_flows_data.append(
            {
                "payer_company": row.get("payer_company", ""),
                "payer_name": row.get("payer_name", ""),
                "receiver_company": row.get("receiver_company", ""),
                "receiver_name": row.get("receiver_name", ""),
                "transaction_id": row.get("transaction_id", ""),
                "transaction_amount": float(row.get("transaction_amount", 0) or 0),
                "transaction_date": row.get("transaction_date", ""),
            }
        )

    if not money_flows_data:
        return []

    money_flows = pd.DataFrame(money_flows_data)
    money_flows["txn_date"] = pd.to_datetime(money_flows["transaction_date"])

    suspicious_patterns = []

    # 对每个公司作为潜在的"核心公司"
    for central_company in money_flows["payer_company"].unique():
        # Step 1: 找出从该公司流出的所有交易
        outflows = money_flows[
            (money_flows["payer_company"] == central_company)
            & (money_flows["transaction_amount"] >= amount_threshold)
        ]

        if len(outflows) < 2:  # 至少分散到 2 个公司
            continue

        # 获取时间范围
        min_date = outflows["txn_date"].min()
        max_date = min_date + timedelta(days=time_window_days)

        # Step 2: 找出在时间窗口内流出的目标公司（分散节点）
        dispersed_companies = set(outflows["receiver_company"].unique())
        total_outflow = outflows["transaction_amount"].sum()

        # Step 3: 检查这些分散节点之间是否有交易
        inter_trades = money_flows[
            (money_flows["payer_company"].isin(dispersed_companies))
            & (money_flows["receiver_company"].isin(dispersed_companies))
            & (money_flows["txn_date"] >= min_date)
            & (money_flows["txn_date"] <= max_date)
        ]

        # Step 4: 检查是否有资金汇聚回核心公司或其关联公司
        related_companies = get_related_companies(central_company, session)

        inflows = money_flows[
            (money_flows["receiver_company"].isin(related_companies))
            & (money_flows["payer_company"].isin(dispersed_companies))
            & (money_flows["txn_date"] >= min_date)
            & (money_flows["txn_date"] <= max_date)
        ]

        if len(inflows) > 0:
            total_inflow = inflows["transaction_amount"].sum()

            # 计算相似度
            similarity = (
                min(total_inflow, total_outflow) / max(total_inflow, total_outflow)
                if max(total_inflow, total_outflow) > 0
                else 0
            )

            if similarity >= 0.7:  # 流入流出金额相似度 >= 70%
                # Collect all transaction IDs involved in this pattern
                outflow_txn_ids = outflows["transaction_id"].tolist()
                inter_txn_ids = inter_trades["transaction_id"].tolist()
                inflow_txn_ids = inflows["transaction_id"].tolist()
                all_txn_ids = list(
                    set(outflow_txn_ids + inter_txn_ids + inflow_txn_ids)
                )

                # Get contract IDs from transactions
                contract_ids = get_contracts_from_transactions(session, all_txn_ids)

                # Get central company name
                central_name = (
                    money_flows[money_flows["payer_company"] == central_company][
                        "payer_name"
                    ].iloc[0]
                    if len(outflows) > 0
                    else ""
                )

                suspicious_patterns.append(
                    {
                        "central_company": central_company,
                        "central_company_name": central_name,
                        "dispersed_companies": list(dispersed_companies),
                        "related_companies": list(related_companies),
                        "total_outflow": total_outflow,
                        "total_inflow": total_inflow,
                        "similarity": similarity,
                        "inter_trade_count": len(inter_trades),
                        "time_span_days": (inflows["txn_date"].max() - min_date).days,
                        "risk_score": calculate_circular_trade_risk(
                            similarity, len(dispersed_companies), len(inter_trades)
                        ),
                        "transaction_ids": all_txn_ids,
                        "contract_ids": contract_ids,
                        "outflow_transactions": outflows[
                            [
                                "transaction_id",
                                "receiver_company",
                                "receiver_name",
                                "transaction_amount",
                                "transaction_date",
                            ]
                        ].to_dict("records"),
                        "inflow_transactions": inflows[
                            [
                                "transaction_id",
                                "payer_company",
                                "payer_name",
                                "transaction_amount",
                                "transaction_date",
                            ]
                        ].to_dict("records"),
                    }
                )

    return suspicious_patterns


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


def generate_circular_trade_html(
    pattern: Dict, session, contract_id: str = None, output_filename: str = None
) -> str:
    """
    生成循环交易模式的交互式HTML页面

    Args:
        pattern: 循环交易模式数据
        session: Nebula session
        contract_id: 入口合同ID（用于展示合同节点和PARTY关系）
        output_filename: 输出文件名

    Returns:
        str: 生成的HTML文件路径
    """
    if output_filename is None:
        safe_id = (
            pattern["central_company"]
            .replace('"', "")
            .replace("'", "")
            .replace("/", "_")
        )
        output_filename = f"circular_trade_pattern_{safe_id}.html"

    os.makedirs(REPORTS_DIR, exist_ok=True)
    output_path = os.path.join(REPORTS_DIR, output_filename)

    # Build nodes and edges for visualization
    nodes = []
    edges = []
    node_ids = set()

    # Central company node
    central_id = pattern["central_company"]
    central_name = pattern.get("central_company_name", central_id)
    nodes.append(
        {
            "id": central_id,
            "type": "CentralCompany",
            "label": central_name,
            "properties": {
                "role": "核心公司",
                "total_outflow": f"¥{pattern['total_outflow']:,.2f}",
                "total_inflow": f"¥{pattern['total_inflow']:,.2f}",
            },
        }
    )
    node_ids.add(central_id)

    # Dispersed company nodes
    for comp_id in pattern["dispersed_companies"]:
        if comp_id not in node_ids:
            nodes.append(
                {
                    "id": comp_id,
                    "type": "DispersedCompany",
                    "label": comp_id,
                    "properties": {"role": "分散节点"},
                }
            )
            node_ids.add(comp_id)

    # Related company nodes
    for comp_id in pattern["related_companies"]:
        if comp_id not in node_ids:
            nodes.append(
                {
                    "id": comp_id,
                    "type": "RelatedCompany",
                    "label": comp_id,
                    "properties": {"role": "关联公司"},
                }
            )
            node_ids.add(comp_id)

    # Outflow edges (Central -> Dispersed)
    for txn in pattern.get("outflow_transactions", []):
        receiver = txn.get("receiver_company", "")
        receiver_name = txn.get("receiver_name", receiver)
        if receiver and receiver in node_ids:
            # Update node label if we have the name
            for n in nodes:
                if n["id"] == receiver and n["label"] == receiver:
                    n["label"] = receiver_name
            edges.append(
                {
                    "source": central_id,
                    "target": receiver,
                    "type": "OUTFLOW",
                    "properties": {
                        "amount": f"¥{txn.get('transaction_amount', 0):,.2f}",
                        "date": txn.get("transaction_date", ""),
                        "transaction_id": txn.get("transaction_id", ""),
                    },
                }
            )

    # Inflow edges (Dispersed -> Related/Central)
    for txn in pattern.get("inflow_transactions", []):
        payer = txn.get("payer_company", "")
        payer_name = txn.get("payer_name", payer)
        if payer and payer in node_ids:
            for n in nodes:
                if n["id"] == payer and n["label"] == payer:
                    n["label"] = payer_name
            # Find the receiver (should be in related_companies)
            for related_id in pattern["related_companies"]:
                edges.append(
                    {
                        "source": payer,
                        "target": related_id,
                        "type": "INFLOW",
                        "properties": {
                            "amount": f"¥{txn.get('transaction_amount', 0):,.2f}",
                            "date": txn.get("transaction_date", ""),
                            "transaction_id": txn.get("transaction_id", ""),
                        },
                    }
                )
                break  # Only add one edge per transaction

    # Add the entry contract node and PARTY edges
    if contract_id:
        contract_info = get_contract_info(session, contract_id)
        party_relations = get_contract_party_relations(session, contract_id)
        
        # Add contract node
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
        
        # Add PARTY_A/PARTY_B edges (Company -> Contract)
        for rel in party_relations:
            company_id = rel["company_id"]
            party_type = rel["party_type"]
            
            # Only add edge if the company is in the pattern
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

    nodes_json = json.dumps(nodes, ensure_ascii=False)
    edges_json = json.dumps(edges, ensure_ascii=False)

    html_content = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>循环交易模式分析 - {central_name}</title>
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
            background: linear-gradient(135deg, #ff6b6b 0%, #ffa502 100%);
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
            color: #ff6b6b;
        }}
        
        .stat-value.warning {{
            color: #ffa502;
        }}
        
        .stat-value.info {{
            color: #00d9ff;
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
            color: #ff6b6b;
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
            background: rgba(255, 107, 107, 0.1);
            border-left-color: #ff6b6b;
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
            background: linear-gradient(135deg, #ff6b6b 0%, #ffa502 100%);
            color: #1a1a2e;
            font-weight: 600;
        }}
        
        .btn-primary:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 20px rgba(255, 107, 107, 0.3);
        }}
        
        #graph-svg {{
            width: 100%;
            height: 700px;
            background: radial-gradient(circle at center, rgba(255, 107, 107, 0.03) 0%, transparent 70%);
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
            border: 1px solid rgba(255, 107, 107, 0.3);
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
            color: #ff6b6b;
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
            border: 1px solid rgba(255, 107, 107, 0.2);
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
            color: #ff6b6b;
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
        
        .risk-badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
        }}
        
        .risk-high {{
            background: rgba(255, 107, 107, 0.2);
            color: #ff6b6b;
        }}
        
        .risk-medium {{
            background: rgba(255, 165, 2, 0.2);
            color: #ffa502;
        }}
        
        ::-webkit-scrollbar {{
            width: 6px;
        }}
        
        ::-webkit-scrollbar-track {{
            background: rgba(255, 255, 255, 0.05);
            border-radius: 3px;
        }}
        
        ::-webkit-scrollbar-thumb {{
            background: rgba(255, 107, 107, 0.3);
            border-radius: 3px;
        }}
        
        ::-webkit-scrollbar-thumb:hover {{
            background: rgba(255, 107, 107, 0.5);
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔄 循环交易模式分析</h1>
            <p>分散-汇聚模式检测 | 核心公司: {central_name}</p>
        </header>
        
        <div class="stats-bar">
            <div class="stat-item">
                <div class="stat-value">{pattern["risk_score"]:.2%}</div>
                <div class="stat-label">风险分数</div>
            </div>
            <div class="stat-item">
                <div class="stat-value warning">¥{pattern["total_outflow"]:,.0f}</div>
                <div class="stat-label">流出金额</div>
            </div>
            <div class="stat-item">
                <div class="stat-value warning">¥{pattern["total_inflow"]:,.0f}</div>
                <div class="stat-label">流入金额</div>
            </div>
            <div class="stat-item">
                <div class="stat-value info">{pattern["similarity"]:.1%}</div>
                <div class="stat-label">金额相似度</div>
            </div>
            <div class="stat-item">
                <div class="stat-value info">{len(pattern["dispersed_companies"])}</div>
                <div class="stat-label">分散节点数</div>
            </div>
            <div class="stat-item">
                <div class="stat-value info">{pattern["time_span_days"]}</div>
                <div class="stat-label">时间跨度(天)</div>
            </div>
        </div>
        
        <div class="main-content">
            <div class="sidebar">
                <div class="legend">
                    <h3>图例说明</h3>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #ff6b6b;"></div>
                        <span>核心公司 (Central)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #ffa502;"></div>
                        <span>分散节点 (Dispersed)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #a855f7;"></div>
                        <span>关联公司 (Related)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #00ff88;"></div>
                        <span>入口合同 (Contract)</span>
                    </div>
                    <div class="legend-item" style="margin-top: 15px; border-top: 1px solid rgba(255,255,255,0.1); padding-top: 10px;">
                        <span style="font-size: 0.85em; color: #8892b0;">边类型：</span>
                    </div>
                    <div class="legend-item">
                        <div style="width: 30px; height: 2px; background: #ff6b6b;"></div>
                        <span>资金流出 (OUTFLOW)</span>
                    </div>
                    <div class="legend-item">
                        <div style="width: 30px; height: 2px; background: #00ff88;"></div>
                        <span>资金流入 (INFLOW)</span>
                    </div>
                    <div class="legend-item">
                        <div style="width: 30px; height: 2px; background: #00d9ff;"></div>
                        <span>合同关系 (PARTY)</span>
                    </div>
                </div>
                
                <h3>节点列表</h3>
                <div class="node-list" id="node-list"></div>
            </div>
            
            <div class="graph-panel">
                <div class="graph-toolbar">
                    <h3>资金流向图谱</h3>
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
            edges: {edges_json},
            centralCompany: "{central_id}"
        }};
        
        const colorMap = {{
            'CentralCompany': '#ff6b6b',
            'DispersedCompany': '#ffa502',
            'RelatedCompany': '#a855f7',
            'Contract': '#00ff88'
        }};
        
        const edgeColorMap = {{
            'OUTFLOW': '#ff6b6b',
            'INFLOW': '#00ff88',
            'PARTY_A': '#00d9ff',
            'PARTY_B': '#00d9ff'
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
        
        // Arrow markers for different edge types
        const defs = svg.append('defs');
        ['OUTFLOW', 'INFLOW', 'PARTY_A', 'PARTY_B'].forEach(type => {{
            defs.append('marker')
                .attr('id', `arrow-${{type}}`)
                .attr('viewBox', '0 -5 10 10')
                .attr('refX', 28)
                .attr('refY', 0)
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .attr('orient', 'auto')
                .append('path')
                .attr('fill', edgeColorMap[type])
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
            .text(d => d.properties?.amount || d.type);
        
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
            .attr('r', d => d.id === graphData.centralCompany ? 28 : 20)
            .attr('fill', d => colorMap[d.type] || '#999')
            .attr('stroke', d => d.id === graphData.centralCompany ? '#fff' : 'rgba(255,255,255,0.3)')
            .attr('stroke-width', d => d.id === graphData.centralCompany ? 4 : 2);
        
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
            a.download = 'circular_trade_pattern.json';
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


def get_circular_trade_pattern_with_html(pattern: Dict, session) -> Dict:
    """
    获取循环交易模式并生成交互式HTML页面

    Args:
        pattern: 循环交易模式数据
        session: Nebula session

    Returns:
        dict: {
            "html_url": str,
            "pattern": dict
        }
    """
    html_path = generate_circular_trade_html(pattern, session)

    return {"html_url": html_path, "pattern": pattern}


def get_contract_parties(session, contract_id: str) -> List[str]:
    """
    获取合同的甲方和乙方公司ID
    
    Args:
        session: Nebula session
        contract_id: 合同ID
    
    Returns:
        list: 公司ID列表
    """
    query = f"""
    MATCH (c:Company)-[:PARTY_A|PARTY_B]->(con:Contract)
    WHERE id(con) == "{contract_id}"
    RETURN DISTINCT id(c) as company_id
    """
    rows = execute_query(session, query)
    return [row.get("company_id", "") for row in rows if row.get("company_id")]


def detect_circular_trade_by_contract(
    session,
    contract_id: str,
    time_window_days: int = 180,
    amount_threshold: float = 500000.0,
) -> Dict:
    """
    以合同为入口检测循环交易模式
    
    找到合同的甲/乙方公司，以这些公司为核心检测循环交易模式
    
    Args:
        session: Nebula session
        contract_id: 合同ID
        time_window_days: 时间窗口（天）
        amount_threshold: 金额阈值
    
    Returns:
        dict: {
            "contract_id": str,
            "parties": list,  # 合同相关方
            "patterns": list,  # 检测到的循环交易模式
            "html_url": str,  # HTML文件路径（如果有模式）
        }
    """
    # Step 1: 获取合同的甲/乙方
    parties = get_contract_parties(session, contract_id)
    
    if not parties:
        return {
            "contract_id": contract_id,
            "parties": [],
            "patterns": [],
            "html_url": None,
            "message": "未找到合同相关方"
        }
    
    # Step 2: 以这些公司为核心检测循环交易
    # 不传 company_ids 过滤，让算法检测所有交易，但只返回与这些公司相关的模式
    all_patterns = detect_fan_out_fan_in(
        session=session,
        time_window_days=time_window_days,
        amount_threshold=amount_threshold,
        company_ids=None,  # 检测全部
        periods=None,
    )
    
    # Step 3: 筛选出核心公司在 parties 中的模式
    relevant_patterns = [
        p for p in all_patterns
        if p["central_company"] in parties
    ]
    
    if not relevant_patterns:
        return {
            "contract_id": contract_id,
            "parties": parties,
            "patterns": [],
            "html_url": None,
            "message": "未检测到与该合同相关的循环交易模式"
        }
    
    # Step 4: 按风险分数排序，取最高风险的模式生成 HTML
    sorted_patterns = sorted(
        relevant_patterns, key=lambda x: x["risk_score"], reverse=True
    )
    top_pattern = sorted_patterns[0]
    
    # Step 5: 生成 HTML（传入 contract_id 以展示入口合同节点）
    html_path = generate_circular_trade_html(top_pattern, session, contract_id=contract_id)
    
    return {
        "contract_id": contract_id,
        "parties": parties,
        "patterns": sorted_patterns,
        "html_url": html_path,
        "message": f"检测到 {len(sorted_patterns)} 个相关循环交易模式"
    }


def main(
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
):
    """
    Main function for circular trade detection

    Args:
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）
    """
    print("=" * 70)
    print("高级循环交易检测 - 分散汇聚模式分析")
    print("=" * 70)

    if company_ids:
        print(f"  过滤公司: {len(company_ids)} 家")
    if periods:
        print(f"  时间范围: {periods}")

    session = None
    try:
        session = get_nebula_session()

        print("\n[1/3] 分析资金流向...")
        suspicious_patterns = detect_fan_out_fan_in(
            session,
            time_window_days=180,
            amount_threshold=500000,  # 50万以上
            company_ids=company_ids,
            periods=periods,
        )

        print(f"  发现可疑模式数: {len(suspicious_patterns)}")

        print("\n[2/3] 生成详细报告...")

        if len(suspicious_patterns) > 0:
            report_df = pd.DataFrame(suspicious_patterns)

            # 按风险分数排序
            report_df = report_df.sort_values("risk_score", ascending=False)

            # 处理列表字段以便保存到CSV
            report_df["dispersed_companies"] = report_df["dispersed_companies"].apply(
                lambda x: ", ".join(x[:5]) + ("..." if len(x) > 5 else "")
            )
            report_df["related_companies"] = report_df["related_companies"].apply(
                lambda x: ", ".join(x[:3]) + ("..." if len(x) > 3 else "")
            )

            # 确保报告目录存在
            os.makedirs(REPORTS_DIR, exist_ok=True)

            # 保存报告
            output_file = os.path.join(
                REPORTS_DIR, "circular_trade_detection_report.csv"
            )
            report_df.to_csv(output_file, index=False, encoding="utf-8-sig")

            print("\n[3/3] 前 5 高风险循环交易模式：\n")
            for idx, row in report_df.head(5).iterrows():
                print(f"模式 #{idx + 1}")
                print(f"  核心公司: {row['central_company']}")
                print(f"  分散节点: {row['dispersed_companies']}")
                print(f"  流出金额: ¥{row['total_outflow']:,.2f}")
                print(f"  流入金额: ¥{row['total_inflow']:,.2f}")
                print(f"  相似度: {row['similarity']:.2%}")
                print(f"  风险分数: {row['risk_score']:.4f}")
                print(f"  时间跨度: {row['time_span_days']} 天")
                print()

            print(f"完整报告已保存至: reports/circular_trade_detection_report.csv")
        else:
            print("\n[3/3] 未发现可疑的循环交易模式")

    finally:
        if session:
            session.release()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="高级循环交易检测")
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
    args = parser.parse_args()

    company_ids = args.company_ids.split(",") if args.company_ids else None
    periods = args.periods.split(",") if args.periods else None

    main(company_ids=company_ids, periods=periods)
