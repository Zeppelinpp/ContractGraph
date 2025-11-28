"""
场景：外部风险事件传导分析 (External Risk Rank)

基于 PageRank 算法，计算企业因行政处罚、经营异常等外部风险事件的风险传导分数
风险传导路径：AdminPenalty/BusinessAbnormal -> Company -> [CONTROLS/TRADES_WITH/...] -> Company
"""

import os
import json
import pandas as pd
from collections import defaultdict
from typing import List, Optional, Dict
from src.utils.nebula_utils import get_nebula_session, execute_query
from src.utils.embedding import (
    compute_edge_weights,
    load_edge_weights,
    save_edge_weights,
)
from src.config.models import ExternalRiskRankConfig

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
CACHE_DIR = os.path.join(BASE_DIR, "cache")

DEFAULT_CONFIG = ExternalRiskRankConfig()


def calculate_admin_penalty_score(event, config: Optional[ExternalRiskRankConfig] = None):
    """
    Calculate risk score for administrative penalty event

    Args:
        event: dict with keys: amount, status, description
        config: Configuration object

    Returns:
        float: 0-1 risk score
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    weights = config.admin_penalty_weights
    status_weights = config.admin_penalty_status_weights
    amount_max = config.admin_penalty_amount_max

    # Amount factor (normalized to 0-1)
    amount = float(event.get("amount", 0) or 0)
    amount_factor = min(amount / amount_max, 1.0)

    # Status factor
    status = event.get("status", "")
    status_factor = status_weights.get(status, 0.6)

    # Severity from description
    description = event.get("description", "").lower()
    severity_factor = 0.5
    if "安全" in description or "safety" in description:
        severity_factor = 0.9
    elif "罚款" in description:
        severity_factor = 0.7
    elif "警告" in description or "通报批评" in description:
        severity_factor = 0.4

    score = (
        weights.get("amount", 0.4) * amount_factor +
        weights.get("status", 0.3) * status_factor +
        weights.get("severity", 0.3) * severity_factor
    )
    return min(score, 1.0)


def calculate_business_abnormal_score(event, config: Optional[ExternalRiskRankConfig] = None):
    """
    Calculate risk score for business abnormal event

    Args:
        event: dict with keys: status, register_date, description
        config: Configuration object

    Returns:
        float: 0-1 risk score
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    weights = config.business_abnormal_weights
    status_weights = config.business_abnormal_status_weights

    # Status factor - removed abnormal has lower risk
    status = event.get("status", "")
    status_factor = status_weights.get(status, 0.9)

    # Reason severity from description
    description = event.get("description", "")
    reason_factor = 0.5
    if "无法联系" in description or "住所" in description:
        reason_factor = 0.7
    elif "年度报告" in description:
        reason_factor = 0.4
    elif "弄虚作假" in description or "隐瞒" in description:
        reason_factor = 0.9

    score = (
        weights.get("status", 0.6) * status_factor +
        weights.get("reason", 0.4) * reason_factor
    )
    return min(score, 1.0)


def load_weighted_graph(
    session,
    embedding_weights=None,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
    config: Optional[ExternalRiskRankConfig] = None,
):
    """
    Load graph data from Nebula Graph and build weighted adjacency list
    Focus on Company-to-Company propagation paths

    Args:
        session: Nebula Graph session
        embedding_weights: Pre-computed embedding weights dict, if None will use static weights
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）
        config: Configuration object

    Returns:
        dict: graph structure with nodes, edges, out_degree
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    edge_weights = config.edge_weights
    graph = {"nodes": set(), "edges": defaultdict(list), "out_degree": defaultdict(int)}

    if embedding_weights is None:
        embedding_weights = {}

    # Build company filter
    company_filter = ""
    edge_filter = ""
    legal_person_filter = ""

    if company_ids:
        ids_str = ", ".join([f"'{cid}'" for cid in company_ids])
        company_filter = f"WHERE c.Company.number IN [{ids_str}]"
        edge_filter = f"WHERE c1.Company.number IN [{ids_str}] AND c2.Company.number IN [{ids_str}]"
        legal_person_filter = f"WHERE c.Company.number IN [{ids_str}]"

    # Load Company nodes
    company_query = f"MATCH (c:Company) {company_filter} RETURN id(c) as company_id"
    companies = execute_query(session, company_query)
    for row in companies:
        company_id = row.get("company_id", "")
        if company_id:
            graph["nodes"].add(company_id)

    # CONTROLS edges (Company -> Company)
    controls_query = f"""
    MATCH (c1:Company)-[:CONTROLS]->(c2:Company)
    {edge_filter}
    RETURN id(c1) as from_node, id(c2) as to_node
    """
    rows = execute_query(session, controls_query)
    for row in rows:
        from_node, to_node = row.get("from_node", ""), row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get(
                (from_node, to_node), edge_weights.get("CONTROLS", 0.85)
            )
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # TRADES_WITH edges (Company -> Company)
    trades_query = f"""
    MATCH (c1:Company)-[:TRADES_WITH]->(c2:Company)
    {edge_filter}
    RETURN id(c1) as from_node, id(c2) as to_node
    """
    rows = execute_query(session, trades_query)
    for row in rows:
        from_node, to_node = row.get("from_node", ""), row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get(
                (from_node, to_node), edge_weights.get("TRADES_WITH", 0.50)
            )
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # IS_SUPPLIER edges
    supplier_query = f"""
    MATCH (c1:Company)-[:IS_SUPPLIER]->(c2:Company)
    {edge_filter}
    RETURN id(c1) as from_node, id(c2) as to_node
    """
    rows = execute_query(session, supplier_query)
    for row in rows:
        from_node, to_node = row.get("from_node", ""), row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get(
                (from_node, to_node), edge_weights.get("IS_SUPPLIER", 0.45)
            )
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # IS_CUSTOMER edges
    customer_query = f"""
    MATCH (c1:Company)-[:IS_CUSTOMER]->(c2:Company)
    {edge_filter}
    RETURN id(c1) as from_node, id(c2) as to_node
    """
    rows = execute_query(session, customer_query)
    for row in rows:
        from_node, to_node = row.get("from_node", ""), row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get(
                (from_node, to_node), edge_weights.get("IS_CUSTOMER", 0.40)
            )
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # LEGAL_PERSON edges (Person -> Company)
    legal_person_query = f"""
    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
    {legal_person_filter}
    RETURN id(p) as from_node, id(c) as to_node
    """
    rows = execute_query(session, legal_person_query)
    for row in rows:
        from_node, to_node = row.get("from_node", ""), row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get(
                (from_node, to_node), edge_weights.get("LEGAL_PERSON", 0.75)
            )
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    return graph


def initialize_external_risk_seeds(
    session,
    risk_type="all",
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
    config: Optional[ExternalRiskRankConfig] = None,
):
    """
    Initialize risk seeds from external risk events (AdminPenalty, BusinessAbnormal)
    Risk is directly assigned to companies linked to these events

    Args:
        session: Nebula Graph session
        risk_type: 'admin_penalty', 'business_abnormal', or 'all'
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围，按register_date过滤）
        config: Configuration object

    Returns:
        dict: {company_id: init_score}, dict: {company_id: risk_details}
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    init_scores = defaultdict(float)
    risk_details = defaultdict(list)

    # Build filters
    where_clauses = []
    if company_ids:
        ids_str = ", ".join([f"'{cid}'" for cid in company_ids])
        where_clauses.append(f"c.Company.number IN [{ids_str}]")

    # AdminPenalty -> Company
    if risk_type in ["admin_penalty", "all"]:
        penalty_where = list(where_clauses)
        if periods:
            if len(periods) == 1:
                penalty_where.append(f"pen.AdminPenalty.register_date == '{periods[0]}'")
            elif len(periods) == 2:
                penalty_where.append(f"pen.AdminPenalty.register_date >= '{periods[0]}' AND pen.AdminPenalty.register_date <= '{periods[1]}'")
        
        penalty_filter = f"WHERE {' AND '.join(penalty_where)}" if penalty_where else ""
        penalty_query = f"""
        MATCH (pen:AdminPenalty)-[:ADMIN_PENALTY_OF]->(c:Company)
        {penalty_filter}
        RETURN id(c) as company_id, id(pen) as event_id,
               pen.AdminPenalty.amount as amount,
               pen.AdminPenalty.status as status,
               pen.AdminPenalty.event_no as event_no,
               pen.AdminPenalty.description as description
        """
        rows = execute_query(session, penalty_query)
        for row in rows:
            company_id = row.get("company_id", "")
            event_id = row.get("event_id", "")
            if company_id and event_id:
                event = {
                    "amount": row.get("amount", 0),
                    "status": row.get("status", ""),
                    "description": row.get("description", ""),
                }
                score = calculate_admin_penalty_score(event, config)
                init_scores[company_id] = max(init_scores[company_id], score)
                risk_details[company_id].append(
                    {
                        "type": "AdminPenalty",
                        "event_id": event_id,
                        "event_no": row.get("event_no", ""),
                        "score": score,
                    }
                )

    # BusinessAbnormal -> Company
    if risk_type in ["business_abnormal", "all"]:
        abnormal_where = list(where_clauses)
        if periods:
            if len(periods) == 1:
                abnormal_where.append(f"abn.BusinessAbnormal.register_date == '{periods[0]}'")
            elif len(periods) == 2:
                abnormal_where.append(f"abn.BusinessAbnormal.register_date >= '{periods[0]}' AND abn.BusinessAbnormal.register_date <= '{periods[1]}'")
        
        abnormal_filter = f"WHERE {' AND '.join(abnormal_where)}" if abnormal_where else ""
        abnormal_query = f"""
        MATCH (abn:BusinessAbnormal)-[:BUSINESS_ABNORMAL_OF]->(c:Company)
        {abnormal_filter}
        RETURN id(c) as company_id, id(abn) as event_id,
               abn.BusinessAbnormal.status as status,
               abn.BusinessAbnormal.register_date as register_date,
               abn.BusinessAbnormal.event_no as event_no,
               abn.BusinessAbnormal.description as description
        """
        rows = execute_query(session, abnormal_query)
        for row in rows:
            company_id = row.get("company_id", "")
            event_id = row.get("event_id", "")
            if company_id and event_id:
                event = {
                    "status": row.get("status", ""),
                    "register_date": row.get("register_date", ""),
                    "description": row.get("description", ""),
                }
                score = calculate_business_abnormal_score(event, config)
                init_scores[company_id] = max(init_scores[company_id], score)
                risk_details[company_id].append(
                    {
                        "type": "BusinessAbnormal",
                        "event_id": event_id,
                        "event_no": row.get("event_no", ""),
                        "score": score,
                    }
                )

    return dict(init_scores), dict(risk_details)


def compute_external_risk_rank(
    graph, init_scores, damping=0.85, max_iter=100, tolerance=1e-6
):
    """
    Compute External Risk Rank scores using PageRank-like algorithm

    Args:
        graph: Graph data structure
        init_scores: dict {node_id: init_score}
        damping: Damping factor
        max_iter: Maximum iterations
        tolerance: Convergence threshold

    Returns:
        dict: {node_id: risk_score}
    """
    scores = {node: init_scores.get(node, 0.0) for node in graph["nodes"]}

    for iteration in range(max_iter):
        new_scores = {}
        max_diff = 0.0

        for node in graph["nodes"]:
            base_score = (1 - damping) * init_scores.get(node, 0.0)

            propagated_score = 0.0
            for neighbor, neighbors_list in graph["edges"].items():
                for target, weight in neighbors_list:
                    if target == node:
                        out_deg = graph["out_degree"][neighbor]
                        if out_deg > 0:
                            propagated_score += weight * scores[neighbor] / out_deg

            new_scores[node] = base_score + damping * propagated_score
            max_diff = max(max_diff, abs(new_scores[node] - scores[node]))

        scores = new_scores

        if max_diff < tolerance:
            print(f"  Converged at iteration {iteration + 1}")
            break

    return scores


def get_risk_level(score, config: Optional[ExternalRiskRankConfig] = None):
    """Risk level classification"""
    if config is None:
        config = DEFAULT_CONFIG
    
    thresholds = config.risk_level_thresholds
    if score >= thresholds.get("high", 0.6):
        return "高风险"
    elif score >= thresholds.get("medium", 0.3):
        return "中风险"
    elif score >= thresholds.get("low", 0.1):
        return "低风险"
    else:
        return "正常"


def analyze_external_risk_results(
    risk_scores, risk_details, session, top_n=50, risk_type="all",
    company_ids: Optional[List[str]] = None,
    config: Optional[ExternalRiskRankConfig] = None,
):
    """
    Analyze External Risk Rank results and generate report
    
    Args:
        risk_scores: dict {node_id: risk_score}
        risk_details: dict {node_id: list of risk event details}
        session: Nebula Graph session
        top_n: Number of top results to include
        risk_type: Risk type for report naming
        company_ids: Company IDs filter (by Company.number)
        config: Configuration object
    
    Returns:
        dict: {
            "company_report": DataFrame,
            "contract_ids": List of contract IDs sorted by risk score
        }
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    # Build filter consistent with load_weighted_graph
    company_filter = ""
    if company_ids:
        ids_str = ", ".join([f"'{cid}'" for cid in company_ids])
        company_filter = f"WHERE c.Company.number IN [{ids_str}]"
    
    company_query = f"""
    MATCH (c:Company)
    {company_filter}
    RETURN id(c) as company_id, c.Company.name as name,
           c.Company.legal_person as legal_person,
           c.Company.credit_code as credit_code
    """
    companies = execute_query(session, company_query)

    company_info = {}
    for row in companies:
        company_id = row.get("company_id", "")
        if company_id:
            company_info[company_id] = {
                "name": row.get("name", "Unknown"),
                "legal_person": row.get("legal_person", "N/A"),
                "credit_code": row.get("credit_code", "N/A"),
            }

    sorted_scores = sorted(risk_scores.items(), key=lambda x: x[1], reverse=True)

    # Build company report
    report = []
    risk_company_ids = set()
    for node_id, score in sorted_scores[:top_n]:
        if node_id in company_info:
            info = company_info[node_id]
            details = risk_details.get(node_id, [])
            risk_events = (
                "; ".join([f"{d['type']}({d['event_no'][:20]}...)" for d in details])
                if details
                else "传导风险"
            )

            report.append(
                {
                    "公司ID": node_id,
                    "公司名称": info.get("name", "Unknown"),
                    "风险分数": round(score, 4),
                    "风险等级": get_risk_level(score, config),
                    "风险来源": "直接关联" if details else "传导",
                    "关联事件": risk_events,
                    "法人代表": info.get("legal_person", "N/A"),
                    "信用代码": info.get("credit_code", "N/A"),
                }
            )
            risk_company_ids.add(node_id)

    df_report = pd.DataFrame(report)

    # Get contracts related to risk companies (sorted by company risk score)
    contract_ids = []
    if risk_company_ids:
        company_scores = {node_id: score for node_id, score in sorted_scores if node_id in risk_company_ids}
        
        contract_query = """
        MATCH (c:Company)-[:PARTY_A|PARTY_B]->(con:Contract)
        RETURN id(c) as company_id, id(con) as contract_id
        """
        contract_rows = execute_query(session, contract_query)
        
        # Build contract -> max company risk score mapping
        contract_risk = {}
        for row in contract_rows:
            company_id = row.get("company_id", "")
            contract_id = row.get("contract_id", "")
            if company_id in company_scores and contract_id:
                current_score = contract_risk.get(contract_id, 0)
                contract_risk[contract_id] = max(current_score, company_scores[company_id])
        
        # Sort contracts by risk score descending
        contract_ids = [cid for cid, _ in sorted(contract_risk.items(), key=lambda x: x[1], reverse=True)]

    os.makedirs(REPORTS_DIR, exist_ok=True)

    output_file = os.path.join(
        REPORTS_DIR, f"external_risk_rank_report_{risk_type}.csv"
    )
    df_report.to_csv(output_file, index=False, encoding="utf-8-sig")

    return {
        "company_report": df_report,
        "contract_ids": contract_ids,
    }


def get_external_risk_subgraph(
    session,
    contract_id: str,
    max_depth: int = 2,
    risk_type: str = "all",
    config: Optional[ExternalRiskRankConfig] = None,
) -> Dict:
    """
    获取外部风险子图
    
    输入contract id -> 与 contract 的存在经营异常/行政处罚的相对方 ->
    与相对方相关的行政处罚和经营异常节点 -> 这些相对方涉及的合同 如此递归
    
    Args:
        session: Nebula Graph session
        contract_id: 合同ID
        max_depth: 递归深度
        risk_type: 风险类型 'admin_penalty', 'business_abnormal', 'all'
        config: Configuration object
    
    Returns:
        Dict with subgraph data and HTML path
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    nodes = []
    edges = []
    node_ids = set()
    visited_companies = set()
    visited_contracts = set()
    all_contract_ids = []
    
    def add_node(node_id: str, node_type: str, label: str, properties: dict):
        if node_id not in node_ids:
            nodes.append({
                "id": node_id,
                "type": node_type,
                "label": label[:25] if label else node_id[:15],
                "properties": properties,
            })
            node_ids.add(node_id)
    
    def add_edge(source: str, target: str, edge_type: str, properties: dict = None):
        edges.append({
            "source": source,
            "target": target,
            "type": edge_type,
            "properties": properties or {},
        })
    
    def explore_contract(con_id: str, depth: int):
        """Recursively explore contract and its risk relationships"""
        if depth > max_depth or con_id in visited_contracts:
            return
        visited_contracts.add(con_id)
        
        # Get contract info
        con_query = f"""
        MATCH (con:Contract)
        WHERE id(con) == '{con_id}'
        RETURN id(con) as contract_id,
               con.Contract.contract_no as contract_no,
               con.Contract.contract_name as contract_name,
               con.Contract.amount as amount
        """
        con_results = execute_query(session, con_query)
        if con_results:
            row = con_results[0]
            add_node(
                con_id, "Contract",
                row.get("contract_name", con_id),
                {
                    "contract_no": row.get("contract_no", ""),
                    "contract_name": row.get("contract_name", ""),
                    "amount": row.get("amount", 0),
                    "depth": depth,
                }
            )
            if con_id not in all_contract_ids:
                all_contract_ids.append(con_id)
        
        # Get parties of the contract
        party_query = f"""
        MATCH (c:Company)-[e:PARTY_A|PARTY_B]->(con:Contract)
        WHERE id(con) == '{con_id}'
        RETURN id(c) as company_id,
               c.Company.name as company_name,
               c.Company.credit_code as credit_code,
               type(e) as edge_type
        """
        party_results = execute_query(session, party_query)
        
        for row in party_results:
            company_id = row.get("company_id", "")
            if not company_id:
                continue
            
            add_node(
                company_id, "Company",
                row.get("company_name", company_id),
                {
                    "name": row.get("company_name", ""),
                    "credit_code": row.get("credit_code", ""),
                }
            )
            add_edge(company_id, con_id, row.get("edge_type", "PARTY"))
            
            # Explore company's risk events
            if company_id not in visited_companies:
                visited_companies.add(company_id)
                explore_company_risks(company_id, con_id, depth)
    
    def explore_company_risks(company_id: str, source_contract_id: str, depth: int):
        """Explore risk events and other contracts of a company"""
        has_risk = False
        
        # Get AdminPenalty events
        if risk_type in ["admin_penalty", "all"]:
            penalty_query = f"""
            MATCH (pen:AdminPenalty)-[:ADMIN_PENALTY_OF]->(c:Company)
            WHERE id(c) == '{company_id}'
            RETURN id(pen) as event_id,
                   pen.AdminPenalty.event_no as event_no,
                   pen.AdminPenalty.description as description,
                   pen.AdminPenalty.amount as amount,
                   pen.AdminPenalty.status as status
            """
            penalty_results = execute_query(session, penalty_query)
            for row in penalty_results:
                event_id = row.get("event_id", "")
                if event_id:
                    has_risk = True
                    event = {
                        "amount": row.get("amount", 0),
                        "status": row.get("status", ""),
                        "description": row.get("description", ""),
                    }
                    score = calculate_admin_penalty_score(event, config)
                    add_node(
                        event_id, "AdminPenalty",
                        f"行政处罚-{row.get('event_no', '')[:10]}",
                        {
                            "event_no": row.get("event_no", ""),
                            "description": row.get("description", ""),
                            "amount": row.get("amount", 0),
                            "risk_score": round(score, 4),
                        }
                    )
                    add_edge(event_id, company_id, "ADMIN_PENALTY_OF")
        
        # Get BusinessAbnormal events
        if risk_type in ["business_abnormal", "all"]:
            abnormal_query = f"""
            MATCH (abn:BusinessAbnormal)-[:BUSINESS_ABNORMAL_OF]->(c:Company)
            WHERE id(c) == '{company_id}'
            RETURN id(abn) as event_id,
                   abn.BusinessAbnormal.event_no as event_no,
                   abn.BusinessAbnormal.description as description,
                   abn.BusinessAbnormal.status as status
            """
            abnormal_results = execute_query(session, abnormal_query)
            for row in abnormal_results:
                event_id = row.get("event_id", "")
                if event_id:
                    has_risk = True
                    event = {
                        "status": row.get("status", ""),
                        "description": row.get("description", ""),
                    }
                    score = calculate_business_abnormal_score(event, config)
                    add_node(
                        event_id, "BusinessAbnormal",
                        f"经营异常-{row.get('event_no', '')[:10]}",
                        {
                            "event_no": row.get("event_no", ""),
                            "description": row.get("description", ""),
                            "risk_score": round(score, 4),
                        }
                    )
                    add_edge(event_id, company_id, "BUSINESS_ABNORMAL_OF")
        
        # If company has risk events, explore its other contracts recursively
        if has_risk and depth < max_depth:
            other_contracts_query = f"""
            MATCH (c:Company)-[:PARTY_A|PARTY_B]->(con:Contract)
            WHERE id(c) == '{company_id}'
            RETURN DISTINCT id(con) as contract_id
            """
            other_contracts = execute_query(session, other_contracts_query)
            for row in other_contracts:
                other_con_id = row.get("contract_id", "")
                if other_con_id and other_con_id != source_contract_id:
                    explore_contract(other_con_id, depth + 1)
    
    # Start exploration from the input contract
    explore_contract(contract_id, 0)
    
    # Generate HTML visualization
    html_path = generate_external_risk_subgraph_html(
        contract_id=contract_id,
        nodes=nodes,
        edges=edges,
        max_depth=max_depth,
    )
    
    # Count risk events
    risk_event_count = sum(1 for n in nodes if n["type"] in ["AdminPenalty", "BusinessAbnormal"])
    company_count = sum(1 for n in nodes if n["type"] == "Company")
    
    return {
        "success": True,
        "contract_id": contract_id,
        "nodes": nodes,
        "edges": edges,
        "html_url": html_path,
        "max_depth": max_depth,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "company_count": company_count,
        "risk_event_count": risk_event_count,
        "contract_ids": all_contract_ids,
    }


def generate_external_risk_subgraph_html(
    contract_id: str,
    nodes: List[Dict],
    edges: List[Dict],
    max_depth: int,
) -> str:
    """
    生成外部风险子图的交互式HTML页面
    """
    safe_id = contract_id.replace('"', '').replace("'", "").replace("/", "_")
    output_filename = f"external_risk_subgraph_{safe_id}.html"
    
    os.makedirs(REPORTS_DIR, exist_ok=True)
    output_path = os.path.join(REPORTS_DIR, output_filename)
    
    nodes_json = json.dumps(nodes, ensure_ascii=False)
    edges_json = json.dumps(edges, ensure_ascii=False)
    
    # Count by type
    contract_count = sum(1 for n in nodes if n["type"] == "Contract")
    company_count = sum(1 for n in nodes if n["type"] == "Company")
    penalty_count = sum(1 for n in nodes if n["type"] == "AdminPenalty")
    abnormal_count = sum(1 for n in nodes if n["type"] == "BusinessAbnormal")
    
    html_content = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>外部风险传导子图 - {contract_id}</title>
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
        
        .stat-value.success {{
            color: #00ff88;
        }}
        
        .stat-value.purple {{
            color: #a855f7;
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
            <h1>⚠️ 外部风险传导分析子图</h1>
            <p>合同关联的行政处罚与经营异常风险传导 | 递归深度: {max_depth}</p>
        </header>
        
        <div class="stats-bar">
            <div class="stat-item">
                <div class="stat-value success">{contract_count}</div>
                <div class="stat-label">合同</div>
            </div>
            <div class="stat-item">
                <div class="stat-value purple">{company_count}</div>
                <div class="stat-label">公司</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{penalty_count}</div>
                <div class="stat-label">行政处罚</div>
            </div>
            <div class="stat-item">
                <div class="stat-value warning">{abnormal_count}</div>
                <div class="stat-label">经营异常</div>
            </div>
            <div class="stat-item">
                <div class="stat-value info">{len(edges)}</div>
                <div class="stat-label">关系数</div>
            </div>
        </div>
        
        <div class="main-content">
            <div class="sidebar">
                <div class="legend">
                    <h3>图例说明</h3>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #00ff88;"></div>
                        <span>合同 (Contract)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #a855f7;"></div>
                        <span>公司 (Company)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #ff6b6b;"></div>
                        <span>行政处罚 (AdminPenalty)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #ffa500;"></div>
                        <span>经营异常 (BusinessAbnormal)</span>
                    </div>
                    <div class="legend-item" style="margin-top: 15px; border-top: 1px solid rgba(255,255,255,0.1); padding-top: 10px;">
                        <span style="font-size: 0.85em; color: #8892b0;">边类型：</span>
                    </div>
                    <div class="legend-item">
                        <div style="width: 30px; height: 2px; background: #ff6b6b;"></div>
                        <span>风险关联 (RISK)</span>
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
                    <h3>风险传导图谱</h3>
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
            'Contract': '#00ff88',
            'Company': '#a855f7',
            'AdminPenalty': '#ff6b6b',
            'BusinessAbnormal': '#ffa500'
        }};
        
        const sizeMap = {{
            'Contract': 24,
            'Company': 20,
            'AdminPenalty': 16,
            'BusinessAbnormal': 16
        }};
        
        const edgeColorMap = {{
            'ADMIN_PENALTY_OF': '#ff6b6b',
            'BUSINESS_ABNORMAL_OF': '#ffa500',
            'PARTY_A': '#00d9ff',
            'PARTY_B': '#00d9ff',
            'PARTY': '#00d9ff'
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
            .force('link', d3.forceLink(links).id(d => d.id).distance(120))
            .force('charge', d3.forceManyBody().strength(-400))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(d => sizeMap[d.type] + 15));
        
        // Arrow markers
        const defs = svg.append('defs');
        Object.keys(edgeColorMap).forEach(type => {{
            defs.append('marker')
                .attr('id', `arrow-${{type}}`)
                .attr('viewBox', '0 -5 10 10')
                .attr('refX', 28)
                .attr('refY', 0)
                .attr('markerWidth', 6)
                .attr('markerHeight', 6)
                .attr('orient', 'auto')
                .append('path')
                .attr('fill', edgeColorMap[type] || '#666')
                .attr('d', 'M0,-5L10,0L0,5');
        }});
        
        const link = g.append('g')
            .selectAll('line')
            .data(links)
            .join('line')
            .attr('class', 'link')
            .attr('stroke', d => edgeColorMap[d.type] || '#666')
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
            .attr('r', d => sizeMap[d.type] || 18)
            .attr('fill', d => colorMap[d.type] || '#888')
            .attr('stroke', 'rgba(255,255,255,0.3)')
            .attr('stroke-width', 2);
        
        node.append('text')
            .attr('dy', d => sizeMap[d.type] + 15)
            .attr('text-anchor', 'middle')
            .text(d => d.label.length > 12 ? d.label.substring(0, 12) + '...' : d.label);
        
        const tooltip = d3.select('#tooltip');
        
        node.on('mouseover', (event, d) => {{
            let html = `<h4>${{d.label}}</h4>`;
            html += `<div class="tooltip-row"><span class="tooltip-key">类型</span><span class="tooltip-value">${{d.type}}</span></div>`;
            html += `<div class="tooltip-row"><span class="tooltip-key">ID</span><span class="tooltip-value">${{d.id}}</span></div>`;
            
            if (d.properties) {{
                for (const [key, value] of Object.entries(d.properties)) {{
                    if (value !== null && value !== undefined && value !== '') {{
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
                    if (value !== null && value !== undefined && value !== '') {{
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
            a.download = 'external_risk_subgraph.json';
            a.click();
            URL.revokeObjectURL(url);
        }}
    </script>
</body>
</html>
'''
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    return output_path


def main(
    risk_type="all",
    use_cached_embedding=True,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
    config: Optional[ExternalRiskRankConfig] = None,
):
    """
    Main function for External Risk Rank analysis

    Args:
        risk_type: 'admin_penalty', 'business_abnormal', or 'all'
        use_cached_embedding: Whether to use cached embedding weights
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）
        config: Configuration object
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    print("=" * 60)
    print(f"外部风险事件传导分析 (External Risk Rank)")
    print(f"风险类型: {risk_type}")
    print("=" * 60)
    
    if company_ids:
        print(f"  过滤公司: {len(company_ids)} 家")
    if periods:
        print(f"  时间范围: {periods}")

    session = None
    try:
        session = get_nebula_session()

        # Step 1: Load or compute embedding weights
        print("\n[1/5] 加载边权重...")
        embedding_weights = None
        cache_file = os.path.join(CACHE_DIR, "edge_weights.json")

        if use_cached_embedding:
            embedding_weights = load_edge_weights(cache_file)
            if embedding_weights:
                print(f"  从缓存加载 {len(embedding_weights)} 条边权重")

        if embedding_weights is None:
            print("  计算 embedding 边权重...")
            embedding_weights = compute_edge_weights(session=session, limit=10000)
            print(f"  已计算 {len(embedding_weights)} 条边的动态权重")
            os.makedirs(CACHE_DIR, exist_ok=True)
            save_edge_weights(embedding_weights, cache_file)
            print(f"  已保存边权重到缓存: {cache_file}")

        # Step 2: Load graph data
        print("\n[2/5] 加载图数据...")
        graph = load_weighted_graph(
            session,
            embedding_weights,
            company_ids=company_ids,
            periods=periods,
            config=config,
        )
        print(f"  节点数: {len(graph['nodes'])}")
        print(f"  边数: {sum(len(v) for v in graph['edges'].values())}")

        # Step 3: Initialize risk seeds
        print("\n[3/5] 初始化外部风险种子节点...")
        init_scores, risk_details = initialize_external_risk_seeds(
            session,
            risk_type,
            company_ids=company_ids,
            periods=periods,
            config=config,
        )
        seed_count = sum(1 for s in init_scores.values() if s > 0)
        print(f"  风险种子节点数: {seed_count}")
        if seed_count > 0:
            print(f"  平均初始分数: {sum(init_scores.values()) / seed_count:.4f}")

        # Step 4: Compute External Risk Rank
        print("\n[4/5] 计算 External Risk Rank（迭代中...）")
        risk_scores = compute_external_risk_rank(
            graph, init_scores, damping=config.damping
        )

        # Step 5: Generate report
        print("\n[5/5] 生成分析报告...")
        result = analyze_external_risk_results(
            risk_scores, risk_details, session, top_n=50, risk_type=risk_type,
            company_ids=company_ids, config=config,
        )

        print("\n" + "=" * 60)
        print("分析完成！")
        print("=" * 60)

        report = result.get("company_report")
        contract_ids = result.get("contract_ids", [])
        
        if len(report) > 0:
            print(f"\n前 10 高风险公司：\n")
            print(report.head(10).to_string(index=False))
            print(
                f"\n完整报告已保存至: reports/external_risk_rank_report_{risk_type}.csv"
            )
            print(f"\n关联风险合同数量: {len(contract_ids)}")
        else:
            print("\n未发现高风险公司")

        return result

    finally:
        if session:
            session.release()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="外部风险事件传导分析")
    parser.add_argument(
        "--risk-type",
        type=str,
        choices=["admin_penalty", "business_abnormal", "all"],
        default="all",
        help="风险类型: admin_penalty(行政处罚), business_abnormal(经营异常), all(全部)",
    )
    parser.add_argument(
        "--no-cache", action="store_true", help="不使用缓存的 embedding 权重，重新计算"
    )
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

    main(
        risk_type=args.risk_type,
        use_cached_embedding=not args.no_cache,
        company_ids=company_ids,
        periods=periods,
    )
