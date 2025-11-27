"""
场景：外部风险事件传导分析 (External Risk Rank)

基于 PageRank 算法，计算企业因行政处罚、经营异常等外部风险事件的风险传导分数
风险传导路径：AdminPenalty/BusinessAbnormal -> Company -> [CONTROLS/TRADES_WITH/...] -> Company
"""

import os
import pandas as pd
from collections import defaultdict
from typing import List, Optional
from src.utils.nebula_utils import get_nebula_session, execute_query
from src.utils.embedding import (
    compute_edge_weights,
    load_edge_weights,
    save_edge_weights,
)

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
CACHE_DIR = os.path.join(BASE_DIR, "cache")

# Edge weights for risk propagation
EDGE_WEIGHTS = {
    "CONTROLS": 0.85,  # Parent company strongly affected by subsidiary
    "LEGAL_PERSON": 0.75,
    "TRADES_WITH": 0.50,
    "IS_SUPPLIER": 0.45,
    "IS_CUSTOMER": 0.40,
    "ADMIN_PENALTY_OF": 0.90,  # Direct penalty impact
    "BUSINESS_ABNORMAL_OF": 0.70,  # Abnormal status impact
}


def calculate_admin_penalty_score(event):
    """
    Calculate risk score for administrative penalty event

    Args:
        event: dict with keys: amount, status, description

    Returns:
        float: 0-1 risk score
    """
    score = 0.0

    # Amount factor (normalized to 0-1, 100万 as upper limit)
    amount = float(event.get("amount", 0) or 0)
    amount_factor = min(amount / 1000000, 1.0)

    # Status factor
    status = event.get("status", "")
    status_factor = {
        "C": 0.7,  # Completed but still impacts credit
        "P": 0.9,  # Pending/Processing
    }.get(status, 0.6)

    # Severity from description
    description = event.get("description", "").lower()
    severity_factor = 0.5
    if "安全" in description or "safety" in description:
        severity_factor = 0.9
    elif "罚款" in description:
        severity_factor = 0.7
    elif "警告" in description or "通报批评" in description:
        severity_factor = 0.4

    score = 0.4 * amount_factor + 0.3 * status_factor + 0.3 * severity_factor
    return min(score, 1.0)


def calculate_business_abnormal_score(event):
    """
    Calculate risk score for business abnormal event

    Args:
        event: dict with keys: status, register_date, description

    Returns:
        float: 0-1 risk score
    """
    score = 0.0

    # Status factor - removed abnormal has lower risk
    status = event.get("status", "")
    if status == "C":  # Removed from abnormal list
        status_factor = 0.3
    else:  # Still in abnormal list
        status_factor = 0.9

    # Reason severity from description
    description = event.get("description", "")
    reason_factor = 0.5
    if "无法联系" in description or "住所" in description:
        reason_factor = 0.7
    elif "年度报告" in description:
        reason_factor = 0.4
    elif "弄虚作假" in description or "隐瞒" in description:
        reason_factor = 0.9

    score = 0.6 * status_factor + 0.4 * reason_factor
    return min(score, 1.0)


def load_weighted_graph(
    session,
    embedding_weights=None,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
):
    """
    Load graph data from Nebula Graph and build weighted adjacency list
    Focus on Company-to-Company propagation paths

    Args:
        session: Nebula Graph session
        embedding_weights: Pre-computed embedding weights dict, if None will use static weights
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）

    Returns:
        dict: graph structure with nodes, edges, out_degree
    """
    graph = {"nodes": set(), "edges": defaultdict(list), "out_degree": defaultdict(int)}

    if embedding_weights is None:
        embedding_weights = {}

    # Build company filter
    company_filter = ""
    if company_ids:
        ids_str = ", ".join([f"'{cid}'" for cid in company_ids])
        company_filter = f"WHERE c.Company.number IN [{ids_str}]"

    # Load Company nodes
    company_query = f"MATCH (c:Company) {company_filter} RETURN id(c) as company_id"
    companies = execute_query(session, company_query)
    for row in companies:
        company_id = row.get("company_id", "")
        if company_id:
            graph["nodes"].add(company_id)

    # CONTROLS edges (Company -> Company)
    controls_query = """
    MATCH (c1:Company)-[:CONTROLS]->(c2:Company)
    RETURN id(c1) as from_node, id(c2) as to_node
    """
    rows = execute_query(session, controls_query)
    for row in rows:
        from_node, to_node = row.get("from_node", ""), row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get(
                (from_node, to_node), EDGE_WEIGHTS["CONTROLS"]
            )
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # TRADES_WITH edges (Company -> Company)
    trades_query = """
    MATCH (c1:Company)-[:TRADES_WITH]->(c2:Company)
    RETURN id(c1) as from_node, id(c2) as to_node
    """
    rows = execute_query(session, trades_query)
    for row in rows:
        from_node, to_node = row.get("from_node", ""), row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get(
                (from_node, to_node), EDGE_WEIGHTS["TRADES_WITH"]
            )
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # IS_SUPPLIER edges
    supplier_query = """
    MATCH (c1:Company)-[:IS_SUPPLIER]->(c2:Company)
    RETURN id(c1) as from_node, id(c2) as to_node
    """
    rows = execute_query(session, supplier_query)
    for row in rows:
        from_node, to_node = row.get("from_node", ""), row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get(
                (from_node, to_node), EDGE_WEIGHTS["IS_SUPPLIER"]
            )
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # IS_CUSTOMER edges
    customer_query = """
    MATCH (c1:Company)-[:IS_CUSTOMER]->(c2:Company)
    RETURN id(c1) as from_node, id(c2) as to_node
    """
    rows = execute_query(session, customer_query)
    for row in rows:
        from_node, to_node = row.get("from_node", ""), row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get(
                (from_node, to_node), EDGE_WEIGHTS["IS_CUSTOMER"]
            )
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # LEGAL_PERSON edges (Person -> Company), for legal person risk propagation
    legal_person_query = """
    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
    RETURN id(p) as from_node, id(c) as to_node
    """
    rows = execute_query(session, legal_person_query)
    for row in rows:
        from_node, to_node = row.get("from_node", ""), row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get(
                (from_node, to_node), EDGE_WEIGHTS["LEGAL_PERSON"]
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
):
    """
    Initialize risk seeds from external risk events (AdminPenalty, BusinessAbnormal)
    Risk is directly assigned to companies linked to these events

    Args:
        session: Nebula Graph session
        risk_type: 'admin_penalty', 'business_abnormal', or 'all'
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围，按register_date过滤）

    Returns:
        dict: {company_id: init_score}
    """
    init_scores = defaultdict(float)
    risk_details = defaultdict(list)  # Store risk event details for reporting

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
                score = calculate_admin_penalty_score(event)
                init_scores[company_id] = max(init_scores[company_id], score)
                risk_details[company_id].append(
                    {
                        "type": "AdminPenalty",
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
                score = calculate_business_abnormal_score(event)
                # Combine scores if company has both penalty and abnormal
                init_scores[company_id] = max(init_scores[company_id], score)
                risk_details[company_id].append(
                    {
                        "type": "BusinessAbnormal",
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


def get_risk_level(score):
    """Risk level classification"""
    if score >= 0.6:
        return "高风险"
    elif score >= 0.3:
        return "中风险"
    elif score >= 0.1:
        return "低风险"
    else:
        return "正常"


def analyze_external_risk_results(
    risk_scores, risk_details, session, top_n=50, risk_type="all",
    company_ids: Optional[List[str]] = None,
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
    """
    # Only query companies that have risk scores to avoid unnecessary data transfer
    scored_company_ids = [cid for cid in risk_scores.keys() if "Company" in str(cid) or risk_scores[cid] > 0]
    
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

    report = []
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
                    "风险等级": get_risk_level(score),
                    "风险来源": "直接关联" if details else "传导",
                    "关联事件": risk_events,
                    "法人代表": info.get("legal_person", "N/A"),
                    "信用代码": info.get("credit_code", "N/A"),
                }
            )

    df_report = pd.DataFrame(report)

    os.makedirs(REPORTS_DIR, exist_ok=True)

    output_file = os.path.join(
        REPORTS_DIR, f"external_risk_rank_report_{risk_type}.csv"
    )
    df_report.to_csv(output_file, index=False, encoding="utf-8-sig")

    return df_report


def main(
    risk_type="all",
    use_cached_embedding=True,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
):
    """
    Main function for External Risk Rank analysis

    Args:
        risk_type: 'admin_penalty', 'business_abnormal', or 'all'
        use_cached_embedding: Whether to use cached embedding weights
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）
    """
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
            # Save to cache
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
        )
        seed_count = sum(1 for s in init_scores.values() if s > 0)
        print(f"  风险种子节点数: {seed_count}")
        if seed_count > 0:
            print(f"  平均初始分数: {sum(init_scores.values()) / seed_count:.4f}")

        # Step 4: Compute External Risk Rank
        print("\n[4/5] 计算 External Risk Rank（迭代中...）")
        risk_scores = compute_external_risk_rank(graph, init_scores, damping=0.85)

        # Step 5: Generate report
        print("\n[5/5] 生成分析报告...")
        report = analyze_external_risk_results(
            risk_scores, risk_details, session, top_n=50, risk_type=risk_type,
            company_ids=company_ids,
        )

        print("\n" + "=" * 60)
        print("分析完成！")
        print("=" * 60)

        if len(report) > 0:
            print(f"\n前 10 高风险公司：\n")
            print(report.head(10).to_string(index=False))
            print(
                f"\n完整报告已保存至: reports/external_risk_rank_report_{risk_type}.csv"
            )
        else:
            print("\n未发现高风险公司")

        return report

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
