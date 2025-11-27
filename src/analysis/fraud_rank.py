"""
场景一：FraudRank 欺诈风险传导分析

基于PageRank算法，计算企业的欺诈风险传导分数
"""

import os
import pandas as pd
from typing import List
from collections import defaultdict
from typing import Optional
from src.utils.nebula_utils import get_nebula_session, execute_query
from src.utils.embedding import get_or_compute_edge_weights
from src.config.models import FraudRankConfig

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
CACHE_DIR = os.path.join(BASE_DIR, "cache")

# 默认配置实例
DEFAULT_CONFIG = FraudRankConfig()


def calculate_init_score(
    company_id, legal_events, config: Optional[FraudRankConfig] = None
):
    """
    根据涉及的法律事件计算初始风险分数

    Args:
        company_id: 公司ID
        legal_events: 该公司涉及的法律事件列表
        config: FraudRank 配置对象，默认使用 DEFAULT_CONFIG

    Returns:
        float: 0-1 之间的初始风险分数
    """
    if not legal_events:
        return 0.0

    if config is None:
        config = DEFAULT_CONFIG

    score = 0.0
    for event in legal_events:
        # 事件类型权重
        type_weight = config.event_type_weights.get(
            event["event_type"], config.event_type_default_weight
        )

        # 金额权重（归一化到 0-1）
        amount_weight = min(event["amount"] / config.amount_threshold, 1.0)

        # 状态权重
        status_weight = config.status_weights.get(
            event["status"], config.status_default_weight
        )

        score += type_weight * amount_weight * status_weight

    return min(score, 1.0)


def load_weighted_graph(
    session,
    use_embedding_weights=True,
    force_recompute=False,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
    config: Optional[FraudRankConfig] = None,
):
    """
    从 Nebula Graph 加载图数据并构建加权邻接表

    Args:
        session: Nebula Graph session
        use_embedding_weights: 是否使用 embedding 计算的动态权重，默认 True
        force_recompute: 是否强制重新计算 embedding 权重
        config: FraudRank 配置对象，默认使用 DEFAULT_CONFIG
        company_ids: 公司ID列表
        periods: 时间段列表

    Returns:
        dict: {
            'nodes': set of node_ids,
            'edges': defaultdict(list),  # node_id -> [(neighbor_id, weight), ...]
            'out_degree': defaultdict(int)
        }
    """
    if config is None:
        config = DEFAULT_CONFIG

    edge_weights = config.edge_weights
    graph = {"nodes": set(), "edges": defaultdict(list), "out_degree": defaultdict(int)}

    # 如果使用 embedding 权重，从缓存加载或计算
    embedding_weights = {}
    if use_embedding_weights:
        print("  加载/计算 embedding 边权重...")
        embedding_weights = get_or_compute_edge_weights(
            session=session,
            cache_dir=CACHE_DIR,
            limit=10000,
            force_recompute=force_recompute,
        )
        print(f"  已加载 {len(embedding_weights)} 条边的动态权重")

    if company_ids:
        ids_filter = ', '.join([f"'{company_id}'" for company_id in company_ids])
        company_query = f"""
        MATCH (c:Company)
        WHERE c.Company.number IN [{ids_filter}]
        RETURN id(c) as company_id
        """
    else:
        company_query = """
        MATCH (c:Company)
        RETURN id(c) as company_id
        """
    companies = execute_query(session, company_query)
    for row in companies:
        company_id = row.get("company_id", "")
        if company_id:
            graph["nodes"].add(company_id)

    # 查询 CONTROLS 边
    controls_query = """
    MATCH (c1:Company)-[:CONTROLS]->(c2:Company)
    RETURN id(c1) as from_node, id(c2) as to_node
    """
    rows = execute_query(session, controls_query)
    for row in rows:
        from_node = row.get("from_node", "")
        to_node = row.get("to_node", "")
        if from_node and to_node:
            # 优先使用 embedding 权重，否则使用静态权重
            weight = embedding_weights.get((from_node, to_node))
            if weight is None:
                weight = edge_weights.get("CONTROLS", 0.3)
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # 查询 LEGAL_PERSON 边（Person -> Company）
    legal_person_query = """
    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
    RETURN id(p) as from_node, id(c) as to_node
    """
    rows = execute_query(session, legal_person_query)
    for row in rows:
        from_node = row.get("from_node", "")
        to_node = row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get((from_node, to_node))
            if weight is None:
                weight = edge_weights.get("LEGAL_PERSON", 0.3)
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # 查询 TRADES_WITH 边
    trades_query = """
    MATCH (c1:Company)-[:TRADES_WITH]->(c2:Company)
    RETURN id(c1) as from_node, id(c2) as to_node
    """
    rows = execute_query(session, trades_query)
    for row in rows:
        from_node = row.get("from_node", "")
        to_node = row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get((from_node, to_node))
            if weight is None:
                weight = edge_weights.get("TRADES_WITH", 0.3)
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # 查询 IS_SUPPLIER 边
    supplier_query = """
    MATCH (c1:Company)-[:IS_SUPPLIER]->(c2:Company)
    RETURN id(c1) as from_node, id(c2) as to_node
    """
    rows = execute_query(session, supplier_query)
    for row in rows:
        from_node = row.get("from_node", "")
        to_node = row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get((from_node, to_node))
            if weight is None:
                weight = edge_weights.get("IS_SUPPLIER", 0.3)
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # 查询 IS_CUSTOMER 边
    customer_query = """
    MATCH (c1:Company)-[:IS_CUSTOMER]->(c2:Company)
    RETURN id(c1) as from_node, id(c2) as to_node
    """
    rows = execute_query(session, customer_query)
    for row in rows:
        from_node = row.get("from_node", "")
        to_node = row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get((from_node, to_node))
            if weight is None:
                weight = edge_weights.get("IS_CUSTOMER", 0.3)
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # 查询 PAYS 边（Company -> Transaction）
    periods_filter = ""
    if periods:
        if len(periods) == 1:
            periods_filter = f"WHERE t.Transaction.transaction_date == '{periods[0]}'"
        elif len(periods) == 2:
            periods_filter = f"WHERE t.Transaction.transaction_date >= '{periods[0]}' AND t.Transaction.transaction_date <= '{periods[1]}'"
        else:
            raise ValueError("时间段列表长度必须为1或2")
    pays_query = f"""
    MATCH (c:Company)-[:PAYS]->(t:Transaction)
    {periods_filter}
    RETURN id(c) as from_node, id(t) as to_node
    """
    rows = execute_query(session, pays_query)
    for row in rows:
        from_node = row.get("from_node", "")
        to_node = row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get((from_node, to_node))
            if weight is None:
                weight = edge_weights.get("PAYS", 0.3)
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # 查询 RECEIVES 边（Transaction -> Company）
    receives_query = f"""
    MATCH (t:Transaction)-[:RECEIVES]->(c:Company)
    {periods_filter}
    RETURN id(t) as from_node, id(c) as to_node
    """
    rows = execute_query(session, receives_query)
    for row in rows:
        from_node = row.get("from_node", "")
        to_node = row.get("to_node", "")
        if from_node and to_node:
            weight = embedding_weights.get((from_node, to_node))
            if weight is None:
                weight = edge_weights.get("RECEIVES", 0.3)
            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    # 查询 PARTY_A 和 PARTY_B 边（Company -> Contract）
    # 修改说明：为了让风险从 Contract 传导给 Company，这里需要反向构建边
    if periods:
        if len(periods) == 1:
            contract_periods_filter = f"WHERE con.Contract.sign_date == '{periods[0]}'"
        elif len(periods) == 2:
            contract_periods_filter = f"WHERE con.Contract.sign_date >= '{periods[0]}' AND con.Contract.sign_date <= '{periods[1]}'"
        else:
            raise ValueError("时间段列表长度必须为1或2")
    else:
        contract_periods_filter = ""
    party_query = f"""
    MATCH (c:Company)-[e:PARTY_A|PARTY_B]->(con:Contract)
    {contract_periods_filter}
    RETURN id(c) as company_id, id(con) as contract_id, type(e) as edge_type
    """
    rows = execute_query(session, party_query)
    for row in rows:
        company_id = row.get("company_id", "")
        contract_id = row.get("contract_id", "")
        edge_type = row.get("edge_type", "")
        if company_id and contract_id:
            # 注意：这里我们将 Contract 作为源节点，Company 作为目标节点
            from_node = contract_id
            to_node = company_id

            weight = embedding_weights.get(
                (company_id, contract_id)
            )  # 权重查询可能需要保持原方向或重新计算
            if weight is None:
                weight = edge_weights.get(edge_type, 0.5)  # 建议适当提高此处的传导权重

            graph["nodes"].add(from_node)
            graph["nodes"].add(to_node)
            graph["edges"][from_node].append((to_node, weight))
            graph["out_degree"][from_node] += 1

    return graph


def initialize_risk_seeds(session, config: Optional[FraudRankConfig] = None):
    """
    从法律事件数据初始化风险种子

    Args:
        session: Nebula Graph session
        config: FraudRank 配置对象，默认使用 DEFAULT_CONFIG
    """
    if config is None:
        config = DEFAULT_CONFIG

    init_scores = defaultdict(float)

    # 查询合同关联的法律事件，直接给合同分配初始风险
    contract_event_query = """
    MATCH (con:Contract)-[:RELATED_TO]->(le:LegalEvent)
    RETURN id(con) as contract_id, id(le) as event_id,
           le.LegalEvent.event_type as event_type,
           le.LegalEvent.amount as amount,
           le.LegalEvent.status as status
    """
    contract_events = execute_query(session, contract_event_query)

    # 给合同分配初始风险分数
    for row in contract_events:
        contract_id = row.get("contract_id", "")
        event_id = row.get("event_id", "")
        event_type = row.get("event_type", "")
        amount = float(row.get("amount", 0) or 0)
        status = row.get("status", "")

        if contract_id and event_id:
            event = {"event_type": event_type, "amount": amount, "status": status}
            contract_score = calculate_init_score(None, [event], config)
            # 合同可能关联多个法律事件，取最大值
            init_scores[contract_id] = max(init_scores[contract_id], contract_score)

    return dict(init_scores)


def compute_fraud_rank(graph, init_scores, damping=0.85, max_iter=100, tolerance=1e-6):
    """
    计算 FraudRank 分数

    Args:
        graph: 图数据结构
        init_scores: dict {node_id: init_score}
        damping: 阻尼系数
        max_iter: 最大迭代次数
        tolerance: 收敛阈值

    Returns:
        dict: {node_id: fraud_rank_score}
    """
    # 初始化所有节点分数
    scores = {node: init_scores.get(node, 0.0) for node in graph["nodes"]}

    for iteration in range(max_iter):
        new_scores = {}
        max_diff = 0.0

        for node in graph["nodes"]:
            # 基础分数（保留初始风险）
            base_score = (1 - damping) * init_scores.get(node, 0.0)

            # 从入边传播来的分数
            propagated_score = 0.0
            for neighbor, neighbors_list in graph["edges"].items():
                for target, weight in neighbors_list:
                    if target == node:
                        # neighbor -> node 的边
                        out_deg = graph["out_degree"][neighbor]
                        if out_deg > 0:
                            propagated_score += weight * scores[neighbor] / out_deg

            new_scores[node] = base_score + damping * propagated_score
            max_diff = max(max_diff, abs(new_scores[node] - scores[node]))

        scores = new_scores

        if max_diff < tolerance:
            print(f"  收敛于第 {iteration + 1} 次迭代")
            break

    return scores


def get_risk_level(score):
    """风险等级划分"""
    if score >= 0.7:
        return "高风险"
    elif score >= 0.4:
        return "中风险"
    elif score >= 0.2:
        return "低风险"
    else:
        return "正常"


def analyze_fraud_rank_results(fraud_scores, session, top_n=50):
    """
    分析 FraudRank 结果并生成报告
    """
    # 查询公司信息
    company_query = """
    MATCH (c:Company)
    RETURN id(c) as company_id, c.Company.name as name,
           c.Company.legal_person as legal_person,
           c.Company.credit_code as credit_code
    """
    companies = execute_query(session, company_query)

    # 构建公司信息字典
    company_info = {}
    for row in companies:
        company_id = row.get("company_id", "")
        if company_id:
            company_info[company_id] = {
                "name": row.get("name", "Unknown"),
                "legal_person": row.get("legal_person", "N/A"),
                "credit_code": row.get("credit_code", "N/A"),
            }

    # 按分数排序
    sorted_scores = sorted(fraud_scores.items(), key=lambda x: x[1], reverse=True)

    # 生成报告
    report = []
    for node_id, score in sorted_scores[:top_n]:
        # 只处理公司节点
        if node_id in company_info:
            info = company_info[node_id]
            report.append(
                {
                    "公司ID": node_id,
                    "公司名称": info.get("name", "Unknown"),
                    "风险分数": round(score, 4),
                    "风险等级": get_risk_level(score),
                    "法人代表": info.get("legal_person", "N/A"),
                    "信用代码": info.get("credit_code", "N/A"),
                }
            )

    df_report = pd.DataFrame(report)

    # 确保报告目录存在
    os.makedirs(REPORTS_DIR, exist_ok=True)

    output_file = os.path.join(REPORTS_DIR, "fraud_rank_report.csv")
    df_report.to_csv(output_file, index=False, encoding="utf-8-sig")

    return df_report


def main(
    force_recompute_embedding=False,
    config: Optional[FraudRankConfig] = None,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
):
    """
    Main function for FraudRank analysis

    Args:
        force_recompute_embedding: 是否强制重新计算 embedding 权重
        config: FraudRank 配置对象，默认使用 DEFAULT_CONFIG
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）
    """
    if config is None:
        config = DEFAULT_CONFIG

    print("=" * 60)
    print("FraudRank 欺诈风险传导分析")
    print("=" * 60)
    
    if company_ids:
        print(f"  过滤公司: {len(company_ids)} 家")
    if periods:
        print(f"  时间范围: {periods}")

    session = None
    try:
        session = get_nebula_session()

        # Step 1: 加载图数据
        print("\n[1/4] 加载图数据...")
        graph = load_weighted_graph(
            session,
            force_recompute=force_recompute_embedding,
            config=config,
            company_ids=company_ids,
            periods=periods,
        )
        print(f"  节点数: {len(graph['nodes'])}")
        print(f"  边数: {sum(len(v) for v in graph['edges'].values())}")

        # Step 2: 初始化风险种子
        print("\n[2/4] 初始化风险种子节点...")
        init_scores = initialize_risk_seeds(session, config=config)
        seed_count = sum(1 for s in init_scores.values() if s > 0)
        print(f"  风险种子节点数: {seed_count}")
        if seed_count > 0:
            print(f"  平均初始分数: {sum(init_scores.values()) / seed_count:.4f}")

        # Step 3: 计算 FraudRank
        print("\n[3/4] 计算 FraudRank（迭代中...）")
        fraud_scores = compute_fraud_rank(graph, init_scores, damping=0.85)

        # Step 4: 生成分析报告
        print("\n[4/4] 生成分析报告...")
        report = analyze_fraud_rank_results(fraud_scores, session, top_n=50)

        print("\n" + "=" * 60)
        print("分析完成！")
        print("=" * 60)

        if len(report) > 0:
            print(f"\n前 10 高风险公司：\n")
            print(report.head(10).to_string(index=False))
            print(f"\n完整报告已保存至: reports/fraud_rank_report.csv")
        else:
            print("\n未发现高风险公司")

    finally:
        if session:
            session.release()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="FraudRank 欺诈风险传导分析")
    parser.add_argument(
        "--force-recompute",
        action="store_true",
        help="强制重新计算 embedding 权重，忽略缓存",
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
        force_recompute_embedding=args.force_recompute,
        company_ids=company_ids,
        periods=periods,
    )
