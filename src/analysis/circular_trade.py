"""
场景二：高级循环交易检测（分散-汇聚模式）

检测复杂的循环交易模式，包括分散-汇聚模式
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from src.utils.nebula_utils import get_nebula_session, execute_query

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")


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


def detect_fan_out_fan_in(session, time_window_days=180, amount_threshold=1000000):
    """
    检测分散-汇聚模式的循环交易

    Args:
        session: Nebula session
        time_window_days: 时间窗口（天）
        amount_threshold: 金额阈值

    Returns:
        list: 可疑模式列表
    """
    # 查询所有资金流：付款公司 -> 交易 -> 收款公司
    money_flow_query = """
    MATCH (payer:Company)-[:PAYS]->(t:Transaction)-[:RECEIVES]->(receiver:Company)
    RETURN id(payer) as payer_company,
           id(receiver) as receiver_company,
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
                "receiver_company": row.get("receiver_company", ""),
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
                suspicious_patterns.append(
                    {
                        "central_company": central_company,
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
                    }
                )

    return suspicious_patterns


def main():
    print("=" * 70)
    print("高级循环交易检测 - 分散汇聚模式分析")
    print("=" * 70)

    session = None
    try:
        session = get_nebula_session()

        print("\n[1/3] 分析资金流向...")
        suspicious_patterns = detect_fan_out_fan_in(
            session,
            time_window_days=180,
            amount_threshold=500000,  # 50万以上
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
    main()
