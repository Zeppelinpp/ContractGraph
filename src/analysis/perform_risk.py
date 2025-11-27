"""
场景：履约关联风险检测

根据相对方获取签署履约状态的合同存在收款逾期或交货逾期的，
排查并列出同一相对方或存在相关关系的相对方的相同标的名称的其他合同，
可能也会存在履约风险。
"""

import os
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional
import pandas as pd
from src.utils.nebula_utils import get_nebula_session, execute_query

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")


def parse_date(date_str: str) -> datetime:
    """Parse date string to datetime object"""
    if not date_str or date_str.strip() == "":
        return None
    try:
        # Try different date formats
        for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"]:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        return None
    except Exception:
        return None


def extract_subject_name(contract_name: str) -> str:
    """Extract subject name from contract name"""
    if not contract_name:
        return ""
    # Extract subject before the dash or company name
    # e.g., "建材采购合同-宝山钢铁股份有限公司" -> "建材采购合同"
    parts = contract_name.split("-")
    if len(parts) > 0:
        return parts[0].strip()
    return contract_name.strip()


def find_overdue_transactions(
    session,
    current_date: datetime,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
):
    """
    Find transactions with payment or delivery overdue
    
    Args:
        session: Nebula session
        current_date: Current date for comparison
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围，按transaction_date过滤）
    
    Returns:
        List of dicts with transaction info and related contract/company info
    """
    # Build transaction filter
    where_clauses = []
    if periods:
        if len(periods) == 1:
            where_clauses.append(f"t.Transaction.transaction_date == '{periods[0]}'")
        elif len(periods) == 2:
            where_clauses.append(f"t.Transaction.transaction_date >= '{periods[0]}' AND t.Transaction.transaction_date <= '{periods[1]}'")
    
    txn_filter = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    
    query = f"""
    MATCH (t:Transaction)
    {txn_filter}
    RETURN id(t) as transaction_id,
           properties(t) as t_props
    """
    
    transaction_results = execute_query(session, query)
    
    # Build company filter for contracts
    company_filter = ""
    if company_ids:
        ids_str = ", ".join([f"'{cid}'" for cid in company_ids])
        company_filter = f"WHERE comp.Company.number IN [{ids_str}]"
    
    contract_query = f"""
    MATCH (c:Contract)
    OPTIONAL MATCH (comp:Company)-[:PARTY_A|PARTY_B]->(c)
    {company_filter}
    RETURN id(c) as contract_id,
           properties(c) as c_props,
           id(comp) as company_id,
           properties(comp) as comp_props
    """
    
    contract_results = execute_query(session, contract_query)
    
    # Build contract lookup by contract_no
    contract_lookup = {}
    for row in contract_results:
        contract_id = row.get("contract_id", "")
        c_props = row.get("c_props", {})
        company_id = row.get("company_id", "")
        comp_props = row.get("comp_props", {})
        
        if contract_id and isinstance(c_props, dict):
            contract_no = c_props.get("contract_no", "")
            if contract_no:
                if contract_no not in contract_lookup:
                    contract_lookup[contract_no] = {
                        "contract_id": contract_id,
                        "contract_name": c_props.get("contract_name", ""),
                        "companies": []
                    }
                if company_id and isinstance(comp_props, dict):
                    contract_lookup[contract_no]["companies"].append({
                        "company_id": company_id,
                        "company_name": comp_props.get("name", ""),
                    })
    
    # Process transactions
    results = []
    for row in transaction_results:
        transaction_id = row.get("transaction_id", "")
        t_props = row.get("t_props", {})
        
        if not transaction_id or not isinstance(t_props, dict):
            continue
        
        contract_no = t_props.get("contract_no", "")
        if contract_no in contract_lookup:
            contract_info = contract_lookup[contract_no]
            for company_info in contract_info["companies"]:
                results.append({
                    "transaction_id": transaction_id,
                    "transaction_no": t_props.get("transaction_no", ""),
                    "contract_no": contract_no,
                    "fpaidamount": t_props.get("fpaidamount", 0),
                    "amount": t_props.get("amount", 0),
                    "duetime": t_props.get("duetime", ""),
                    "status": t_props.get("status", ""),
                    "transaction_type": t_props.get("transaction_type", ""),
                    "contract_id": contract_info["contract_id"],
                    "contract_name": contract_info["contract_name"],
                    "company_id": company_info["company_id"],
                    "company_name": company_info["company_name"],
                })
    
    overdue_transactions = []
    
    for row in results:
        transaction_id = row.get("transaction_id", "")
        transaction_no = row.get("transaction_no", "")
        contract_no = row.get("contract_no", "")
        fpaidamount = row.get("fpaidamount", 0)
        amount = row.get("amount", 0)
        duetime_str = row.get("duetime", "")
        status = row.get("status", "")
        transaction_type = row.get("transaction_type", "")
        contract_id = row.get("contract_id", "")
        contract_name = row.get("contract_name", "")
        company_id = row.get("company_id", "")
        company_name = row.get("company_name", "")
        
        # Skip if status is completed (C = 已履约)
        if status == "C":
            continue
        
        # Parse amounts
        try:
            fpaidamount = float(fpaidamount) if fpaidamount else 0.0
            amount = float(amount) if amount else 0.0
        except (ValueError, TypeError):
            fpaidamount = 0.0
            amount = 0.0
        
        # Parse due date
        due_date = parse_date(duetime_str)
        if not due_date:
            continue
        
        # Check if overdue
        is_overdue = False
        overdue_type = ""
        
        # Payment overdue: due_date < current_date AND amount > fpaidamount
        if due_date < current_date and amount > fpaidamount:
            is_overdue = True
            overdue_type = "收款逾期"
        
        # Delivery overdue: due_date < current_date AND status != C
        if due_date < current_date and status != "C":
            is_overdue = True
            if overdue_type:
                overdue_type = "收款逾期+交货逾期"
            else:
                overdue_type = "交货逾期"
        
        if is_overdue:
            overdue_transactions.append({
                "transaction_id": transaction_id,
                "transaction_no": transaction_no,
                "contract_no": contract_no,
                "contract_id": contract_id,
                "contract_name": contract_name,
                "company_id": company_id,
                "company_name": company_name,
                "fpaidamount": fpaidamount,
                "amount": amount,
                "due_date": due_date,
                "overdue_days": (current_date - due_date).days,
                "overdue_type": overdue_type,
                "transaction_type": transaction_type,
            })
    
    return overdue_transactions


def find_related_companies(session, company_ids: Set[str]) -> Set[str]:
    """
    Find companies related to the given companies through various relationships
    
    Returns:
        Set of related company IDs
    """
    related = set(company_ids)
    
    # Find companies connected through TRADES_WITH, IS_SUPPLIER, IS_CUSTOMER, CONTROLS
    # Query all relationships and filter in Python
    query = """
    MATCH (c1:Company)-[r:TRADES_WITH|IS_SUPPLIER|IS_CUSTOMER|CONTROLS]-(c2:Company)
    RETURN DISTINCT id(c1) as company1, id(c2) as company2
    """
    
    results = execute_query(session, query)
    
    # Filter to only include related companies
    filtered_results = []
    for row in results:
        comp1 = row.get("company1", "")
        comp2 = row.get("company2", "")
        if comp1 in company_ids or comp2 in company_ids:
            filtered_results.append(row)
    results = filtered_results
    
    for row in results:
        comp1 = row.get("company1", "")
        comp2 = row.get("company2", "")
        if comp1:
            related.add(comp1)
        if comp2:
            related.add(comp2)
    
    return related


def find_risk_contracts(session, overdue_transactions: List[Dict], current_date: datetime):
    """
    Find contracts with same subject name from same or related counterparties
    
    Returns:
        Dict mapping company_id to risk contracts and risk score
    """
    # Group overdue transactions by contract to get subject names and counterparties
    contract_subjects = {}  # contract_id -> subject_name
    contract_companies = defaultdict(set)  # contract_id -> set of company_ids
    
    for txn in overdue_transactions:
        contract_id = txn.get("contract_id", "")
        contract_name = txn.get("contract_name", "")
        company_id = txn.get("company_id", "")
        
        if contract_id and contract_name:
            subject_name = extract_subject_name(contract_name)
            contract_subjects[contract_id] = subject_name
            if company_id:
                contract_companies[contract_id].add(company_id)
    
    # Get all counterparties from overdue contracts
    all_counterparties = set()
    for companies in contract_companies.values():
        all_counterparties.update(companies)
    
    # Find related companies
    related_companies = find_related_companies(session, all_counterparties)
    
    # Find contracts with same subject name from same or related counterparties
    risk_contracts_by_company = defaultdict(list)
    
    for subject_name in set(contract_subjects.values()):
        if not subject_name:
            continue
        
        # Query all contracts and filter by subject name in Python
        query = """
        MATCH (c:Contract)
        OPTIONAL MATCH (comp:Company)-[:PARTY_A|PARTY_B]->(c)
        RETURN DISTINCT id(c) as contract_id,
               properties(c) as c_props,
               id(comp) as company_id,
               properties(comp) as comp_props
        """
        
        query_results = execute_query(session, query)
        
        # Process results
        results = []
        for row in query_results:
            contract_id = row.get("contract_id", "")
            c_props = row.get("c_props", {})
            company_id = row.get("company_id", "")
            comp_props = row.get("comp_props", {})
            
            if not contract_id or not isinstance(c_props, dict):
                continue
            
            contract_name = c_props.get("contract_name", "")
            # Filter by subject name
            if subject_name in contract_name:
                results.append({
                    "contract_id": contract_id,
                    "contract_no": c_props.get("contract_no", ""),
                    "contract_name": contract_name,
                    "amount": c_props.get("amount", 0),
                    "status": c_props.get("status", ""),
                    "company_id": company_id,
                    "company_name": comp_props.get("name", "") if isinstance(comp_props, dict) else "",
                })
        
        for row in results:
            contract_id = row.get("contract_id", "")
            contract_no = row.get("contract_no", "")
            contract_name = row.get("contract_name", "")
            amount = row.get("amount", 0)
            status = row.get("status", "")
            company_id = row.get("company_id", "")
            company_name = row.get("company_name", "")
            
            # Only include contracts from same or related counterparties
            if company_id and company_id in related_companies:
                # Check if this contract has overdue transactions
                has_overdue = contract_id in contract_subjects
                
                risk_contracts_by_company[company_id].append({
                    "contract_id": contract_id,
                    "contract_no": contract_no,
                    "contract_name": contract_name,
                    "amount": amount,
                    "status": status,
                    "has_overdue": has_overdue,
                    "subject_name": subject_name,
                })
    
    return risk_contracts_by_company


def calculate_risk_score(company_id: str, risk_contracts: List[Dict], overdue_transactions: List[Dict]) -> float:
    """
    Calculate risk score for a company based on overdue transactions and risk contracts
    
    Returns:
        Risk score (0-1)
    """
    score = 0.0
    
    # Get overdue transactions for this company
    company_overdue_txns = [
        txn for txn in overdue_transactions
        if txn.get("company_id") == company_id
    ]
    company_overdue_count = len(company_overdue_txns)
    
    # Base score from overdue count with severity consideration
    if company_overdue_count > 0:
        # Calculate overdue severity based on maximum overdue days
        # Using max instead of avg to reflect the most severe case
        overdue_days_list = [txn.get("overdue_days", 0) for txn in company_overdue_txns]
        if overdue_days_list:
            max_overdue_days = max(overdue_days_list)
            # Normalize overdue days severity (365 days as max, beyond that treated as 1.0)
            # Use power function (0.7) to emphasize longer overdue periods
            # e.g., 30 days -> 0.19, 90 days -> 0.40, 180 days -> 0.64, 365 days -> 1.0
            severity_factor = min(1.0, (max_overdue_days / 365.0) ** 0.7)
            # Base score: count * base_weight * (1 + severity_factor * 0.5)
            # Severity multiplier ranges from 1.0 (no severity) to 1.5 (max severity)
            # This means longer overdue periods get higher weight
            base_weight = 0.15
            severity_multiplier = 1.0 + severity_factor * 0.5
            score += min(company_overdue_count * base_weight * severity_multiplier, 0.5)
    
    # Count risk contracts (contracts with same subject from same/related counterparties)
    total_risk_contracts = len(risk_contracts)
    contracts_with_overdue = sum(1 for c in risk_contracts if c.get("has_overdue"))
    
    # Add score based on risk contracts
    if total_risk_contracts > 0:
        risk_ratio = contracts_with_overdue / total_risk_contracts
        score += risk_ratio * 0.3
    
    # Add score based on total amount of risk contracts
    total_amount = sum(float(c.get("amount", 0) or 0) for c in risk_contracts)
    if total_amount > 0:
        # Normalize amount (assume 10M as max)
        amount_score = min(total_amount / 10000000, 1.0) * 0.2
        score += amount_score
    
    return min(score, 1.0)


def analyze_perform_risk(
    session,
    current_date: datetime,
    top_n: int = 10,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
):
    """
    Main analysis function
    
    Args:
        session: Nebula session
        current_date: Current date for comparison
        top_n: Number of top risk companies to return
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）
    
    Returns:
        DataFrame with top N risk companies
    """
    print(f"\n[1/4] 查找逾期交易...")
    overdue_transactions = find_overdue_transactions(
        session,
        current_date,
        company_ids=company_ids,
        periods=periods,
    )
    print(f"  发现 {len(overdue_transactions)} 笔逾期交易")
    
    if not overdue_transactions:
        print("\n未发现逾期交易")
        return pd.DataFrame()
    
    print(f"\n[2/4] 查找关联相对方...")
    risk_contracts_by_company = find_risk_contracts(session, overdue_transactions, current_date)
    print(f"  涉及 {len(risk_contracts_by_company)} 个相对方")
    
    print(f"\n[3/4] 计算风险分数...")
    company_scores = {}
    
    # Get company info with filter consistent with find_overdue_transactions
    company_filter = ""
    if company_ids:
        ids_str = ", ".join([f"'{cid}'" for cid in company_ids])
        company_filter = f"WHERE c.Company.number IN [{ids_str}]"
    
    company_query = f"""
    MATCH (c:Company)
    {company_filter}
    RETURN id(c) as company_id,
           properties(c) as c_props
    """
    companies = execute_query(session, company_query)
    company_info = {}
    for row in companies:
        company_id = row.get("company_id", "")
        c_props = row.get("c_props", {})
        if company_id and isinstance(c_props, dict):
            company_info[company_id] = {
                "name": c_props.get("name", "Unknown"),
                "legal_person": c_props.get("legal_person", "N/A"),
                "credit_code": c_props.get("credit_code", "N/A"),
            }
    
    # Calculate scores
    for company_id, risk_contracts in risk_contracts_by_company.items():
        if company_id in company_info:
            score = calculate_risk_score(company_id, risk_contracts, overdue_transactions)
            company_scores[company_id] = {
                "score": score,
                "risk_contracts": risk_contracts,
                "overdue_count": sum(
                    1 for txn in overdue_transactions
                    if txn.get("company_id") == company_id
                ),
            }
    
    print(f"\n[4/4] 生成分析报告...")
    
    # Sort by score
    sorted_companies = sorted(
        company_scores.items(),
        key=lambda x: x[1]["score"],
        reverse=True
    )[:top_n]
    
    # Build report
    report = []
    for company_id, data in sorted_companies:
        info = company_info.get(company_id, {})
        risk_contracts = data["risk_contracts"]
        
        report.append({
            "公司ID": company_id,
            "公司名称": info.get("name", "Unknown"),
            "风险分数": round(data["score"], 4),
            "逾期交易数": data["overdue_count"],
            "风险合同数": len(risk_contracts),
            "法人代表": info.get("legal_person", "N/A"),
            "信用代码": info.get("credit_code", "N/A"),
            "风险合同列表": "; ".join([
                f"{c['contract_no']}({c['contract_name']})"
                for c in risk_contracts[:5]  # Limit to first 5
            ]),
        })
    
    df_report = pd.DataFrame(report)
    
    # Save report
    os.makedirs(REPORTS_DIR, exist_ok=True)
    output_file = os.path.join(REPORTS_DIR, "perform_risk_report.csv")
    df_report.to_csv(output_file, index=False, encoding="utf-8-sig")
    
    return df_report


def main(
    current_date: datetime = None,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
):
    """
    Main function for performance risk analysis
    
    Args:
        current_date: Current date for comparison. If None, uses datetime.now()
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）
    """
    print("=" * 60)
    print("履约关联风险检测")
    print("=" * 60)
    
    # Use current date or provided date
    if current_date is None:
        current_date = datetime.now()
    
    print(f"\n当前时间: {current_date.strftime('%Y-%m-%d')}")
    if company_ids:
        print(f"  过滤公司: {len(company_ids)} 家")
    if periods:
        print(f"  时间范围: {periods}")
    
    session = None
    try:
        session = get_nebula_session()
        
        report = analyze_perform_risk(
            session,
            current_date,
            top_n=10,
            company_ids=company_ids,
            periods=periods,
        )
        
        print("\n" + "=" * 60)
        print("分析完成！")
        print("=" * 60)
        
        if len(report) > 0:
            print(f"\n前 10 高风险企业：\n")
            print(report.to_string(index=False))
            print(f"\n完整报告已保存至: reports/perform_risk_report.csv")
        else:
            print("\n未发现高风险企业")
    
    finally:
        if session:
            session.release()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="履约关联风险检测")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="指定当前日期，格式：YYYY-MM-DD（默认：当前日期）",
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
    
    current_date = None
    if args.date:
        try:
            current_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print(f"错误: 日期格式不正确，请使用 YYYY-MM-DD 格式")
            exit(1)
    
    company_ids = args.company_ids.split(",") if args.company_ids else None
    periods = args.periods.split(",") if args.periods else None
    
    main(current_date=current_date, company_ids=company_ids, periods=periods)

