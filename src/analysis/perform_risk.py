"""
场景：履约关联风险检测

根据相对方获取签署履约状态的合同存在收款逾期或交货逾期的，
排查并列出同一相对方或存在相关关系的相对方的相同标的名称的其他合同，
可能也会存在履约风险。
"""

import os
import json
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional
import pandas as pd
from src.utils.nebula_utils import get_nebula_session, execute_query
from src.config.models import PerformRiskConfig

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

DEFAULT_CONFIG = PerformRiskConfig()


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


def calculate_risk_score(
    company_id: str,
    risk_contracts: List[Dict],
    overdue_transactions: List[Dict],
    config: PerformRiskConfig = None,
) -> float:
    """
    Calculate risk score for a company based on overdue transactions and risk contracts
    
    Args:
        company_id: Company ID
        risk_contracts: List of risk contracts
        overdue_transactions: List of overdue transactions
        config: Configuration parameters
    
    Returns:
        Risk score (0-1)
    """
    if config is None:
        config = DEFAULT_CONFIG
    
    score = 0.0
    
    # Get overdue transactions for this company
    company_overdue_txns = [
        txn for txn in overdue_transactions
        if txn.get("company_id") == company_id
    ]
    company_overdue_count = len(company_overdue_txns)
    
    # Base score from overdue count with severity consideration
    if company_overdue_count > 0:
        overdue_days_list = [txn.get("overdue_days", 0) for txn in company_overdue_txns]
        if overdue_days_list:
            max_overdue_days = max(overdue_days_list)
            severity_factor = min(1.0, (max_overdue_days / config.overdue_days_max) ** config.severity_power)
            severity_multiplier = 1.0 + severity_factor * config.severity_multiplier_max
            score += min(company_overdue_count * config.overdue_base_weight * severity_multiplier, config.overdue_score_cap)
    
    # Count risk contracts (contracts with same subject from same/related counterparties)
    total_risk_contracts = len(risk_contracts)
    contracts_with_overdue = sum(1 for c in risk_contracts if c.get("has_overdue"))
    
    # Add score based on risk contracts
    if total_risk_contracts > 0:
        risk_ratio = contracts_with_overdue / total_risk_contracts
        score += risk_ratio * config.risk_contract_weight
    
    # Add score based on total amount of risk contracts
    total_amount = sum(float(c.get("amount", 0) or 0) for c in risk_contracts)
    if total_amount > 0:
        amount_score = min(total_amount / config.amount_threshold, 1.0) * config.amount_weight
        score += amount_score
    
    return min(score, 1.0)


def analyze_perform_risk(
    session,
    current_date: datetime,
    top_n: int = 10,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
    config: PerformRiskConfig = None,
):
    """
    Main analysis function
    
    Args:
        session: Nebula session
        current_date: Current date for comparison
        top_n: Number of top risk companies to return
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）
        config: 算法配置参数
    
    Returns:
        Dict with report DataFrame and risk contract IDs
    """
    if config is None:
        config = DEFAULT_CONFIG
    
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
        return {
            "report": pd.DataFrame(),
            "risk_contract_ids": [],
            "overdue_transactions": [],
            "risk_contracts_by_company": {},
        }
    
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
            score = calculate_risk_score(company_id, risk_contracts, overdue_transactions, config)
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
    
    # Collect all risk contract IDs
    all_risk_contract_ids = set()
    for company_id, data in company_scores.items():
        for contract in data["risk_contracts"]:
            contract_id = contract.get("contract_id", "")
            if contract_id:
                all_risk_contract_ids.add(contract_id)
    
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
    
    return {
        "report": df_report,
        "risk_contract_ids": list(all_risk_contract_ids),
        "overdue_transactions": overdue_transactions,
        "risk_contracts_by_company": risk_contracts_by_company,
    }


def get_perform_risk_subgraph(
    session,
    contract_id: str,
    current_date: datetime = None,
) -> Dict:
    """
    获取履约风险子图
    
    传入risk contract id -> 找到这个合同id的相关方 -> 
    这些相关方有哪些逾期交易以及这些逾期交易涉及哪些合同
    
    Args:
        session: Nebula session
        contract_id: 风险合同ID
        current_date: 当前日期，默认为今天
    
    Returns:
        Dict with subgraph data and HTML path
    """
    if current_date is None:
        current_date = datetime.now()
    
    # Step 1: Find parties of the contract
    party_query = f"""
    MATCH (comp:Company)-[:PARTY_A|PARTY_B]->(c:Contract)
    WHERE id(c) == '{contract_id}'
    RETURN DISTINCT id(comp) as company_id,
           properties(comp) as comp_props,
           id(c) as contract_id,
           properties(c) as c_props
    """
    party_results = execute_query(session, party_query)
    
    if not party_results:
        return {
            "success": False,
            "message": f"未找到合同 {contract_id} 的相关方",
            "nodes": [],
            "edges": [],
            "html_url": None,
        }
    
    # Collect company info and contract info
    companies = {}
    contract_info = None
    for row in party_results:
        company_id = row.get("company_id", "")
        comp_props = row.get("comp_props", {})
        c_props = row.get("c_props", {})
        
        if company_id and isinstance(comp_props, dict):
            companies[company_id] = {
                "name": comp_props.get("name", ""),
                "credit_code": comp_props.get("credit_code", ""),
            }
        
        if contract_info is None and isinstance(c_props, dict):
            contract_info = {
                "contract_id": row.get("contract_id", ""),
                "contract_no": c_props.get("contract_no", ""),
                "contract_name": c_props.get("contract_name", ""),
                "amount": c_props.get("amount", 0),
            }
    
    company_ids = list(companies.keys())
    
    # Step 2: Find overdue transactions for these companies
    overdue_transactions = find_overdue_transactions(session, current_date, company_ids=None, periods=None)
    
    # Filter to transactions related to these companies
    related_overdue_txns = [
        txn for txn in overdue_transactions
        if txn.get("company_id") in company_ids
    ]
    
    # Step 3: Collect all contracts from overdue transactions
    overdue_contracts = {}
    for txn in related_overdue_txns:
        c_id = txn.get("contract_id", "")
        if c_id and c_id not in overdue_contracts:
            overdue_contracts[c_id] = {
                "contract_id": c_id,
                "contract_no": txn.get("contract_no", ""),
                "contract_name": txn.get("contract_name", ""),
            }
    
    # Step 4: Build subgraph nodes and edges
    nodes = []
    edges = []
    node_ids = set()
    
    # Add the input contract node
    if contract_info:
        nodes.append({
            "id": contract_info["contract_id"],
            "type": "Contract",
            "label": contract_info.get("contract_name", contract_info["contract_id"])[:20],
            "properties": {
                "contract_no": contract_info.get("contract_no", ""),
                "contract_name": contract_info.get("contract_name", ""),
                "amount": contract_info.get("amount", 0),
                "is_input": True,
            },
        })
        node_ids.add(contract_info["contract_id"])
    
    # Add company nodes
    for company_id, info in companies.items():
        if company_id not in node_ids:
            nodes.append({
                "id": company_id,
                "type": "Company",
                "label": info.get("name", company_id)[:20],
                "properties": {
                    "name": info.get("name", ""),
                    "credit_code": info.get("credit_code", ""),
                },
            })
            node_ids.add(company_id)
        
        # Add edge from company to input contract
        if contract_info:
            edges.append({
                "source": company_id,
                "target": contract_info["contract_id"],
                "type": "PARTY",
                "properties": {},
            })
    
    # Add overdue contract nodes
    for c_id, c_info in overdue_contracts.items():
        if c_id not in node_ids:
            nodes.append({
                "id": c_id,
                "type": "Contract",
                "label": c_info.get("contract_name", c_id)[:20],
                "properties": {
                    "contract_no": c_info.get("contract_no", ""),
                    "contract_name": c_info.get("contract_name", ""),
                    "has_overdue": True,
                },
            })
            node_ids.add(c_id)
    
    # Add transaction nodes and edges
    for txn in related_overdue_txns:
        txn_id = txn.get("transaction_id", "")
        company_id = txn.get("company_id", "")
        c_id = txn.get("contract_id", "")
        
        if txn_id and txn_id not in node_ids:
            nodes.append({
                "id": txn_id,
                "type": "Transaction",
                "label": f"逾期{txn.get('overdue_days', 0)}天",
                "properties": {
                    "transaction_no": txn.get("transaction_no", ""),
                    "overdue_type": txn.get("overdue_type", ""),
                    "overdue_days": txn.get("overdue_days", 0),
                    "amount": txn.get("amount", 0),
                    "fpaidamount": txn.get("fpaidamount", 0),
                },
            })
            node_ids.add(txn_id)
        
        # Add edge from transaction to company
        if txn_id and company_id:
            edges.append({
                "source": txn_id,
                "target": company_id,
                "type": "OVERDUE_FOR",
                "properties": {
                    "overdue_type": txn.get("overdue_type", ""),
                },
            })
        
        # Add edge from transaction to contract
        if txn_id and c_id:
            edges.append({
                "source": txn_id,
                "target": c_id,
                "type": "BELONGS_TO",
                "properties": {},
            })
    
    # Step 5: Generate HTML visualization
    html_path = generate_perform_risk_subgraph_html(
        contract_id=contract_id,
        nodes=nodes,
        edges=edges,
        contract_info=contract_info,
        companies=companies,
        overdue_transactions=related_overdue_txns,
    )
    
    return {
        "success": True,
        "contract_id": contract_id,
        "nodes": nodes,
        "edges": edges,
        "html_url": html_path,
        "overdue_transaction_count": len(related_overdue_txns),
        "related_contract_count": len(overdue_contracts),
        "company_count": len(companies),
        "contract_ids": list(overdue_contracts.keys()),
    }


def generate_perform_risk_subgraph_html(
    contract_id: str,
    nodes: List[Dict],
    edges: List[Dict],
    contract_info: Dict,
    companies: Dict,
    overdue_transactions: List[Dict],
) -> str:
    """
    生成履约风险子图的交互式HTML页面
    """
    safe_id = contract_id.replace('"', '').replace("'", "")
    output_filename = f"perform_risk_subgraph_{safe_id}.html"
    
    os.makedirs(REPORTS_DIR, exist_ok=True)
    output_path = os.path.join(REPORTS_DIR, output_filename)
    
    nodes_json = json.dumps(nodes, ensure_ascii=False)
    edges_json = json.dumps(edges, ensure_ascii=False)
    
    html_content = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>履约风险子图 - {contract_id}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            color: #e0e0e0;
            min-height: 100vh;
        }}
        .container {{
            max-width: 1600px;
            margin: 0 auto;
            padding: 20px;
        }}
        header {{
            text-align: center;
            padding: 20px 0;
            border-bottom: 1px solid rgba(255,255,255,0.1);
            margin-bottom: 20px;
        }}
        header h1 {{
            font-size: 1.8rem;
            color: #00d9ff;
            margin-bottom: 8px;
        }}
        header p {{
            color: #888;
            font-size: 0.9rem;
        }}
        .stats-bar {{
            display: flex;
            justify-content: center;
            gap: 40px;
            padding: 15px;
            background: rgba(255,255,255,0.05);
            border-radius: 10px;
            margin-bottom: 20px;
        }}
        .stat-item {{
            text-align: center;
        }}
        .stat-value {{
            font-size: 1.5rem;
            font-weight: bold;
            color: #00ff88;
        }}
        .stat-label {{
            font-size: 0.8rem;
            color: #888;
            margin-top: 4px;
        }}
        .main-content {{
            display: flex;
            gap: 20px;
            height: calc(100vh - 220px);
        }}
        .sidebar {{
            width: 280px;
            background: rgba(255,255,255,0.05);
            border-radius: 10px;
            padding: 15px;
            overflow-y: auto;
        }}
        .legend {{
            margin-bottom: 20px;
        }}
        .legend h3 {{
            font-size: 0.9rem;
            margin-bottom: 10px;
            color: #00d9ff;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 5px 0;
            font-size: 0.85rem;
        }}
        .legend-dot {{
            width: 12px;
            height: 12px;
            border-radius: 50%;
        }}
        .graph-panel {{
            flex: 1;
            background: rgba(255,255,255,0.03);
            border-radius: 10px;
            overflow: hidden;
            position: relative;
        }}
        #graph-svg {{
            width: 100%;
            height: 100%;
        }}
        .node {{
            cursor: pointer;
        }}
        .node circle {{
            stroke: #fff;
            stroke-width: 2px;
        }}
        .node text {{
            font-size: 10px;
            fill: #fff;
            text-anchor: middle;
            pointer-events: none;
        }}
        .link {{
            stroke: #666;
            stroke-opacity: 0.6;
        }}
        .link-label {{
            font-size: 9px;
            fill: #888;
        }}
        .tooltip {{
            position: absolute;
            background: rgba(0,0,0,0.9);
            border: 1px solid #00d9ff;
            border-radius: 8px;
            padding: 10px;
            font-size: 12px;
            pointer-events: none;
            z-index: 1000;
            max-width: 300px;
        }}
        .tooltip-key {{
            color: #00d9ff;
            margin-right: 5px;
        }}
        .tooltip-value {{
            color: #fff;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>📋 履约风险关联子图</h1>
            <p>合同相关方逾期交易分析</p>
        </header>
        
        <div class="stats-bar">
            <div class="stat-item">
                <div class="stat-value" id="node-count">{len(nodes)}</div>
                <div class="stat-label">节点数量</div>
            </div>
            <div class="stat-item">
                <div class="stat-value" id="edge-count">{len(edges)}</div>
                <div class="stat-label">关系数量</div>
            </div>
            <div class="stat-item">
                <div class="stat-value" id="txn-count">{len(overdue_transactions)}</div>
                <div class="stat-label">逾期交易</div>
            </div>
            <div class="stat-item">
                <div class="stat-value" id="company-count">{len(companies)}</div>
                <div class="stat-label">相关公司</div>
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
                        <span>逾期交易 (Transaction)</span>
                    </div>
                </div>
            </div>
            
            <div class="graph-panel">
                <svg id="graph-svg"></svg>
            </div>
        </div>
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
            'Transaction': '#ff6b6b'
        }};
        
        const svg = d3.select('#graph-svg');
        const container = svg.node().parentElement;
        const width = container.clientWidth;
        const height = container.clientHeight;
        
        svg.attr('viewBox', [0, 0, width, height]);
        
        const g = svg.append('g');
        
        // Zoom behavior
        const zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', (event) => g.attr('transform', event.transform));
        svg.call(zoom);
        
        // Build node map
        const nodeMap = new Map(graphData.nodes.map(n => [n.id, n]));
        
        // Create links
        const links = graphData.edges.map(e => ({{
            source: e.source,
            target: e.target,
            type: e.type,
            properties: e.properties
        }}));
        
        // Force simulation
        const simulation = d3.forceSimulation(graphData.nodes)
            .force('link', d3.forceLink(links).id(d => d.id).distance(100))
            .force('charge', d3.forceManyBody().strength(-300))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(40));
        
        // Draw links
        const link = g.append('g')
            .selectAll('line')
            .data(links)
            .join('line')
            .attr('class', 'link')
            .attr('stroke-width', 1.5);
        
        // Draw nodes
        const node = g.append('g')
            .selectAll('g')
            .data(graphData.nodes)
            .join('g')
            .attr('class', 'node')
            .call(d3.drag()
                .on('start', dragstarted)
                .on('drag', dragged)
                .on('end', dragended));
        
        node.append('circle')
            .attr('r', d => d.type === 'Transaction' ? 15 : 20)
            .attr('fill', d => colorMap[d.type] || '#888');
        
        node.append('text')
            .attr('dy', 4)
            .text(d => d.label || d.id.substring(0, 8));
        
        // Tooltip
        const tooltip = d3.select('#tooltip');
        
        node.on('mouseover', (event, d) => {{
            let html = `<div><span class="tooltip-key">ID:</span><span class="tooltip-value">${{d.id}}</span></div>`;
            html += `<div><span class="tooltip-key">类型:</span><span class="tooltip-value">${{d.type}}</span></div>`;
            if (d.properties) {{
                for (const [key, value] of Object.entries(d.properties)) {{
                    if (value !== null && value !== undefined && value !== '') {{
                        html += `<div><span class="tooltip-key">${{key}}:</span><span class="tooltip-value">${{value}}</span></div>`;
                    }}
                }}
            }}
            tooltip.html(html)
                .style('display', 'block')
                .style('left', (event.pageX + 10) + 'px')
                .style('top', (event.pageY - 10) + 'px');
        }})
        .on('mouseout', () => tooltip.style('display', 'none'));
        
        simulation.on('tick', () => {{
            link
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);
            
            node.attr('transform', d => `translate(${{d.x}},${{d.y}})`);
        }});
        
        function dragstarted(event) {{
            if (!event.active) simulation.alphaTarget(0.3).restart();
            event.subject.fx = event.subject.x;
            event.subject.fy = event.subject.y;
        }}
        
        function dragged(event) {{
            event.subject.fx = event.x;
            event.subject.fy = event.y;
        }}
        
        function dragended(event) {{
            if (!event.active) simulation.alphaTarget(0);
            event.subject.fx = null;
            event.subject.fy = null;
        }}
    </script>
</body>
</html>
'''
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    return output_path


def main(
    current_date: datetime = None,
    company_ids: Optional[List[str]] = None,
    periods: Optional[List[str]] = None,
    config: PerformRiskConfig = None,
):
    """
    Main function for performance risk analysis
    
    Args:
        current_date: Current date for comparison. If None, uses datetime.now()
        company_ids: 公司ID列表（按Company.number过滤）
        periods: 时间段列表（单值或[start, end]范围）
        config: 算法配置参数
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
        
        result = analyze_perform_risk(
            session,
            current_date,
            top_n=10,
            company_ids=company_ids,
            periods=periods,
            config=config,
        )
        
        print("\n" + "=" * 60)
        print("分析完成！")
        print("=" * 60)
        
        report = result["report"]
        if len(report) > 0:
            print(f"\n前 10 高风险企业：\n")
            print(report.to_string(index=False))
            print(f"\n完整报告已保存至: reports/perform_risk_report.csv")
            print(f"\n风险合同数量: {len(result['risk_contract_ids'])}")
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

