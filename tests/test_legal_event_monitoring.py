"""
法律事件监测Demo

功能：追踪法律事件对企业及其关联方的影响（风险传导监督）

场景描述：某公司的法人代表被限高，追踪对其控股公司及交易对手的影响

图谱路径：
Person (被限高)
  → [LEGAL_PERSON] →
Company_A (母公司)
  → [CONTROLS] →
Company_B (子公司)
  → [PARTY_A] →
Contract_X
  → [PARTY_B] ←
Company_C (交易对手)
"""
from typing import List, Dict, Set
from nebula3.gclient.net import ConnectionPool, Session
from nebula3.Config import Config
from src.settings import settings


def get_nebula_session() -> Session:
    """Create and return Nebula session"""
    config = Config()
    config.max_connection_pool_size = 10
    
    connection_pool = ConnectionPool()
    ok = connection_pool.init(
        [(settings.nebula_config["host"], settings.nebula_config["port"])], 
        config
    )
    if not ok:
        raise Exception("Failed to initialize connection pool")
    
    session = connection_pool.get_session(
        settings.nebula_config["user"], 
        settings.nebula_config["password"]
    )
    result = session.execute(f"USE {settings.nebula_config['space']}")
    if not result.is_succeeded():
        raise Exception(f"Failed to use space: {result.error_msg()}")
    return session


def find_persons_with_legal_events(session: Session) -> List[Dict]:
    """Find persons involved in legal events"""
    query = """
    MATCH (p:Person)-[:INVOLVED_IN]->(le:LegalEvent)
    RETURN id(p) as person_id, p.Person.name as person_name, 
           id(le) as event_id, le.LegalEvent.event_name as event_name,
           le.LegalEvent.event_type as event_type, le.LegalEvent.status as event_status
    """
    
    result = session.execute(query)
    if not result.is_succeeded():
        print(f"Error querying persons with legal events: {result.error_msg()}")
        return []
    
    persons = []
    try:
        rows = result.as_primitive()
        for row in rows:
            if isinstance(row, dict):
                persons.append({
                    "person_id": row.get('person_id', ''),
                    "person_name": row.get('person_name', '') or '',
                    "event_id": row.get('event_id', ''),
                    "event_name": row.get('event_name', '') or '',
                    "event_type": row.get('event_type', '') or '',
                    "event_status": row.get('event_status', '') or ''
                })
            else:
                persons.append({
                    "person_id": str(row[0]) if len(row) > 0 else '',
                    "person_name": str(row[1]) if len(row) > 1 and row[1] else '',
                    "event_id": str(row[2]) if len(row) > 2 else '',
                    "event_name": str(row[3]) if len(row) > 3 and row[3] else '',
                    "event_type": str(row[4]) if len(row) > 4 and row[4] else '',
                    "event_status": str(row[5]) if len(row) > 5 and row[5] else ''
                })
    except Exception as e:
        print(f"Error processing query results: {e}")
        import traceback
        traceback.print_exc()
    
    return persons


def trace_risk_propagation(session: Session, person_id: str) -> Dict:
    """
    Trace risk propagation from a person to related companies and contracts
    
    Path: Person → LEGAL_PERSON → Company → CONTROLS → Company → PARTY_A → Contract → PARTY_B → Company
    """
    risk_paths = []
    
    # Step 1: Find companies where person is legal representative
    query1 = f"""
    MATCH (p:Person)-[:LEGAL_PERSON]->(c1:Company)
    WHERE id(p) == "{person_id}"
    RETURN id(c1) as company_id, c1.Company.name as company_name
    """
    
    result1 = session.execute(query1)
    if not result1.is_succeeded():
        print(f"Error querying person companies: {result1.error_msg()}")
        return {"person_id": person_id, "risk_paths": []}
    
    try:
        companies = result1.as_primitive()
        for company_row in companies:
            if isinstance(company_row, dict):
                company_id = company_row.get('company_id', '')
                company_name = company_row.get('company_name', '') or ''
            else:
                company_id = str(company_row[0]) if len(company_row) > 0 else ''
                company_name = str(company_row[1]) if len(company_row) > 1 and company_row[1] else ''
            
            # Step 2: Find subsidiaries (including self)
            query2 = f"""
            MATCH (c1:Company)-[:CONTROLS*0..3]->(c2:Company)
            WHERE id(c1) == "{company_id}"
            RETURN DISTINCT id(c2) as sub_company_id, c2.Company.name as sub_company_name
            """
            
            result2 = session.execute(query2)
            if not result2.is_succeeded():
                continue
            
            sub_companies = result2.as_primitive()
            for sub_row in sub_companies:
                if isinstance(sub_row, dict):
                    sub_company_id = sub_row.get('sub_company_id', '')
                    sub_company_name = sub_row.get('sub_company_name', '') or ''
                else:
                    sub_company_id = str(sub_row[0]) if len(sub_row) > 0 else ''
                    sub_company_name = str(sub_row[1]) if len(sub_row) > 1 and sub_row[1] else ''
                
                # Step 3: Find contracts where subsidiary is a party
                # Query contracts where subsidiary is PARTY_A
                contracts = []
                
                query3a = f"""
                MATCH (c2:Company)-[:PARTY_A]->(con:Contract)<-[:PARTY_B]-(c3:Company)
                WHERE id(c2) == "{sub_company_id}" AND id(c3) != "{sub_company_id}"
                RETURN id(con) as contract_id, con.Contract.contract_name as contract_name,
                       con.Contract.amount as contract_amount, id(c3) as counterpart_id, c3.Company.name as counterpart_name
                LIMIT 20
                """
                
                result3a = session.execute(query3a)
                if result3a.is_succeeded():
                    contracts.extend(result3a.as_primitive())
                
                # Query contracts where subsidiary is PARTY_B
                query3b = f"""
                MATCH (c2:Company)-[:PARTY_B]->(con:Contract)<-[:PARTY_A]-(c3:Company)
                WHERE id(c2) == "{sub_company_id}" AND id(c3) != "{sub_company_id}"
                RETURN id(con) as contract_id, con.Contract.contract_name as contract_name,
                       con.Contract.amount as contract_amount, id(c3) as counterpart_id, c3.Company.name as counterpart_name
                LIMIT 20
                """
                
                result3b = session.execute(query3b)
                if result3b.is_succeeded():
                    contracts.extend(result3b.as_primitive())
                
                if not contracts:
                    continue
                for contract_row in contracts:
                    if isinstance(contract_row, dict):
                        contract_id = contract_row.get('contract_id', '')
                        contract_name = contract_row.get('contract_name', '') or ''
                        contract_amount = contract_row.get('contract_amount', '') or ''
                        counterpart_id = contract_row.get('counterpart_id', '')
                        counterpart_name = contract_row.get('counterpart_name', '') or ''
                    else:
                        contract_id = str(contract_row[0]) if len(contract_row) > 0 else ''
                        contract_name = str(contract_row[1]) if len(contract_row) > 1 and contract_row[1] else ''
                        contract_amount = str(contract_row[2]) if len(contract_row) > 2 and contract_row[2] else ''
                        counterpart_id = str(contract_row[3]) if len(contract_row) > 3 else ''
                        counterpart_name = str(contract_row[4]) if len(contract_row) > 4 and contract_row[4] else ''
                    
                    # Build path structure
                    path = {
                        "nodes": [
                            {
                                "id": person_id,
                                "type": "Person",
                                "properties": {}
                            },
                            {
                                "id": company_id,
                                "type": "Company",
                                "properties": {"name": company_name}
                            },
                            {
                                "id": sub_company_id,
                                "type": "Company",
                                "properties": {"name": sub_company_name}
                            },
                            {
                                "id": contract_id,
                                "type": "Contract",
                                "properties": {
                                    "contract_name": contract_name,
                                    "amount": contract_amount
                                }
                            },
                            {
                                "id": counterpart_id,
                                "type": "Company",
                                "properties": {"name": counterpart_name}
                            }
                        ],
                        "edges": [
                            {"from": person_id, "to": company_id, "type": "LEGAL_PERSON"},
                            {"from": company_id, "to": sub_company_id, "type": "CONTROLS"},
                            {"from": sub_company_id, "to": contract_id, "type": "PARTY"},
                            {"from": contract_id, "to": counterpart_id, "type": "PARTY"}
                        ]
                    }
                    
                    risk_paths.append(path)
    except Exception as e:
        print(f"Error processing risk propagation: {e}")
        import traceback
        traceback.print_exc()
    
    return {
        "person_id": person_id,
        "risk_paths": risk_paths
    }


def get_person_companies(session: Session, person_id: str) -> List[Dict]:
    """Get companies where the person is legal representative"""
    query = f"""
    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
    WHERE id(p) == "{person_id}"
    RETURN id(c) as company_id, c.Company.name as company_name, 
           c.Company.company_type as company_type
    """
    
    result = session.execute(query)
    if not result.is_succeeded():
        return []
    
    companies = []
    try:
        rows = result.as_primitive()
        for row in rows:
            if isinstance(row, dict):
                companies.append({
                    "company_id": row.get('company_id', ''),
                    "company_name": row.get('company_name', '') or '',
                    "company_type": row.get('company_type', '') or ''
                })
            else:
                companies.append({
                    "company_id": str(row[0]) if len(row) > 0 else '',
                    "company_name": str(row[1]) if len(row) > 1 and row[1] else '',
                    "company_type": str(row[2]) if len(row) > 2 and row[2] else ''
                })
    except Exception as e:
        print(f"Error processing companies: {e}")
        import traceback
        traceback.print_exc()
    
    return companies


def format_risk_report(person_info: Dict, risk_data: Dict) -> str:
    """Format risk propagation report"""
    report = f"\n{'='*60}\n"
    report += f"风险传导监测报告\n"
    report += f"{'='*60}\n\n"
    
    report += f"涉及人员: {person_info['person_name']} ({person_info['person_id']})\n"
    report += f"法律事件: {person_info['event_name']} ({person_info['event_type']})\n"
    report += f"事件状态: {person_info['event_status']}\n\n"
    
    risk_paths = risk_data.get("risk_paths", [])
    if not risk_paths:
        report += "未发现风险传导路径\n"
        return report
    
    report += f"发现 {len(risk_paths)} 条风险传导路径:\n\n"
    
    affected_companies = set()
    affected_contracts = set()
    
    for i, path in enumerate(risk_paths[:10], 1):
        report += f"[路径 {i}]\n"
        nodes = path["nodes"]
        edges = path["edges"]
        
        path_str = ""
        for j, node in enumerate(nodes):
            node_type = node["type"]
            props = node["properties"]
            
            if node_type == "Person":
                path_str += f"{props.get('name', node['id'])}"
            elif node_type == "Company":
                company_name = props.get('name', node['id'])
                path_str += f"{company_name}"
                affected_companies.add(company_name)
            elif node_type == "Contract":
                contract_name = props.get('contract_name', node['id'])
                path_str += f"{contract_name}"
                affected_contracts.add(contract_name)
            
            if j < len(nodes) - 1:
                edge_type = edges[j]["type"] if j < len(edges) else ""
                path_str += f" --[{edge_type}]--> "
        
        report += path_str + "\n\n"
    
    report += f"\n风险影响范围:\n"
    report += f"- 受影响公司数量: {len(affected_companies)}\n"
    report += f"- 受影响合同数量: {len(affected_contracts)}\n"
    
    if affected_companies:
        report += f"\n受影响公司列表:\n"
        for company in list(affected_companies)[:10]:
            report += f"  - {company}\n"
    
    if affected_contracts:
        report += f"\n受影响合同列表:\n"
        for contract in list(affected_contracts)[:10]:
            report += f"  - {contract}\n"
    
    report += f"\n⚠️  风险预警: 建议对上述公司和合同进行风险评估！\n"
    
    return report


def main():
    """Main function to demonstrate legal event monitoring"""
    print("=" * 60)
    print("法律事件监测 Demo")
    print("=" * 60)
    
    session = None
    try:
        session = get_nebula_session()
        print("✓ 成功连接到 Nebula Graph\n")
        
        print("正在查找涉及法律事件的人员...")
        persons = find_persons_with_legal_events(session)
        
        if not persons:
            print("\n未找到涉及法律事件的人员")
            return
        
        print(f"\n找到 {len(persons)} 个涉及法律事件的人员\n")
        
        for person_info in persons[:5]:
            print(f"正在分析: {person_info['person_name']} ({person_info['event_name']})...")
            
            risk_data = trace_risk_propagation(session, person_info['person_id'])
            companies = get_person_companies(session, person_info['person_id'])
            
            if companies:
                print(f"  该人员担任 {len(companies)} 家公司的法人代表")
            
            if risk_data.get("risk_paths"):
                report = format_risk_report(person_info, risk_data)
                print(report)
            else:
                print(f"  未发现风险传导路径\n")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if session:
            session.release()


if __name__ == "__main__":
    main()

