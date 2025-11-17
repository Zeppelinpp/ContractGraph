"""
交互式网页Demo - 央企穿透式监督知识图谱
整合法律事件监测和循环交易检测功能
"""
import os
from flask import Flask, render_template, jsonify, request
from nebula3.gclient.net import ConnectionPool, Session
from nebula3.Config import Config
from src.settings import settings
from typing import List, Dict, Set

app = Flask(__name__, template_folder='templates', static_folder='static')

# Nebula连接池
connection_pool = None


def init_nebula_pool():
    """Initialize Nebula connection pool"""
    global connection_pool
    config = Config()
    config.max_connection_pool_size = 10
    
    connection_pool = ConnectionPool()
    ok = connection_pool.init(
        [(settings.nebula_config["host"], settings.nebula_config["port"])], 
        config
    )
    if not ok:
        raise Exception("Failed to initialize connection pool")
    return connection_pool


def get_nebula_session() -> Session:
    """Get Nebula session from pool"""
    global connection_pool
    if connection_pool is None:
        init_nebula_pool()
    
    session = connection_pool.get_session(
        settings.nebula_config["user"], 
        settings.nebula_config["password"]
    )
    result = session.execute(f"USE {settings.nebula_config['space']}")
    if not result.is_succeeded():
        raise Exception(f"Failed to use space: {result.error_msg()}")
    return session


# ============================================================================
# 法律事件监测 API
# ============================================================================

@app.route('/api/legal-events/persons')
def api_get_legal_event_persons():
    """获取涉及法律事件的人员列表"""
    session = None
    try:
        session = get_nebula_session()
        
        query = """
        MATCH (p:Person)-[:INVOLVED_IN]->(le:LegalEvent)
        RETURN id(p) as person_id, p.Person.name as person_name, 
               id(le) as event_id, le.LegalEvent.event_name as event_name,
               le.LegalEvent.event_type as event_type, le.LegalEvent.status as event_status
        """
        
        result = session.execute(query)
        if not result.is_succeeded():
            return jsonify({"error": result.error_msg()}), 500
        
        persons = []
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
        
        return jsonify({"persons": persons})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if session:
            session.release()


@app.route('/api/legal-events/trace/<person_id>')
def api_trace_risk(person_id: str):
    """追踪风险传导路径"""
    session = None
    try:
        session = get_nebula_session()
        
        risk_paths = []
        
        # Step 1: Find companies where person is legal representative
        query1 = f"""
        MATCH (p:Person)-[:LEGAL_PERSON]->(c1:Company)
        WHERE id(p) == "{person_id}"
        RETURN id(c1) as company_id, c1.Company.name as company_name
        """
        
        result1 = session.execute(query1)
        if not result1.is_succeeded():
            return jsonify({"error": result1.error_msg()}), 500
        
        companies = result1.as_primitive()
        for company_row in companies:
            if isinstance(company_row, dict):
                company_id = company_row.get('company_id', '')
                company_name = company_row.get('company_name', '') or ''
            else:
                company_id = str(company_row[0]) if len(company_row) > 0 else ''
                company_name = str(company_row[1]) if len(company_row) > 1 and company_row[1] else ''
            
            # Step 2: Find subsidiaries
            query2 = f"""
            MATCH (c1:Company)-[:CONTROLS*0..3]->(c2:Company)
            WHERE id(c1) == "{company_id}"
            RETURN DISTINCT id(c2) as sub_company_id, c2.Company.name as sub_company_name
            """
            
            result2 = session.execute(query2)
            if not result2.is_succeeded():
                continue
            
            sub_companies = result2.as_primitive()
            for sub_row in sub_companies[:10]:
                if isinstance(sub_row, dict):
                    sub_company_id = sub_row.get('sub_company_id', '')
                    sub_company_name = sub_row.get('sub_company_name', '') or ''
                else:
                    sub_company_id = str(sub_row[0]) if len(sub_row) > 0 else ''
                    sub_company_name = str(sub_row[1]) if len(sub_row) > 1 and sub_row[1] else ''
                
                # Step 3: Find contracts
                query3a = f"""
                MATCH (c2:Company)-[:PARTY_A]->(con:Contract)<-[:PARTY_B]-(c3:Company)
                WHERE id(c2) == "{sub_company_id}" AND id(c3) != "{sub_company_id}"
                RETURN id(con) as contract_id, con.Contract.contract_name as contract_name,
                       con.Contract.amount as contract_amount, id(c3) as counterpart_id, 
                       c3.Company.name as counterpart_name
                LIMIT 5
                """
                
                result3a = session.execute(query3a)
                contracts = []
                if result3a.is_succeeded():
                    contracts.extend(result3a.as_primitive())
                
                query3b = f"""
                MATCH (c2:Company)-[:PARTY_B]->(con:Contract)<-[:PARTY_A]-(c3:Company)
                WHERE id(c2) == "{sub_company_id}" AND id(c3) != "{sub_company_id}"
                RETURN id(con) as contract_id, con.Contract.contract_name as contract_name,
                       con.Contract.amount as contract_amount, id(c3) as counterpart_id, 
                       c3.Company.name as counterpart_name
                LIMIT 5
                """
                
                result3b = session.execute(query3b)
                if result3b.is_succeeded():
                    contracts.extend(result3b.as_primitive())
                
                for contract_row in contracts[:5]:
                    if isinstance(contract_row, dict):
                        contract_id = contract_row.get('contract_id', '')
                        contract_name = contract_row.get('contract_name', '') or ''
                        contract_amount = contract_row.get('contract_amount', 0)
                        counterpart_id = contract_row.get('counterpart_id', '')
                        counterpart_name = contract_row.get('counterpart_name', '') or ''
                    else:
                        contract_id = str(contract_row[0]) if len(contract_row) > 0 else ''
                        contract_name = str(contract_row[1]) if len(contract_row) > 1 and contract_row[1] else ''
                        contract_amount = float(contract_row[2]) if len(contract_row) > 2 and contract_row[2] else 0
                        counterpart_id = str(contract_row[3]) if len(contract_row) > 3 else ''
                        counterpart_name = str(contract_row[4]) if len(contract_row) > 4 and contract_row[4] else ''
                    
                    path = {
                        "nodes": [
                            {"id": person_id, "type": "Person", "label": "人员"},
                            {"id": company_id, "type": "Company", "label": company_name},
                            {"id": sub_company_id, "type": "Company", "label": sub_company_name},
                            {"id": contract_id, "type": "Contract", "label": contract_name, "amount": contract_amount},
                            {"id": counterpart_id, "type": "Company", "label": counterpart_name}
                        ],
                        "edges": [
                            {"source": person_id, "target": company_id, "type": "LEGAL_PERSON"},
                            {"source": company_id, "target": sub_company_id, "type": "CONTROLS"},
                            {"source": sub_company_id, "target": contract_id, "type": "PARTY"},
                            {"source": contract_id, "target": counterpart_id, "type": "PARTY"}
                        ]
                    }
                    risk_paths.append(path)
        
        return jsonify({"person_id": person_id, "risk_paths": risk_paths[:20]})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if session:
            session.release()


# ============================================================================
# 循环交易检测 API
# ============================================================================

def calculate_similarity(amounts: List[float]) -> float:
    """计算金额相似度"""
    if len(amounts) < 2:
        return 100.0
    
    avg = sum(amounts) / len(amounts)
    if avg == 0:
        return 0.0
    
    variance = sum((x - avg) ** 2 for x in amounts) / len(amounts)
    std_dev = variance ** 0.5
    cv = std_dev / avg
    
    similarity = (1 - cv) * 100
    return max(0.0, min(100.0, similarity))


def extract_amount_from_properties(properties: str) -> float:
    """从边属性中提取金额"""
    try:
        if "交易金额:" in properties:
            amount_str = properties.split("交易金额:")[1].split(",")[0]
            return float(amount_str)
    except:
        pass
    return 0.0


@app.route('/api/circular-trades/detect')
def api_detect_circular_trades():
    """检测循环交易"""
    threshold = float(request.args.get('threshold', 90.0))
    max_depth = int(request.args.get('max_depth', 5))
    
    session = None
    try:
        session = get_nebula_session()
        
        # Get sample companies
        query = """
        MATCH (c:Company)
        RETURN id(c) as company_id, c.Company.name as name
        LIMIT 30
        """
        
        result = session.execute(query)
        if not result.is_succeeded():
            return jsonify({"error": result.error_msg()}), 500
        
        all_cycles = []
        rows = result.as_primitive()
        
        for row in rows[:20]:
            company_id = row.get('company_id', '')
            company_name = row.get('name', '') or company_id
            
            # Find cycles from this company
            cycles = find_circular_trades_for_company(session, company_id, company_name, max_depth)
            
            for cycle in cycles:
                if cycle["similarity"] >= threshold:
                    all_cycles.append(cycle)
        
        # Remove duplicates
        unique_cycles = []
        seen_paths = set()
        for cycle in all_cycles:
            path_key = tuple(sorted(cycle["path"]))
            if path_key not in seen_paths:
                seen_paths.add(path_key)
                unique_cycles.append(cycle)
        
        return jsonify({"cycles": unique_cycles[:10]})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if session:
            session.release()


def find_circular_trades_for_company(session: Session, start_company: str, start_company_name: str, max_depth: int) -> List[Dict]:
    """查找从指定公司开始的循环交易"""
    cycles = []
    
    def dfs(current: str, path: List[str], visited_edges: Set[str], amounts: List[float], depth: int):
        if depth > max_depth:
            return
        
        query = f"""
        MATCH (c:Company)-[e:TRADES_WITH]->(target:Company)
        WHERE id(c) == "{current}"
        RETURN id(target) as target_id, e.properties as properties
        """
        
        result = session.execute(query)
        if not result.is_succeeded():
            return
        
        rows = result.as_primitive()
        for row in rows:
            target_id = row.get('target_id', '')
            properties = str(row.get('properties', ''))
            edge_key = f"{current}->{target_id}"
            
            if edge_key in visited_edges:
                continue
            
            amount = extract_amount_from_properties(properties)
            new_path = path + [target_id]
            new_amounts = amounts + [amount]
            new_visited = visited_edges | {edge_key}
            
            if target_id == start_company and len(path) >= 2:
                similarity = calculate_similarity(new_amounts)
                cycles.append({
                    "path": [start_company] + new_path,
                    "amounts": new_amounts,
                    "similarity": similarity,
                    "start_company_name": start_company_name
                })
            else:
                dfs(target_id, new_path, new_visited, new_amounts, depth + 1)
    
    dfs(start_company, [], set(), [], 0)
    return cycles


@app.route('/api/graph/neighbors/<node_id>')
def api_get_neighbors(node_id: str):
    """获取节点两跳内的邻居节点"""
    session = None
    try:
        session = get_nebula_session()
        
        # Query for 2-hop neighbors
        query = f"""
        MATCH (n)-[e1]-(n1)-[e2]-(n2)
        WHERE id(n) == "{node_id}"
        RETURN DISTINCT 
            id(n) as n0_id, tags(n)[0] as n0_type,
            id(n1) as n1_id, tags(n1)[0] as n1_type,
            id(n2) as n2_id, tags(n2)[0] as n2_type,
            type(e1) as e1_type,
            type(e2) as e2_type
        LIMIT 100
        """
        
        result = session.execute(query)
        if not result.is_succeeded():
            return jsonify({"error": result.error_msg()}), 500
        
        nodes_dict = {}
        edges_list = []
        
        rows = result.as_primitive()
        for row in rows:
            # Add nodes
            for i in range(3):
                nid = row.get(f'n{i}_id', '')
                ntype = row.get(f'n{i}_type', '')
                if nid and nid not in nodes_dict:
                    nodes_dict[nid] = {"id": nid, "type": ntype, "label": nid}
            
            # Add edges
            n0_id = row.get('n0_id', '')
            n1_id = row.get('n1_id', '')
            n2_id = row.get('n2_id', '')
            e1_type = row.get('e1_type', '')
            e2_type = row.get('e2_type', '')
            
            if n0_id and n1_id:
                edges_list.append({"source": n0_id, "target": n1_id, "type": e1_type})
            if n1_id and n2_id:
                edges_list.append({"source": n1_id, "target": n2_id, "type": e2_type})
        
        # Get node properties
        for nid in list(nodes_dict.keys())[:50]:
            prop_query = f"""
            MATCH (n)
            WHERE id(n) == "{nid}"
            RETURN properties(n) as props
            """
            prop_result = session.execute(prop_query)
            if prop_result.is_succeeded():
                prop_rows = prop_result.as_primitive()
                if prop_rows and len(prop_rows) > 0:
                    props = prop_rows[0].get('props', {})
                    if isinstance(props, dict):
                        name = props.get('name', props.get('contract_name', props.get('event_name', '')))
                        if name:
                            nodes_dict[nid]["label"] = name
        
        return jsonify({
            "nodes": list(nodes_dict.values()),
            "edges": edges_list
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if session:
            session.release()


# ============================================================================
# 页面路由
# ============================================================================

@app.route('/')
def index():
    """主页"""
    return render_template('index.html')


if __name__ == '__main__':
    print("启动交互式Demo...")
    print(f"Nebula Graph连接: {settings.nebula_config['host']}:{settings.nebula_config['port']}")
    print(f"图空间: {settings.nebula_config['space']}")
    
    try:
        init_nebula_pool()
        print("✓ Nebula连接池初始化成功")
    except Exception as e:
        print(f"✗ Nebula连接失败: {e}")
        exit(1)
    
    app.run(host='0.0.0.0', port=9010, debug=True)

