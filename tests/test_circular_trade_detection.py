"""
循环交易监测Demo

功能：检测公司间的循环交易模式，当金额相似度 > 90% 时触发预警

查询逻辑：
1. 从任意公司出发，通过TRADES_WITH关系遍历
2. 检测是否形成闭环（回到起始公司）
3. 提取闭环中所有交易的金额
4. 计算金额相似度
5. 如果相似度 > 90%，触发预警
"""
from typing import List, Dict, Set, Tuple
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


def calculate_similarity(amounts: List[float]) -> float:
    """Calculate similarity of amounts (coefficient of variation)"""
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
    """Extract amount from edge properties string"""
    try:
        if "交易金额:" in properties:
            amount_str = properties.split("交易金额:")[1].split(",")[0]
            return float(amount_str)
    except:
        pass
    return 0.0


def find_circular_trades(session: Session, start_company: str, start_company_name: str = '', max_depth: int = 5) -> List[Dict]:
    """
    Find circular trade patterns starting from a company
    
    Args:
        session: Nebula session
        start_company: Starting company node ID
        start_company_name: Starting company name
        max_depth: Maximum traversal depth
    
    Returns:
        List of circular trade patterns with details
    """
    # Cache company names
    company_names = {start_company: start_company_name} if start_company_name else {}
    
    def get_company_name(company_id: str) -> str:
        """Get company name, with caching"""
        if company_id in company_names:
            return company_names[company_id]
        
        query = f"""
        MATCH (c:Company)
        WHERE id(c) == "{company_id}"
        RETURN c.Company.name as name
        LIMIT 1
        """
        result = session.execute(query)
        if result.is_succeeded():
            rows = result.as_primitive()
            if rows and len(rows) > 0:
                name = rows[0].get('name', '') if isinstance(rows[0], dict) else ''
                company_names[company_id] = name or company_id
                return company_names[company_id]
        company_names[company_id] = company_id
        return company_id
    
    def dfs(current: str, path: List[str], path_names: List[str], visited_edges: Set[str], amounts: List[float]) -> List[Dict]:
        """Depth-first search to find cycles"""
        if len(path) > max_depth:
            return []
        
        cycles = []
        
        query = f"""
        MATCH (c:Company)-[e:TRADES_WITH]->(target:Company)
        WHERE id(c) == "{current}"
        RETURN id(target) as target_id, e.properties as properties
        """
        
        result = session.execute(query)
        if not result.is_succeeded():
            return cycles
        
        try:
            rows = result.as_primitive()
            for row in rows:
                target_id = row.get('target_id', '')
                properties = str(row.get('properties', ''))
                edge_key = f"{current}->{target_id}"
                
                if edge_key in visited_edges:
                    continue
                
                amount = extract_amount_from_properties(properties)
                target_name = get_company_name(target_id)
                new_path = path + [target_id]
                new_path_names = path_names + [target_name]
                new_amounts = amounts + [amount]
                new_visited = visited_edges | {edge_key}
                
                if target_id == start_company and len(path) >= 2:
                    similarity = calculate_similarity(new_amounts)
                    cycles.append({
                        "path": [start_company] + new_path,
                        "path_names": [start_company_name or start_company] + new_path_names,
                        "amounts": new_amounts,
                        "similarity": similarity,
                        "cycle_length": len(new_path)
                    })
                else:
                    cycles.extend(dfs(target_id, new_path, new_path_names, new_visited, new_amounts))
        except Exception as e:
            print(f"Error processing query results: {e}")
        
        return cycles
    
    return dfs(start_company, [], [start_company_name or start_company], set(), [])


def detect_circular_trades(session: Session, similarity_threshold: float = 90.0) -> List[Dict]:
    """
    Detect all circular trades in the graph
    
    Args:
        session: Nebula session
        similarity_threshold: Minimum similarity to trigger alert (default: 90%)
    
    Returns:
        List of detected circular trades with similarity >= threshold
    """
    query = """
    MATCH (c:Company)
    RETURN id(c) as company_id, c.Company.name as name
    LIMIT 50
    """
    
    result = session.execute(query)
    if not result.is_succeeded():
        print(f"Error querying companies: {result.error_msg()}")
        return []
    
    all_cycles = []
    processed_companies = set()
    
    try:
        rows = result.as_primitive()
        for row in rows:
            company_id = row.get('company_id', '')
            company_name = row.get('name', '') or ''
            
            if company_id in processed_companies:
                continue
            
            cycles = find_circular_trades(session, company_id, company_name)
            for cycle in cycles:
                cycle["start_company_id"] = company_id
                cycle["start_company_name"] = company_name or company_id
                all_cycles.append(cycle)
            
            processed_companies.add(company_id)
    except Exception as e:
        print(f"Error processing companies: {e}")
    
    filtered_cycles = [
        cycle for cycle in all_cycles 
        if cycle["similarity"] >= similarity_threshold
    ]
    
    return filtered_cycles


def format_cycle_info(cycle: Dict) -> str:
    """Format cycle information for display"""
    path = cycle["path"]
    path_names = cycle.get("path_names", path)
    amounts = cycle["amounts"]
    similarity = cycle["similarity"]
    
    info = f"\n循环交易检测到闭环:\n"
    info += f"起始公司: {cycle.get('start_company_name', cycle.get('start_company_id', ''))}\n"
    info += f"交易路径: {' -> '.join(path_names)}\n"
    info += f"交易金额: {', '.join([f'{amt:,.2f}' for amt in amounts])}\n"
    info += f"金额相似度: {similarity:.2f}%\n"
    info += f"循环长度: {cycle['cycle_length']} 个节点\n"
    
    if similarity >= 90:
        info += "⚠️  预警: 金额相似度超过90%，疑似循环交易！\n"
    
    return info


def main():
    """Main function to demonstrate circular trade detection"""
    print("=" * 60)
    print("循环交易监测 Demo")
    print("=" * 60)
    
    session = None
    try:
        session = get_nebula_session()
        print("✓ 成功连接到 Nebula Graph\n")
        
        print("正在检测循环交易模式...")
        cycles = detect_circular_trades(session, similarity_threshold=90.0)
        
        if not cycles:
            print("\n未检测到符合条件的循环交易模式")
            print("(相似度 >= 90% 的循环交易)")
        else:
            print(f"\n检测到 {len(cycles)} 个可疑的循环交易模式:\n")
            for i, cycle in enumerate(cycles, 1):
                print(f"[{i}]" + format_cycle_info(cycle))
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if session:
            session.release()


if __name__ == "__main__":
    main()

