"""
场景四：关联方串通网络分析

检测关联方串通网络，包括轮流中标、围标等模式
"""

import os
import pandas as pd
import json
from collections import defaultdict, Counter
from nebula_utils import get_nebula_session, execute_query

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")


def calculate_rotation_score(win_sequence):
    """
    计算轮换分数：检测是否存在规律的轮流中标
    完美轮换 = 1.0，完全随机 = 0.0
    """
    if len(win_sequence) < 3:
        return 0.0
    
    # 统计每个公司出现的次数
    counter = Counter(win_sequence)
    
    # 方差越小，说明分布越均匀（越像轮换）
    counts = list(counter.values())
    if len(counts) < 2:
        return 0.0
    
    mean_count = sum(counts) / len(counts)
    variance = sum((c - mean_count) ** 2 for c in counts) / len(counts)
    
    # 归一化：方差为 0 时分数为 1
    max_variance = mean_count ** 2
    rotation_score = 1 - min(variance / max_variance, 1.0) if max_variance > 0 else 0
    
    return rotation_score


def is_near_threshold(amount, thresholds=[1000000, 3000000, 5000000, 10000000], margin=0.05):
    """
    检测金额是否刻意卡在审批阈值附近
    """
    for threshold in thresholds:
        lower = threshold * (1 - margin)
        upper = threshold
        if lower <= amount <= upper:
            return True
    return False


def analyze_collusion_patterns(company_cluster, session):
    """
    分析公司集群的串通模式
    """
    # 查询集群内公司作为乙方的合同
    company_ids_str = ', '.join([f'"{c}"' for c in company_cluster])
    contract_query = f"""
    MATCH (c:Company)-[:PARTY_B]->(con:Contract)
    WHERE id(c) IN [{company_ids_str}]
    RETURN id(c) as company_id, id(con) as contract_id,
           con.Contract.sign_date as sign_date,
           con.Contract.amount as amount
    ORDER BY sign_date
    """
    rows = execute_query(session, contract_query)
    
    if len(rows) == 0:
        return {'risk_score': 0.0}
    
    # 转换为 DataFrame
    contracts_data = []
    for row in rows:
        contracts_data.append({
            'company_id': row.get('company_id', ''),
            'contract_id': row.get('contract_id', ''),
            'sign_date': row.get('sign_date', ''),
            'amount': float(row.get('amount', 0) or 0)
        })
    
    cluster_contracts = pd.DataFrame(contracts_data)
    cluster_contracts['sign_date'] = pd.to_datetime(cluster_contracts['sign_date'])
    cluster_contracts = cluster_contracts.sort_values('sign_date')
    
    # 计算中标轮换度
    win_companies = cluster_contracts['company_id'].tolist()
    rotation_score = calculate_rotation_score(win_companies)
    
    # 特征 2: 合同金额相似度
    amounts = cluster_contracts['amount'].dropna()
    if len(amounts) >= 2:
        amount_std = amounts.std()
        amount_mean = amounts.mean()
        amount_cv = amount_std / amount_mean if amount_mean > 0 else 0
        amount_similarity = 1 - min(amount_cv, 1.0)  # CV 越小，相似度越高
    else:
        amount_similarity = 0
    
    # 特征 3: 合同金额卡阈值检测
    threshold_count = sum(1 for amt in amounts if is_near_threshold(amt))
    threshold_ratio = threshold_count / len(amounts) if len(amounts) > 0 else 0
    
    # 特征 4: 网络密度（关联关系的紧密程度）
    # 查询集群内公司之间的关联数量
    relation_query = f"""
    MATCH (c1:Company)-[e:LEGAL_PERSON|CONTROLS]-(c2:Company)
    WHERE id(c1) IN [{company_ids_str}] AND id(c2) IN [{company_ids_str}]
    RETURN count(e) as relation_count
    """
    relation_rows = execute_query(session, relation_query)
    internal_relations = relation_rows[0].get('relation_count', 0) if relation_rows else 0
    
    max_possible_relations = len(company_cluster) * (len(company_cluster) - 1) / 2
    density = internal_relations / max_possible_relations if max_possible_relations > 0 else 0
    
    # 特征 5: 关联类型强度
    has_strong_relation = len(company_cluster) >= 2  # 如果能聚类到一起，说明有关联
    
    # 综合风险分数
    risk_score = (
        rotation_score * 0.3 +
        amount_similarity * 0.2 +
        threshold_ratio * 0.2 +
        density * 0.2 +
        (0.1 if has_strong_relation else 0)
    )
    
    return {
        'risk_score': risk_score,
        'rotation_score': rotation_score,
        'amount_similarity': amount_similarity,
        'threshold_ratio': threshold_ratio,
        'network_density': density,
        'contract_count': len(cluster_contracts),
        'total_amount': amounts.sum(),
        'avg_amount': amounts.mean()
    }


def detect_collusion_network(session, min_cluster_size=3):
    """
    检测关联方串通网络
    
    Returns:
        list: 可疑串通网络列表
    """
    # 查询所有公司
    company_query = """
    MATCH (c:Company)
    RETURN id(c) as company_id
    """
    companies = execute_query(session, company_query)
    all_companies = {row.get('company_id', '') for row in companies if row.get('company_id', '')}
    
    # 构建关联关系图（字典形式）
    relation_graph = defaultdict(set)
    
    # 添加共享法人的边
    legal_person_query = """
    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
    WITH p, collect(id(c)) as companies
    WHERE size(companies) >= 2
    RETURN companies
    """
    rows = execute_query(session, legal_person_query)
    for row in rows:
        companies = row.get('companies', [])
        # 为同一法人的所有公司两两建立边
        for i, c1 in enumerate(companies):
            for c2 in companies[i+1:]:
                if c1 and c2:
                    relation_graph[c1].add(c2)
                    relation_graph[c2].add(c1)
    
    # 添加控股关系的边
    controls_query = """
    MATCH (c1:Company)-[:CONTROLS]-(c2:Company)
    RETURN id(c1) as c1, id(c2) as c2
    """
    rows = execute_query(session, controls_query)
    for row in rows:
        c1 = row.get('c1', '')
        c2 = row.get('c2', '')
        if c1 and c2:
            relation_graph[c1].add(c2)
            relation_graph[c2].add(c1)
    
    # 社区检测：找出连通的公司集群（简化版BFS）
    visited = set()
    communities = []
    
    for node in all_companies:
        if node not in visited:
            # BFS找出连通分量
            queue = [node]
            community = set()
            
            while queue:
                current = queue.pop(0)
                if current in visited:
                    continue
                
                visited.add(current)
                community.add(current)
                
                # 添加邻居节点
                for neighbor in relation_graph.get(current, []):
                    if neighbor not in visited:
                        queue.append(neighbor)
            
            if len(community) >= min_cluster_size:
                communities.append(list(community))
    
    suspicious_networks = []
    
    for comm_idx, comm in enumerate(communities):
        # 分析这个集群的可疑行为
        collusion_features = analyze_collusion_patterns(comm, session)
        
        if collusion_features['risk_score'] >= 0.5:
            suspicious_networks.append({
                'network_id': f"NETWORK_{comm_idx + 1}",
                'companies': comm,
                'size': len(comm),
                **collusion_features
            })
    
    return suspicious_networks


def main():
    print("=" * 70)
    print("关联方串通网络分析")
    print("=" * 70)
    
    session = None
    try:
        session = get_nebula_session()
        
        print("\n[1/3] 构建关联关系图...")
        suspicious_networks = detect_collusion_network(session, min_cluster_size=3)
        
        print(f"  发现可疑串通网络数: {len(suspicious_networks)}")
        
        if len(suspicious_networks) == 0:
            print("\n未发现可疑的串通网络")
            return
        
        print("\n[2/3] 分析串通模式...")
        
        # 查询公司信息用于展示
        company_query = """
        MATCH (c:Company)
        RETURN id(c) as company_id, c.Company.name as name
        """
        companies = execute_query(session, company_query)
        company_names = {row.get('company_id', ''): row.get('name', '') for row in companies}
        
        # 生成详细报告
        report_data = []
        for network in suspicious_networks:
            company_list = network['companies'][:5]
            company_names_str = ', '.join([
                company_names.get(c, str(c)) for c in company_list
            ]) + ('...' if len(network['companies']) > 5 else '')
            
            report_data.append({
                'network_id': network['network_id'],
                'company_count': network['size'],
                'risk_score': network['risk_score'],
                'rotation_score': network.get('rotation_score', 0),
                'amount_similarity': network.get('amount_similarity', 0),
                'threshold_ratio': network.get('threshold_ratio', 0),
                'network_density': network.get('network_density', 0),
                'contract_count': network.get('contract_count', 0),
                'total_amount': network.get('total_amount', 0),
                'companies': company_names_str
            })
        
        report_df = pd.DataFrame(report_data)
        report_df = report_df.sort_values('risk_score', ascending=False)
        
        print("\n[3/3] 生成报告...")
        
        # 确保报告目录存在
        os.makedirs(REPORTS_DIR, exist_ok=True)
        
        output_file = os.path.join(REPORTS_DIR, 'collusion_network_report.csv')
        report_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print("\n前 5 高风险串通网络：\n")
        for idx, row in report_df.head(5).iterrows():
            print(f"{row['network_id']}:")
            print(f"  公司数量: {row['company_count']}")
            print(f"  风险分数: {row['risk_score']:.4f}")
            print(f"  轮换分数: {row['rotation_score']:.4f}")
            print(f"  金额相似度: {row['amount_similarity']:.4f}")
            print(f"  卡阈值比例: {row['threshold_ratio']:.2%}")
            print(f"  网络密度: {row['network_density']:.4f}")
            print(f"  合同总数: {row['contract_count']}")
            print(f"  涉及金额: ¥{row['total_amount']:,.2f}")
            print(f"  公司列表: {row['companies']}")
            print()
        
        print(f"完整报告已保存至: reports/collusion_network_report.csv")
    
    finally:
        if session:
            session.release()


if __name__ == '__main__':
    main()
