"""
场景四：关联方串通网络分析

检测关联方串通网络，包括轮流中标、围标等模式
"""

import os
import csv
import pandas as pd
import json
from collections import defaultdict, Counter

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
GRAPH_DIR = os.path.join(BASE_DIR, "data", "graph_data")
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


def analyze_collusion_patterns(company_cluster, graph_data_dir, relation_graph):
    """
    分析公司集群的串通模式
    """
    # 加载合同数据
    contracts_file = os.path.join(graph_data_dir, 'nodes_contract.csv')
    party_file = os.path.join(graph_data_dir, 'edges_party.csv')
    
    contracts_df = pd.read_csv(contracts_file)
    party_edges_df = pd.read_csv(party_file)
    
    # 特征 1: 轮流中标模式检测
    # 找出集群内公司作为乙方的合同
    cluster_contracts = party_edges_df[
        (party_edges_df['from_node'].isin(company_cluster)) &
        (party_edges_df['edge_type'] == 'PARTY_B')
    ]
    
    if len(cluster_contracts) == 0:
        return {'risk_score': 0.0}
    
    # 按签订日期排序
    cluster_contracts = cluster_contracts.merge(
        contracts_df[['node_id', 'sign_date', 'amount']],
        left_on='to_node',
        right_on='node_id'
    )
    cluster_contracts['sign_date'] = pd.to_datetime(cluster_contracts['sign_date'])
    cluster_contracts = cluster_contracts.sort_values('sign_date')
    
    # 计算中标轮换度
    win_companies = cluster_contracts['from_node'].tolist()
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
    # 简化计算：计算集群内公司之间的关联数量
    internal_relations = 0
    max_possible_relations = len(company_cluster) * (len(company_cluster) - 1) / 2
    
    for i, comp1 in enumerate(company_cluster):
        for comp2 in company_cluster[i+1:]:
            if comp1 in relation_graph and comp2 in relation_graph[comp1]:
                internal_relations += 1
    
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


def detect_collusion_network(graph_data_dir, min_cluster_size=3):
    """
    检测关联方串通网络
    
    Returns:
        list: 可疑串通网络列表
    """
    # 构建关联关系图
    companies_file = os.path.join(graph_data_dir, 'nodes_company.csv')
    legal_person_file = os.path.join(graph_data_dir, 'edges_legal_person.csv')
    controls_file = os.path.join(graph_data_dir, 'edges_controls.csv')
    
    companies_df = pd.read_csv(companies_file)
    
    # 1. 加载所有公司节点
    all_companies = set(companies_df['node_id'].tolist())
    
    # 2. 构建关联关系图（字典形式）
    relation_graph = defaultdict(set)
    
    # 添加共享法人的边
    if os.path.exists(legal_person_file):
        legal_person_df = pd.read_csv(legal_person_file)
        person_to_companies = defaultdict(list)
        
        for _, row in legal_person_df.iterrows():
            person_to_companies[row['from_node']].append(row['to_node'])
        
        for person_id, companies in person_to_companies.items():
            if len(companies) >= 2:
                # 这些公司之间建立"共享法人"边
                for i in range(len(companies)):
                    for j in range(i + 1, len(companies)):
                        relation_graph[companies[i]].add(companies[j])
                        relation_graph[companies[j]].add(companies[i])
    
    # 3. 添加控股关系的边
    if os.path.exists(controls_file):
        controls_df = pd.read_csv(controls_file)
        for _, row in controls_df.iterrows():
            if row['from_node'] in all_companies and row['to_node'] in all_companies:
                relation_graph[row['from_node']].add(row['to_node'])
                relation_graph[row['to_node']].add(row['from_node'])
    
    # 4. 社区检测：找出连通的公司集群（简化版BFS）
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
        collusion_features = analyze_collusion_patterns(
            comm, graph_data_dir, relation_graph
        )
        
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
    
    print("\n[1/3] 构建关联关系图...")
    suspicious_networks = detect_collusion_network(GRAPH_DIR, min_cluster_size=3)
    
    print(f"  发现可疑串通网络数: {len(suspicious_networks)}")
    
    if len(suspicious_networks) == 0:
        print("\n未发现可疑的串通网络")
        return
    
    print("\n[2/3] 分析串通模式...")
    
    # 加载公司信息用于展示
    companies_file = os.path.join(GRAPH_DIR, 'nodes_company.csv')
    companies_df = pd.read_csv(companies_file)
    company_names = companies_df.set_index('node_id')['name'].to_dict()
    
    # 生成详细报告
    report_data = []
    for network in suspicious_networks:
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
            'companies': ', '.join([
                company_names.get(c, c) for c in network['companies'][:5]
            ]) + ('...' if len(network['companies']) > 5 else '')
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


if __name__ == '__main__':
    main()

