"""
场景三：空壳公司网络识别

识别具有空壳公司特征的企业网络
"""

import os
import csv
import pandas as pd
from collections import defaultdict
from tqdm import tqdm

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
GRAPH_DIR = os.path.join(BASE_DIR, "data", "graph_data")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")


def count_companies_with_same_legal_person(company_id, graph_data_dir):
    """统计与该公司共享法人的公司数量"""
    legal_person_edges = os.path.join(graph_data_dir, 'edges_legal_person.csv')
    
    if not os.path.exists(legal_person_edges):
        return 0
    
    company_to_person = {}
    person_to_companies = defaultdict(list)
    
    with open(legal_person_edges, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            person_id = row['from_node']
            comp_id = row['to_node']
            company_to_person[comp_id] = person_id
            person_to_companies[person_id].append(comp_id)
    
    if company_id in company_to_person:
        legal_person = company_to_person[company_id]
        return len(person_to_companies[legal_person]) - 1  # 不包括自己
    
    return 0


def extract_shell_company_features(company_id, graph_data_dir):
    """
    提取空壳公司特征
    
    Returns:
        dict: 特征字典
    """
    # 加载数据
    transactions_file = os.path.join(graph_data_dir, 'nodes_transaction.csv')
    company_txn_file = os.path.join(graph_data_dir, 'edges_company_transaction.csv')
    contracts_file = os.path.join(graph_data_dir, 'nodes_contract.csv')
    party_file = os.path.join(graph_data_dir, 'edges_party.csv')
    
    transactions_df = pd.read_csv(transactions_file)
    company_txn_edges = pd.read_csv(company_txn_file)
    contracts_df = pd.read_csv(contracts_file)
    party_edges = pd.read_csv(party_file)
    
    # 特征 1: 资金穿透率 (Pass-through Ratio)
    pays_txns = company_txn_edges[
        (company_txn_edges['from_node'] == company_id) &
        (company_txn_edges['edge_type'] == 'PAYS')
    ]['to_node'].tolist()
    
    receives_txns = company_txn_edges[
        (company_txn_edges['to_node'] == company_id) &
        (company_txn_edges['edge_type'] == 'RECEIVES')
    ]['from_node'].tolist()
    
    total_outflow = transactions_df[
        transactions_df['node_id'].isin(pays_txns)
    ]['amount'].sum()
    
    total_inflow = transactions_df[
        transactions_df['node_id'].isin(receives_txns)
    ]['amount'].sum()
    
    pass_through_ratio = min(total_inflow, total_outflow) / max(total_inflow, total_outflow) if max(total_inflow, total_outflow) > 0 else 0
    
    # 特征 2: 交易速度 (Transaction Velocity)
    transactions_df['transaction_date'] = pd.to_datetime(transactions_df['transaction_date'])
    
    txn_dates = transactions_df[
        transactions_df['node_id'].isin(pays_txns + receives_txns)
    ]['transaction_date'].sort_values()
    
    if len(txn_dates) > 1:
        avg_time_gap = (txn_dates.max() - txn_dates.min()).days / (len(txn_dates) - 1)
    else:
        avg_time_gap = 0
    
    # 特征 3: 交易对手多样性 (Partner Diversity)
    all_partners = set()
    pays_edges = company_txn_edges[
        (company_txn_edges['from_node'] == company_id) &
        (company_txn_edges['edge_type'] == 'PAYS')
    ]
    
    for txn_id in pays_edges['to_node']:
        # 找到这笔交易的收款方
        receiver = company_txn_edges[
            (company_txn_edges['from_node'] == txn_id) &
            (company_txn_edges['edge_type'] == 'RECEIVES')
        ]['to_node'].tolist()
        all_partners.update(receiver)
    
    receives_edges = company_txn_edges[
        (company_txn_edges['to_node'] == company_id) &
        (company_txn_edges['edge_type'] == 'RECEIVES')
    ]
    
    for txn_id in receives_edges['from_node']:
        # 找到这笔交易的付款方
        payer = company_txn_edges[
            (company_txn_edges['to_node'] == txn_id) &
            (company_txn_edges['edge_type'] == 'PAYS')
        ]['from_node'].tolist()
        all_partners.update(payer)
    
    total_txns = len(pays_txns) + len(receives_txns)
    partner_diversity = len(all_partners) / total_txns if total_txns > 0 else 0
    
    # 特征 4: 合同集中度 (Contract Concentration)
    company_contracts = party_edges[
        party_edges['from_node'] == company_id
    ]['to_node'].unique()
    
    # 特征 5: 网络中心性 (Network Centrality)
    degree = len(all_partners)
    
    # 特征 6: 法人公司数量 (Legal Person Company Count)
    legal_person_count = count_companies_with_same_legal_person(company_id, graph_data_dir)
    
    return {
        'company_id': company_id,
        'pass_through_ratio': pass_through_ratio,
        'transaction_velocity_days': avg_time_gap,
        'partner_diversity': partner_diversity,
        'total_transaction_count': total_txns,
        'total_inflow': total_inflow,
        'total_outflow': total_outflow,
        'degree_centrality': degree,
        'legal_person_company_count': legal_person_count,
        'contract_count': len(company_contracts)
    }


def calculate_shell_company_score(features):
    """
    计算空壳公司嫌疑分数 (0-1)
    """
    score = 0.0
    
    # 1. 穿透率高 (0.8-1.0) => 高嫌疑
    if features['pass_through_ratio'] >= 0.9:
        score += 0.25
    elif features['pass_through_ratio'] >= 0.8:
        score += 0.15
    
    # 2. 交易速度快 (< 7 天) => 高嫌疑
    if 0 < features['transaction_velocity_days'] < 7:
        score += 0.20
    elif 7 <= features['transaction_velocity_days'] < 30:
        score += 0.10
    
    # 3. 交易对手单一 (diversity < 0.3) => 高嫌疑
    if features['partner_diversity'] < 0.2:
        score += 0.20
    elif features['partner_diversity'] < 0.4:
        score += 0.10
    
    # 4. 法人关联公司多 (>= 5) => 高嫌疑
    if features['legal_person_company_count'] >= 5:
        score += 0.20
    elif features['legal_person_company_count'] >= 3:
        score += 0.10
    
    # 5. 合同数量少但金额大 => 高嫌疑
    if features['contract_count'] > 0:
        avg_contract_amount = (features['total_inflow'] + features['total_outflow']) / features['contract_count']
        if avg_contract_amount > 5000000 and features['contract_count'] < 3:
            score += 0.15
    
    return min(score, 1.0)


def identify_shell_networks(high_risk_df, graph_data_dir):
    """识别共享法人的空壳公司网络"""
    legal_person_edges = os.path.join(graph_data_dir, 'edges_legal_person.csv')
    persons_file = os.path.join(graph_data_dir, 'nodes_person.csv')
    
    if not os.path.exists(legal_person_edges):
        return []
    
    person_to_companies = defaultdict(list)
    
    with open(legal_person_edges, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            person_id = row['from_node']
            comp_id = row['to_node']
            person_to_companies[person_id].append(comp_id)
    
    # 筛选出至少有 2 家高嫌疑公司的法人
    networks = []
    for person_id, companies in person_to_companies.items():
        high_risk_companies = [
            c for c in companies 
            if c in high_risk_df['company_id'].values
        ]
        
        if len(high_risk_companies) >= 2:
            # 加载人员信息
            persons_df = pd.read_csv(persons_file)
            person_info = persons_df[persons_df['node_id'] == person_id]
            
            person_name = person_info.iloc[0]['name'] if len(person_info) > 0 else person_id
            
            networks.append({
                'legal_person': person_name,
                'person_id': person_id,
                'companies': high_risk_companies,
                'network_size': len(high_risk_companies)
            })
    
    return sorted(networks, key=lambda x: x['network_size'], reverse=True)


def main():
    print("=" * 70)
    print("空壳公司网络识别分析")
    print("=" * 70)
    
    # 加载所有公司
    companies_file = os.path.join(GRAPH_DIR, 'nodes_company.csv')
    companies_df = pd.read_csv(companies_file)
    
    print(f"\n[1/3] 提取特征 (共 {len(companies_df)} 家公司)...")
    
    all_features = []
    for company_id in tqdm(companies_df['node_id'], desc="处理进度"):
        features = extract_shell_company_features(company_id, GRAPH_DIR)
        features['shell_score'] = calculate_shell_company_score(features)
        
        # 添加公司基本信息
        company_info = companies_df[companies_df['node_id'] == company_id].iloc[0]
        features['company_name'] = company_info['name']
        features['legal_person'] = company_info['legal_person']
        
        all_features.append(features)
    
    print("\n[2/3] 分析结果...")
    features_df = pd.DataFrame(all_features)
    features_df = features_df.sort_values('shell_score', ascending=False)
    
    # 筛选高嫌疑公司
    high_risk = features_df[features_df['shell_score'] >= 0.6]
    
    print(f"  高嫌疑空壳公司数量: {len(high_risk)} ({len(high_risk)/len(features_df)*100:.1f}%)")
    
    print("\n[3/3] 生成报告...")
    
    # 确保报告目录存在
    os.makedirs(REPORTS_DIR, exist_ok=True)
    
    output_file = os.path.join(REPORTS_DIR, 'shell_company_detection_report.csv')
    features_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    if len(high_risk) > 0:
        print("\n前 10 高嫌疑空壳公司：\n")
        print(high_risk.head(10)[[
            'company_id', 'company_name', 'shell_score',
            'pass_through_ratio', 'partner_diversity', 'legal_person_company_count'
        ]].to_string(index=False))
        
        # 额外分析：识别空壳公司网络
        print("\n[额外] 识别空壳公司网络（共享法人）...")
        shell_networks = identify_shell_networks(high_risk, GRAPH_DIR)
        
        if shell_networks:
            print(f"  发现 {len(shell_networks)} 个空壳公司网络")
            for i, network in enumerate(shell_networks[:3], 1):
                print(f"\n  网络 #{i}:")
                print(f"    共同法人: {network['legal_person']}")
                print(f"    公司数量: {len(network['companies'])}")
                print(f"    公司列表: {', '.join(network['companies'][:5])}...")
    
    print(f"\n完整报告已保存至: reports/shell_company_detection_report.csv")


if __name__ == '__main__':
    main()

