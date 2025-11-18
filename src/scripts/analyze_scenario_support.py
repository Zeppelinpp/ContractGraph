"""
分析现有数据对知识图谱高级分析场景的支持情况
"""

import os
import csv
import pandas as pd
from collections import defaultdict

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
GRAPH_DIR = os.path.join(BASE_DIR, "data", "graph_data")


def analyze_scenario_support():
    """分析四个场景的数据支持情况"""
    
    print("=" * 80)
    print("知识图谱高级分析场景数据支持情况分析")
    print("=" * 80)
    
    # 场景一：FraudRank 欺诈风险传导分析
    print("\n【场景一】FraudRank 欺诈风险传导分析")
    print("-" * 80)
    analyze_fraudrank_support()
    
    # 场景二：高级循环交易检测
    print("\n【场景二】高级循环交易检测（分散-汇聚模式）")
    print("-" * 80)
    analyze_circular_trade_support()
    
    # 场景三：空壳公司网络识别
    print("\n【场景三】空壳公司网络识别")
    print("-" * 80)
    analyze_shell_company_support()
    
    # 场景四：关联方串通网络分析
    print("\n【场景四】关联方串通网络分析")
    print("-" * 80)
    analyze_collusion_support()
    
    print("\n" + "=" * 80)
    print("分析完成")
    print("=" * 80)


def analyze_fraudrank_support():
    """分析FraudRank场景的数据支持"""
    
    # 检查法律事件
    legal_events_file = os.path.join(GRAPH_DIR, "nodes_legal_event.csv")
    if os.path.exists(legal_events_file):
        df_events = pd.read_csv(legal_events_file)
        print(f"✓ 法律事件节点: {len(df_events)} 个")
        print(f"  - Case类型: {len(df_events[df_events['event_type'] == 'Case'])} 个")
        print(f"  - Dispute类型: {len(df_events[df_events['event_type'] == 'Dispute'])} 个")
        print(f"  - 状态分布: {df_events['status'].value_counts().to_dict()}")
    else:
        print("✗ 缺少法律事件数据")
    
    # 检查合同-法律事件关联
    case_contract_file = os.path.join(GRAPH_DIR, "edges_case_contract.csv")
    dispute_contract_file = os.path.join(GRAPH_DIR, "edges_dispute_contract.csv")
    
    case_contract_count = 0
    if os.path.exists(case_contract_file):
        df_case = pd.read_csv(case_contract_file)
        case_contract_count = len(df_case)
        print(f"✓ 案件-合同关联: {case_contract_count} 条")
    
    dispute_contract_count = 0
    if os.path.exists(dispute_contract_file):
        df_dispute = pd.read_csv(dispute_contract_file)
        dispute_contract_count = len(df_dispute)
        print(f"✓ 纠纷-合同关联: {dispute_contract_count} 条")
    
    # 检查法人关系
    legal_person_file = os.path.join(GRAPH_DIR, "edges_legal_person.csv")
    if os.path.exists(legal_person_file):
        df_legal = pd.read_csv(legal_person_file)
        print(f"✓ 法人关系: {len(df_legal)} 条")
        
        # 统计共享法人的公司数量
        person_to_companies = defaultdict(list)
        for _, row in df_legal.iterrows():
            person_to_companies[row['from_node']].append(row['to_node'])
        
        shared_legal_count = sum(1 for companies in person_to_companies.values() if len(companies) > 1)
        print(f"  - 共享法人的公司组数: {shared_legal_count} 组")
    else:
        print("✗ 缺少法人关系数据")
    
    # 检查控股关系
    controls_file = os.path.join(GRAPH_DIR, "edges_controls.csv")
    if os.path.exists(controls_file):
        df_controls = pd.read_csv(controls_file)
        print(f"✓ 控股关系: {len(df_controls)} 条")
    else:
        print("✗ 缺少控股关系数据")
    
    # 检查交易关系
    trades_file = os.path.join(GRAPH_DIR, "edges_trades_with.csv")
    if os.path.exists(trades_file):
        df_trades = pd.read_csv(trades_file)
        print(f"✓ 交易关系: {len(df_trades)} 条")
    else:
        print("✗ 缺少交易关系数据")
    
    # 检查公司-交易关系（PAYS/RECEIVES）
    company_txn_file = os.path.join(GRAPH_DIR, "edges_company_transaction.csv")
    if os.path.exists(company_txn_file):
        df_ct = pd.read_csv(company_txn_file)
        pays_count = len(df_ct[df_ct['edge_type'] == 'PAYS'])
        receives_count = len(df_ct[df_ct['edge_type'] == 'RECEIVES'])
        print(f"✓ 支付关系(PAYS): {pays_count} 条")
        print(f"✓ 收款关系(RECEIVES): {receives_count} 条")
    else:
        print("✗ 缺少公司-交易关系数据")
    
    # 评估支持度
    if case_contract_count + dispute_contract_count > 0:
        print("\n✓ 支持度: 良好 - 可以进行FraudRank分析")
    else:
        print("\n⚠ 支持度: 一般 - 建议增加合同-法律事件关联数据")


def analyze_circular_trade_support():
    """分析循环交易检测场景的数据支持"""
    
    # 检查交易节点
    transactions_file = os.path.join(GRAPH_DIR, "nodes_transaction.csv")
    if os.path.exists(transactions_file):
        df_txn = pd.read_csv(transactions_file)
        df_txn['transaction_date'] = pd.to_datetime(df_txn['transaction_date'])
        print(f"✓ 交易节点: {len(df_txn)} 个")
        print(f"  - INFLOW: {len(df_txn[df_txn['transaction_type'] == 'INFLOW'])} 个")
        print(f"  - OUTFLOW: {len(df_txn[df_txn['transaction_type'] == 'OUTFLOW'])} 个")
        print(f"  - 时间范围: {df_txn['transaction_date'].min()} 至 {df_txn['transaction_date'].max()}")
    else:
        print("✗ 缺少交易节点数据")
    
    # 检查公司-交易关系
    company_txn_file = os.path.join(GRAPH_DIR, "edges_company_transaction.csv")
    if os.path.exists(company_txn_file):
        df_ct = pd.read_csv(company_txn_file)
        
        # 构建资金流图
        pays_edges = df_ct[df_ct['edge_type'] == 'PAYS']
        receives_edges = df_ct[df_ct['edge_type'] == 'RECEIVES']
        
        # 合并得到：付款公司 -> 收款公司
        money_flows = pays_edges.merge(
            receives_edges,
            left_on='to_node',
            right_on='from_node',
            suffixes=('_payer', '_receiver')
        )
        
        print(f"✓ 资金流路径: {len(money_flows)} 条")
        
        # 检查是否有分散-汇聚模式
        payer_counts = money_flows['from_node_payer'].value_counts()
        receiver_counts = money_flows['to_node_receiver'].value_counts()
        
        fan_out_companies = payer_counts[payer_counts >= 3].index.tolist()
        fan_in_companies = receiver_counts[receiver_counts >= 3].index.tolist()
        
        print(f"  - 扇出节点(>=3笔流出): {len(fan_out_companies)} 个")
        print(f"  - 扇入节点(>=3笔流入): {len(fan_in_companies)} 个")
        
        if len(fan_out_companies) > 0 and len(fan_in_companies) > 0:
            print("\n✓ 支持度: 良好 - 可以进行分散-汇聚模式检测")
        else:
            print("\n⚠ 支持度: 一般 - 建议增加分散-汇聚模式的交易数据")
    else:
        print("✗ 缺少公司-交易关系数据")


def analyze_shell_company_support():
    """分析空壳公司识别场景的数据支持"""
    
    # 检查公司节点
    companies_file = os.path.join(GRAPH_DIR, "nodes_company.csv")
    if os.path.exists(companies_file):
        df_companies = pd.read_csv(companies_file)
        print(f"✓ 公司节点: {len(df_companies)} 个")
    else:
        print("✗ 缺少公司节点数据")
    
    # 检查交易数据
    transactions_file = os.path.join(GRAPH_DIR, "nodes_transaction.csv")
    company_txn_file = os.path.join(GRAPH_DIR, "edges_company_transaction.csv")
    
    if os.path.exists(transactions_file) and os.path.exists(company_txn_file):
        df_txn = pd.read_csv(transactions_file)
        df_ct = pd.read_csv(company_txn_file)
        
        # 统计每个公司的交易数量
        pays_edges = df_ct[df_ct['edge_type'] == 'PAYS']
        receives_edges = df_ct[df_ct['edge_type'] == 'RECEIVES']
        
        company_txn_counts = defaultdict(int)
        for _, row in pays_edges.iterrows():
            company_txn_counts[row['from_node']] += 1
        for _, row in receives_edges.iterrows():
            company_txn_counts[row['to_node']] += 1
        
        low_txn_companies = [c for c, count in company_txn_counts.items() if count <= 2]
        print(f"✓ 交易数据可用")
        print(f"  - 低交易量公司(<=2笔): {len(low_txn_companies)} 个")
        
        # 检查法人关系
        legal_person_file = os.path.join(GRAPH_DIR, "edges_legal_person.csv")
        if os.path.exists(legal_person_file):
            df_legal = pd.read_csv(legal_person_file)
            person_to_companies = defaultdict(list)
            for _, row in df_legal.iterrows():
                person_to_companies[row['from_node']].append(row['to_node'])
            
            multi_company_persons = {p: companies for p, companies in person_to_companies.items() if len(companies) >= 3}
            print(f"  - 多公司法人(>=3家): {len(multi_company_persons)} 个")
            
            if len(multi_company_persons) > 0:
                print("\n✓ 支持度: 良好 - 可以进行空壳公司识别")
            else:
                print("\n⚠ 支持度: 一般 - 建议增加多公司法人数据")
        else:
            print("✗ 缺少法人关系数据")
    else:
        print("✗ 缺少交易数据")


def analyze_collusion_support():
    """分析串通网络场景的数据支持"""
    
    # 检查法人关系
    legal_person_file = os.path.join(GRAPH_DIR, "edges_legal_person.csv")
    if os.path.exists(legal_person_file):
        df_legal = pd.read_csv(legal_person_file)
        person_to_companies = defaultdict(list)
        for _, row in df_legal.iterrows():
            person_to_companies[row['from_node']].append(row['to_node'])
        
        shared_legal_groups = {p: companies for p, companies in person_to_companies.items() if len(companies) >= 2}
        print(f"✓ 共享法人公司组: {len(shared_legal_groups)} 组")
        
        large_groups = {p: companies for p, companies in shared_legal_groups.items() if len(companies) >= 3}
        print(f"  - 大型组(>=3家公司): {len(large_groups)} 组")
    else:
        print("✗ 缺少法人关系数据")
    
    # 检查控股关系
    controls_file = os.path.join(GRAPH_DIR, "edges_controls.csv")
    if os.path.exists(controls_file):
        df_controls = pd.read_csv(controls_file)
        print(f"✓ 控股关系: {len(df_controls)} 条")
    else:
        print("✗ 缺少控股关系数据")
    
    # 检查合同数据
    contracts_file = os.path.join(GRAPH_DIR, "nodes_contract.csv")
    party_file = os.path.join(GRAPH_DIR, "edges_party.csv")
    
    if os.path.exists(contracts_file) and os.path.exists(party_file):
        df_contracts = pd.read_csv(contracts_file)
        df_party = pd.read_csv(party_file)
        
        # 找出作为乙方的合同（中标合同）
        party_b_contracts = df_party[df_party['edge_type'] == 'PARTY_B']
        
        print(f"✓ 合同数据可用")
        print(f"  - 总合同数: {len(df_contracts)} 个")
        print(f"  - 乙方合同数: {len(party_b_contracts)} 个")
        
        # 检查是否有轮流中标模式
        if len(party_b_contracts) > 0:
            company_contract_counts = party_b_contracts['from_node'].value_counts()
            multi_contract_companies = company_contract_counts[company_contract_counts >= 2].index.tolist()
            print(f"  - 多次中标公司(>=2次): {len(multi_contract_companies)} 个")
            
            if len(shared_legal_groups) > 0 and len(multi_contract_companies) > 0:
                print("\n✓ 支持度: 良好 - 可以进行串通网络分析")
            else:
                print("\n⚠ 支持度: 一般 - 建议增加共享法人的多次中标数据")
    else:
        print("✗ 缺少合同数据")


if __name__ == "__main__":
    analyze_scenario_support()

