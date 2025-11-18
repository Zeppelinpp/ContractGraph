"""
为知识图谱高级分析场景生成增强的mock数据
包括：
1. FraudRank场景：增加法律事件和关联关系
2. 循环交易场景：增加分散-汇聚模式的交易数据
3. 空壳公司场景：增加空壳公司特征数据
4. 串通网络场景：增加共享法人的轮流中标数据
"""

import os
import csv
import pandas as pd
from datetime import datetime, timedelta
import random

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
GRAPH_DIR = os.path.join(BASE_DIR, "data", "graph_data")

# 设置随机种子以保证可重复性
random.seed(42)


def generate_fraudrank_data():
    """生成FraudRank场景的增强数据"""
    print("\n[1/4] 生成FraudRank场景数据...")
    
    # 读取现有数据
    legal_events_file = os.path.join(GRAPH_DIR, "nodes_legal_event.csv")
    contracts_file = os.path.join(GRAPH_DIR, "nodes_contract.csv")
    companies_file = os.path.join(GRAPH_DIR, "nodes_company.csv")
    
    df_events = pd.read_csv(legal_events_file)
    df_contracts = pd.read_csv(contracts_file)
    df_companies = pd.read_csv(companies_file)
    
    # 新增高风险法律事件（涉及大金额）
    new_events = []
    event_id_start = len(df_events) + 1
    
    # 选择一些公司作为高风险种子
    high_risk_companies = df_companies.sample(n=5, random_state=42)['node_id'].tolist()
    
    for i, company_id in enumerate(high_risk_companies):
        event_id = f"CASE_{event_id_start + i:03d}"
        new_events.append({
            'node_id': event_id,
            'node_type': 'LegalEvent',
            'event_type': 'Case',
            'event_no': f'AJ202500{event_id_start + i:03d}',
            'event_name': f'高风险案件-{company_id}',
            'amount': random.uniform(5000000, 15000000),  # 500万-1500万
            'status': random.choice(['F', 'I']),  # 已立案或一审
            'register_date': (datetime.now() - timedelta(days=random.randint(30, 180))).strftime('%Y-%m-%d'),
            'description': f'高风险案件-涉及金额较大'
        })
    
    # 将新事件追加到现有数据
    new_events_df = pd.DataFrame(new_events)
    updated_events_df = pd.concat([df_events, new_events_df], ignore_index=True)
    
    # 生成合同-法律事件关联
    case_contract_file = os.path.join(GRAPH_DIR, "edges_case_contract.csv")
    df_case_contract = pd.read_csv(case_contract_file) if os.path.exists(case_contract_file) else pd.DataFrame()
    
    new_case_contracts = []
    edge_id_start = len(df_case_contract) + 1
    
    # 为每个新事件关联一个合同
    for i, event in enumerate(new_events):
        # 随机选择一个合同
        contract = df_contracts.sample(n=1, random_state=i).iloc[0]
        new_case_contracts.append({
            'edge_id': f'CASE_C_{edge_id_start + i:04d}',
            'edge_type': 'RELATED_TO',
            'from_node': contract['node_id'],
            'to_node': event['node_id'],
            'from_type': 'Contract',
            'to_type': 'LegalEvent',
            'properties': f'关联合同-{contract["contract_name"]}'
        })
    
    # 保存更新的数据
    updated_events_df.to_csv(legal_events_file, index=False, encoding='utf-8')
    
    new_case_contracts_df = pd.DataFrame(new_case_contracts)
    updated_case_contract_df = pd.concat([df_case_contract, new_case_contracts_df], ignore_index=True)
    updated_case_contract_df.to_csv(case_contract_file, index=False, encoding='utf-8')
    
    print(f"  ✓ 新增法律事件: {len(new_events)} 个")
    print(f"  ✓ 新增合同-法律事件关联: {len(new_case_contracts)} 条")


def generate_circular_trade_data():
    """生成循环交易场景的分散-汇聚模式数据"""
    print("\n[2/4] 生成循环交易场景数据...")
    
    # 读取现有数据
    transactions_file = os.path.join(GRAPH_DIR, "nodes_transaction.csv")
    company_txn_file = os.path.join(GRAPH_DIR, "edges_company_transaction.csv")
    companies_file = os.path.join(GRAPH_DIR, "nodes_company.csv")
    contracts_file = os.path.join(GRAPH_DIR, "nodes_contract.csv")
    
    df_txn = pd.read_csv(transactions_file)
    df_ct = pd.read_csv(company_txn_file)
    df_companies = pd.read_csv(companies_file)
    df_contracts = pd.read_csv(contracts_file)
    
    # 选择一个核心公司
    central_company = df_companies[df_companies['node_id'].str.startswith('ORG_')].sample(n=1, random_state=100).iloc[0]
    central_id = central_company['node_id']
    
    # 选择3-5个分散节点
    dispersed_companies = df_companies[~df_companies['node_id'].str.startswith('ORG_')].sample(n=4, random_state=101)['node_id'].tolist()
    
    # 选择一个关联公司作为汇聚节点（可以是控股公司或共享法人的公司）
    related_companies = df_companies[df_companies['node_id'] != central_id].sample(n=1, random_state=102)['node_id'].tolist()
    
    # 生成分散-汇聚模式的交易
    base_date = datetime(2025, 6, 1)
    amount = 8000000  # 800万
    
    new_transactions = []
    new_edges = []
    
    txn_id_start = len(df_txn) + 1
    edge_id_start = len(df_ct) + 1
    
    # 阶段1：核心公司分散支付到多个公司
    for i, dispersed_id in enumerate(dispersed_companies):
        txn_id = f"TXN_OUT_{txn_id_start + i:04d}"
        txn_date = base_date + timedelta(days=i*5)
        
        new_transactions.append({
            'node_id': txn_id,
            'node_type': 'Transaction',
            'transaction_type': 'OUTFLOW',
            'transaction_no': f'OUT202500{txn_id_start + i:04d}',
            'contract_no': f'HT202500{txn_id_start + i:04d}',
            'amount': amount / len(dispersed_companies),
            'transaction_date': txn_date.strftime('%Y-%m-%d'),
            'status': 'C',
            'description': f'分散支付-{central_id}向{dispersed_id}支付'
        })
        
        # PAYS边
        new_edges.append({
            'edge_id': f'CT_{edge_id_start:06d}',
            'edge_type': 'PAYS',
            'from_node': central_id,
            'to_node': txn_id,
            'from_type': 'Company',
            'to_type': 'Transaction',
            'properties': f'付款-{central_id}向{dispersed_id}支付'
        })
        edge_id_start += 1
        
        # RECEIVES边
        new_edges.append({
            'edge_id': f'CT_{edge_id_start:06d}',
            'edge_type': 'RECEIVES',
            'from_node': txn_id,
            'to_node': dispersed_id,
            'from_type': 'Transaction',
            'to_type': 'Company',
            'properties': f'收款-{dispersed_id}收到{central_id}付款'
        })
        edge_id_start += 1
    
    # 阶段2：分散节点之间的交易
    for i in range(len(dispersed_companies) - 1):
        txn_id = f"TXN_OUT_{txn_id_start + len(dispersed_companies) + i:04d}"
        txn_date = base_date + timedelta(days=20 + i*3)
        
        payer = dispersed_companies[i]
        receiver = dispersed_companies[i + 1]
        
        new_transactions.append({
            'node_id': txn_id,
            'node_type': 'Transaction',
            'transaction_type': 'OUTFLOW',
            'transaction_no': f'OUT202500{txn_id_start + len(dispersed_companies) + i:04d}',
            'contract_no': f'HT202500{txn_id_start + len(dispersed_companies) + i:04d}',
            'amount': amount / len(dispersed_companies) * 0.8,
            'transaction_date': txn_date.strftime('%Y-%m-%d'),
            'status': 'C',
            'description': f'中间交易-{payer}向{receiver}支付'
        })
        
        new_edges.append({
            'edge_id': f'CT_{edge_id_start:06d}',
            'edge_type': 'PAYS',
            'from_node': payer,
            'to_node': txn_id,
            'from_type': 'Company',
            'to_type': 'Transaction',
            'properties': f'付款-{payer}向{receiver}支付'
        })
        edge_id_start += 1
        
        new_edges.append({
            'edge_id': f'CT_{edge_id_start:06d}',
            'edge_type': 'RECEIVES',
            'from_node': txn_id,
            'to_node': receiver,
            'from_type': 'Transaction',
            'to_type': 'Company',
            'properties': f'收款-{receiver}收到{payer}付款'
        })
        edge_id_start += 1
    
    # 阶段3：汇聚回关联公司
    for i, dispersed_id in enumerate(dispersed_companies):
        txn_id = f"TXN_IN_{txn_id_start + len(dispersed_companies) * 2 + i:04d}"
        txn_date = base_date + timedelta(days=40 + i*5)
        related_id = related_companies[0]
        
        new_transactions.append({
            'node_id': txn_id,
            'node_type': 'Transaction',
            'transaction_type': 'INFLOW',
            'transaction_no': f'IN202500{txn_id_start + len(dispersed_companies) * 2 + i:04d}',
            'contract_no': f'HT202500{txn_id_start + len(dispersed_companies) * 2 + i:04d}',
            'amount': amount / len(dispersed_companies) * 0.9,
            'transaction_date': txn_date.strftime('%Y-%m-%d'),
            'status': 'C',
            'description': f'汇聚支付-{dispersed_id}向{related_id}支付'
        })
        
        new_edges.append({
            'edge_id': f'CT_{edge_id_start:06d}',
            'edge_type': 'PAYS',
            'from_node': dispersed_id,
            'to_node': txn_id,
            'from_type': 'Company',
            'to_type': 'Transaction',
            'properties': f'付款-{dispersed_id}向{related_id}支付'
        })
        edge_id_start += 1
        
        new_edges.append({
            'edge_id': f'CT_{edge_id_start:06d}',
            'edge_type': 'RECEIVES',
            'from_node': txn_id,
            'to_node': related_id,
            'from_type': 'Transaction',
            'to_type': 'Company',
            'properties': f'收款-{related_id}收到{dispersed_id}付款'
        })
        edge_id_start += 1
    
    # 保存更新的数据
    new_txn_df = pd.DataFrame(new_transactions)
    updated_txn_df = pd.concat([df_txn, new_txn_df], ignore_index=True)
    updated_txn_df.to_csv(transactions_file, index=False, encoding='utf-8')
    
    new_edges_df = pd.DataFrame(new_edges)
    updated_ct_df = pd.concat([df_ct, new_edges_df], ignore_index=True)
    updated_ct_df.to_csv(company_txn_file, index=False, encoding='utf-8')
    
    print(f"  ✓ 新增交易节点: {len(new_transactions)} 个")
    print(f"  ✓ 新增交易边: {len(new_edges)} 条")
    print(f"  ✓ 核心公司: {central_id}")
    print(f"  ✓ 分散节点: {len(dispersed_companies)} 个")
    print(f"  ✓ 汇聚节点: {related_companies[0]}")


def generate_shell_company_data():
    """生成空壳公司场景的数据"""
    print("\n[3/4] 生成空壳公司场景数据...")
    
    # 读取现有数据
    companies_file = os.path.join(GRAPH_DIR, "nodes_company.csv")
    legal_person_file = os.path.join(GRAPH_DIR, "edges_legal_person.csv")
    persons_file = os.path.join(GRAPH_DIR, "nodes_person.csv")
    
    df_companies = pd.read_csv(companies_file)
    df_legal = pd.read_csv(legal_person_file)
    df_persons = pd.read_csv(persons_file)
    
    # 创建一个共享法人的空壳公司网络
    # 选择一个法人，为其创建多个空壳公司
    person = df_persons.sample(n=1, random_state=200).iloc[0]
    person_id = person['node_id']
    
    # 创建3-5个空壳公司
    shell_companies = []
    company_id_start = len(df_companies) + 1
    
    for i in range(4):
        company_id = f"SHELL_{company_id_start + i:03d}"
        shell_companies.append({
            'node_id': company_id,
            'node_type': 'Company',
            'name': f'空壳公司{i+1}-{person["name"]}',
            'number': f'SHELL{company_id_start + i:03d}',
            'legal_person': person['name'],
            'credit_code': f'91110000MA{random.randint(10000000, 99999999)}',
            'establish_date': (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d'),
            'status': 'C',
            'description': f'空壳公司-快速资金流转'
        })
    
    # 添加法人关系
    new_legal_edges = []
    edge_id_start = len(df_legal) + 1
    
    for i, company in enumerate(shell_companies):
        new_legal_edges.append({
            'edge_id': f'LEGAL_{edge_id_start + i:04d}',
            'edge_type': 'LEGAL_PERSON',
            'from_node': person_id,
            'to_node': company['node_id'],
            'from_type': 'Person',
            'to_type': 'Company',
            'properties': f'法人关系-{person["name"]}担任{company["name"]}法人'
        })
    
    # 为这些空壳公司生成快速流转的交易数据
    transactions_file = os.path.join(GRAPH_DIR, "nodes_transaction.csv")
    company_txn_file = os.path.join(GRAPH_DIR, "edges_company_transaction.csv")
    
    df_txn = pd.read_csv(transactions_file)
    df_ct = pd.read_csv(company_txn_file)
    
    new_txn = []
    new_edges = []
    
    txn_id_start = len(df_txn) + 1
    edge_id_start = len(df_ct) + 1
    
    # 创建快速流转的交易（资金快速进出）
    for i, shell_company in enumerate(shell_companies):
        # 流入交易
        txn_in_id = f"TXN_IN_{txn_id_start + i*2:04d}"
        txn_date_in = datetime.now() - timedelta(days=random.randint(1, 10))
        
        new_txn.append({
            'node_id': txn_in_id,
            'node_type': 'Transaction',
            'transaction_type': 'INFLOW',
            'transaction_no': f'IN202500{txn_id_start + i*2:04d}',
            'contract_no': f'HT202500{txn_id_start + i*2:04d}',
            'amount': random.uniform(2000000, 5000000),
            'transaction_date': txn_date_in.strftime('%Y-%m-%d'),
            'status': 'C',
            'description': f'空壳公司快速流入-{shell_company["name"]}'
        })
        
        # 流出交易（几乎同时）
        txn_out_id = f"TXN_OUT_{txn_id_start + i*2 + 1:04d}"
        txn_date_out = txn_date_in + timedelta(days=random.randint(1, 5))
        
        new_txn.append({
            'node_id': txn_out_id,
            'node_type': 'Transaction',
            'transaction_type': 'OUTFLOW',
            'transaction_no': f'OUT202500{txn_id_start + i*2 + 1:04d}',
            'contract_no': f'HT202500{txn_id_start + i*2 + 1:04d}',
            'amount': random.uniform(1900000, 4800000),  # 几乎全部流出
            'transaction_date': txn_date_out.strftime('%Y-%m-%d'),
            'status': 'C',
            'description': f'空壳公司快速流出-{shell_company["name"]}'
        })
        
        # 添加边
        # 流入边
        payer = df_companies.sample(n=1, random_state=i*100).iloc[0]['node_id']
        new_edges.append({
            'edge_id': f'CT_{edge_id_start:06d}',
            'edge_type': 'PAYS',
            'from_node': payer,
            'to_node': txn_in_id,
            'from_type': 'Company',
            'to_type': 'Transaction',
            'properties': f'付款-{payer}向{shell_company["name"]}支付'
        })
        edge_id_start += 1
        
        new_edges.append({
            'edge_id': f'CT_{edge_id_start:06d}',
            'edge_type': 'RECEIVES',
            'from_node': txn_in_id,
            'to_node': shell_company['node_id'],
            'from_type': 'Transaction',
            'to_type': 'Company',
            'properties': f'收款-{shell_company["name"]}收到{payer}付款'
        })
        edge_id_start += 1
        
        # 流出边
        receiver = df_companies.sample(n=1, random_state=i*100+1).iloc[0]['node_id']
        new_edges.append({
            'edge_id': f'CT_{edge_id_start:06d}',
            'edge_type': 'PAYS',
            'from_node': shell_company['node_id'],
            'to_node': txn_out_id,
            'from_type': 'Company',
            'to_type': 'Transaction',
            'properties': f'付款-{shell_company["name"]}向{receiver}支付'
        })
        edge_id_start += 1
        
        new_edges.append({
            'edge_id': f'CT_{edge_id_start:06d}',
            'edge_type': 'RECEIVES',
            'from_node': txn_out_id,
            'to_node': receiver,
            'from_type': 'Transaction',
            'to_type': 'Company',
            'properties': f'收款-{receiver}收到{shell_company["name"]}付款'
        })
        edge_id_start += 1
    
    # 保存更新的数据
    new_companies_df = pd.DataFrame(shell_companies)
    updated_companies_df = pd.concat([df_companies, new_companies_df], ignore_index=True)
    updated_companies_df.to_csv(companies_file, index=False, encoding='utf-8')
    
    new_legal_df = pd.DataFrame(new_legal_edges)
    updated_legal_df = pd.concat([df_legal, new_legal_df], ignore_index=True)
    updated_legal_df.to_csv(legal_person_file, index=False, encoding='utf-8')
    
    new_txn_df = pd.DataFrame(new_txn)
    updated_txn_df = pd.concat([df_txn, new_txn_df], ignore_index=True)
    updated_txn_df.to_csv(transactions_file, index=False, encoding='utf-8')
    
    new_edges_df = pd.DataFrame(new_edges)
    updated_ct_df = pd.concat([df_ct, new_edges_df], ignore_index=True)
    updated_ct_df.to_csv(company_txn_file, index=False, encoding='utf-8')
    
    print(f"  ✓ 新增空壳公司: {len(shell_companies)} 个")
    print(f"  ✓ 新增法人关系: {len(new_legal_edges)} 条")
    print(f"  ✓ 新增交易数据: {len(new_txn)} 个")
    print(f"  ✓ 共同法人: {person['name']}")


def generate_collusion_data():
    """生成串通网络场景的数据"""
    print("\n[4/4] 生成串通网络场景数据...")
    
    # 读取现有数据
    companies_file = os.path.join(GRAPH_DIR, "nodes_company.csv")
    legal_person_file = os.path.join(GRAPH_DIR, "edges_legal_person.csv")
    persons_file = os.path.join(GRAPH_DIR, "nodes_person.csv")
    contracts_file = os.path.join(GRAPH_DIR, "nodes_contract.csv")
    party_file = os.path.join(GRAPH_DIR, "edges_party.csv")
    
    df_companies = pd.read_csv(companies_file)
    df_legal = pd.read_csv(legal_person_file)
    df_persons = pd.read_csv(persons_file)
    df_contracts = pd.read_csv(contracts_file)
    df_party = pd.read_csv(party_file)
    
    # 创建一个共享法人的公司组（3-5家公司）
    person = df_persons.sample(n=1, random_state=300).iloc[0]
    person_id = person['node_id']
    
    # 创建3家公司
    collusion_companies = []
    company_id_start = len(df_companies) + 1
    
    for i in range(3):
        company_id = f"COLL_{company_id_start + i:03d}"
        collusion_companies.append({
            'node_id': company_id,
            'node_type': 'Company',
            'name': f'串通公司{i+1}-{person["name"]}',
            'number': f'COLL{company_id_start + i:03d}',
            'legal_person': person['name'],
            'credit_code': f'91110000MA{random.randint(10000000, 99999999)}',
            'establish_date': (datetime.now() - timedelta(days=random.randint(180, 730))).strftime('%Y-%m-%d'),
            'status': 'C',
            'description': f'串通网络公司-{person["name"]}控制'
        })
    
    # 添加法人关系
    new_legal_edges = []
    edge_id_start = len(df_legal) + 1
    
    for i, company in enumerate(collusion_companies):
        new_legal_edges.append({
            'edge_id': f'LEGAL_{edge_id_start + i:04d}',
            'edge_type': 'LEGAL_PERSON',
            'from_node': person_id,
            'to_node': company['node_id'],
            'from_type': 'Person',
            'to_type': 'Company',
            'properties': f'法人关系-{person["name"]}担任{company["name"]}法人'
        })
    
    # 为这些公司生成轮流中标的合同
    new_contracts = []
    new_party_edges = []
    
    contract_id_start = len(df_contracts) + 1
    party_edge_id_start = len(df_party) + 1
    
    # 选择一个甲方（央企组织）
    party_a = df_companies[df_companies['node_id'].str.startswith('ORG_')].sample(n=1, random_state=301).iloc[0]
    
    # 生成6-9个合同，轮流分配给这3家公司
    base_date = datetime(2024, 1, 1)
    amount = 950000  # 95万，接近100万审批阈值
    
    for i in range(9):
        contract_id = f"CON_{contract_id_start + i:04d}"
        contract_date = base_date + timedelta(days=i*30)
        
        # 轮流分配：公司0, 公司1, 公司2, 公司0, ...
        company_idx = i % len(collusion_companies)
        party_b_company = collusion_companies[company_idx]
        
        new_contracts.append({
            'node_id': contract_id,
            'node_type': 'Contract',
            'contract_no': f'HT202400{contract_id_start + i:04d}',
            'contract_name': f'串通合同-{party_a["name"]}与{party_b_company["name"]}',
            'amount': amount + random.uniform(-50000, 50000),  # 在阈值附近
            'sign_date': contract_date.strftime('%Y-%m-%d'),
            'status': 'EXECUTING',
            'description': f'串通网络合同-轮流中标模式'
        })
        
        # PARTY_A边
        new_party_edges.append({
            'edge_id': f'PARTY_{party_edge_id_start + i*2:04d}',
            'edge_type': 'PARTY_A',
            'from_node': party_a['node_id'],
            'to_node': contract_id,
            'from_type': 'Company',
            'to_type': 'Contract',
            'properties': f'甲方-{party_a["name"]}'
        })
        
        # PARTY_B边
        new_party_edges.append({
            'edge_id': f'PARTY_{party_edge_id_start + i*2 + 1:04d}',
            'edge_type': 'PARTY_B',
            'from_node': party_b_company['node_id'],
            'to_node': contract_id,
            'from_type': 'Company',
            'to_type': 'Contract',
            'properties': f'乙方-{party_b_company["name"]}'
        })
    
    # 保存更新的数据
    new_companies_df = pd.DataFrame(collusion_companies)
    updated_companies_df = pd.concat([df_companies, new_companies_df], ignore_index=True)
    updated_companies_df.to_csv(companies_file, index=False, encoding='utf-8')
    
    new_legal_df = pd.DataFrame(new_legal_edges)
    updated_legal_df = pd.concat([df_legal, new_legal_df], ignore_index=True)
    updated_legal_df.to_csv(legal_person_file, index=False, encoding='utf-8')
    
    new_contracts_df = pd.DataFrame(new_contracts)
    updated_contracts_df = pd.concat([df_contracts, new_contracts_df], ignore_index=True)
    updated_contracts_df.to_csv(contracts_file, index=False, encoding='utf-8')
    
    new_party_df = pd.DataFrame(new_party_edges)
    updated_party_df = pd.concat([df_party, new_party_df], ignore_index=True)
    updated_party_df.to_csv(party_file, index=False, encoding='utf-8')
    
    print(f"  ✓ 新增串通公司: {len(collusion_companies)} 个")
    print(f"  ✓ 新增法人关系: {len(new_legal_edges)} 条")
    print(f"  ✓ 新增合同: {len(new_contracts)} 个")
    print(f"  ✓ 共同法人: {person['name']}")
    print(f"  ✓ 轮流中标模式: {len(collusion_companies)} 家公司轮流中标 {len(new_contracts)} 个合同")


def main():
    """主函数"""
    print("=" * 80)
    print("知识图谱高级分析场景Mock数据生成")
    print("=" * 80)
    
    # 备份现有数据（可选）
    print("\n提示: 建议先备份现有数据文件")
    
    try:
        generate_fraudrank_data()
        generate_circular_trade_data()
        generate_shell_company_data()
        generate_collusion_data()
        
        print("\n" + "=" * 80)
        print("✓ 所有场景数据生成完成！")
        print("=" * 80)
        print("\n下一步:")
        print("1. 运行 analyze_scenario_support.py 验证数据支持情况")
        print("2. 重新导入数据到Nebula Graph")
        print("3. 运行各场景的分析脚本")
        
    except Exception as e:
        print(f"\n✗ 错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

