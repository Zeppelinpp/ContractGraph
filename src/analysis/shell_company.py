"""
场景三：空壳公司网络识别

识别具有空壳公司特征的企业网络
"""

import os
import pandas as pd
from collections import defaultdict
from tqdm import tqdm
from nebula_utils import get_nebula_session, execute_query

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")


def count_companies_with_same_legal_person(company_id, session):
    """统计与该公司共享法人的公司数量"""
    query = f"""
    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
    WHERE id(c) == "{company_id}"
    WITH p
    MATCH (p)-[:LEGAL_PERSON]->(c2:Company)
    RETURN count(c2) as count
    """
    rows = execute_query(session, query)
    if rows and len(rows) > 0:
        count = rows[0].get('count', 0)
        return max(0, count - 1)  # 不包括自己
    return 0


def extract_shell_company_features(company_id, session):
    """
    提取空壳公司特征
    
    Returns:
        dict: 特征字典
    """
    # 查询该公司的支付交易
    pays_query = f"""
    MATCH (c:Company)-[:PAYS]->(t:Transaction)
    WHERE id(c) == "{company_id}"
    RETURN id(t) as txn_id, t.Transaction.amount as amount,
           t.Transaction.transaction_date as txn_date
    """
    pays_rows = execute_query(session, pays_query)
    
    # 查询该公司的收款交易
    receives_query = f"""
    MATCH (t:Transaction)-[:RECEIVES]->(c:Company)
    WHERE id(c) == "{company_id}"
    RETURN id(t) as txn_id, t.Transaction.amount as amount,
           t.Transaction.transaction_date as txn_date
    """
    receives_rows = execute_query(session, receives_query)
    
    # 计算总流入和总流出
    total_outflow = sum(float(row.get('amount', 0) or 0) for row in pays_rows)
    total_inflow = sum(float(row.get('amount', 0) or 0) for row in receives_rows)
    
    # 特征 1: 资金穿透率
    pass_through_ratio = min(total_inflow, total_outflow) / max(total_inflow, total_outflow) if max(total_inflow, total_outflow) > 0 else 0
    
    # 特征 2: 交易速度
    all_dates = []
    for row in pays_rows + receives_rows:
        date_str = row.get('txn_date', '')
        if date_str:
            try:
                all_dates.append(pd.to_datetime(date_str))
            except:
                pass
    
    if len(all_dates) > 1:
        all_dates.sort()
        avg_time_gap = (all_dates[-1] - all_dates[0]).days / (len(all_dates) - 1)
    else:
        avg_time_gap = 0
    
    # 特征 3: 交易对手多样性
    all_partners = set()
    
    # 查询支付交易的收款方
    for row in pays_rows:
        txn_id = row.get('txn_id', '')
        if txn_id:
            receiver_query = f"""
            MATCH (t:Transaction)-[:RECEIVES]->(c:Company)
            WHERE id(t) == "{txn_id}"
            RETURN id(c) as company_id
            """
            receiver_rows = execute_query(session, receiver_query)
            for r in receiver_rows:
                comp_id = r.get('company_id', '')
                if comp_id:
                    all_partners.add(comp_id)
    
    # 查询收款交易的付款方
    for row in receives_rows:
        txn_id = row.get('txn_id', '')
        if txn_id:
            payer_query = f"""
            MATCH (c:Company)-[:PAYS]->(t:Transaction)
            WHERE id(t) == "{txn_id}"
            RETURN id(c) as company_id
            """
            payer_rows = execute_query(session, payer_query)
            for r in payer_rows:
                comp_id = r.get('company_id', '')
                if comp_id:
                    all_partners.add(comp_id)
    
    total_txns = len(pays_rows) + len(receives_rows)
    partner_diversity = len(all_partners) / total_txns if total_txns > 0 else 0
    
    # 特征 4: 合同数量
    contract_query = f"""
    MATCH (c:Company)-[:PARTY_A|PARTY_B]->(con:Contract)
    WHERE id(c) == "{company_id}"
    RETURN count(con) as contract_count
    """
    contract_rows = execute_query(session, contract_query)
    contract_count = contract_rows[0].get('contract_count', 0) if contract_rows else 0
    
    # 特征 5: 网络中心性
    degree = len(all_partners)
    
    # 特征 6: 法人公司数量
    legal_person_count = count_companies_with_same_legal_person(company_id, session)
    
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
        'contract_count': contract_count
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


def identify_shell_networks(high_risk_df, session):
    """识别共享法人的空壳公司网络"""
    high_risk_company_ids = set(high_risk_df['company_id'].values)
    
    # 查询共享法人的公司组
    query = """
    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
    WITH p, collect(id(c)) as companies
    WHERE size(companies) >= 2
    RETURN id(p) as person_id, companies
    """
    rows = execute_query(session, query)
    
    # 查询人员信息
    person_info_query = """
    MATCH (p:Person)
    RETURN id(p) as person_id, p.Person.name as name
    """
    person_rows = execute_query(session, person_info_query)
    person_info = {row.get('person_id', ''): row.get('name', '') for row in person_rows}
    
    networks = []
    for row in rows:
        person_id = row.get('person_id', '')
        companies = row.get('companies', [])
        
        high_risk_companies = [c for c in companies if c in high_risk_company_ids]
        
        if len(high_risk_companies) >= 2:
            person_name = person_info.get(person_id, person_id)
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
    
    session = None
    try:
        session = get_nebula_session()
        
        # 查询所有公司
        company_query = """
        MATCH (c:Company)
        RETURN id(c) as company_id, c.Company.name as name,
               c.Company.legal_person as legal_person
        """
        companies = execute_query(session, company_query)
        
        print(f"\n[1/3] 提取特征 (共 {len(companies)} 家公司)...")
        
        all_features = []
        for row in tqdm(companies, desc="处理进度"):
            company_id = row.get('company_id', '')
            if not company_id:
                continue
            
            features = extract_shell_company_features(company_id, session)
            features['shell_score'] = calculate_shell_company_score(features)
            features['company_name'] = row.get('name', 'Unknown')
            features['legal_person'] = row.get('legal_person', 'N/A')
            
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
            shell_networks = identify_shell_networks(high_risk, session)
            
            if shell_networks:
                print(f"  发现 {len(shell_networks)} 个空壳公司网络")
                for i, network in enumerate(shell_networks[:3], 1):
                    print(f"\n  网络 #{i}:")
                    print(f"    共同法人: {network['legal_person']}")
                    print(f"    公司数量: {len(network['companies'])}")
                    print(f"    公司列表: {', '.join(str(c) for c in network['companies'][:5])}...")
        
        print(f"\n完整报告已保存至: reports/shell_company_detection_report.csv")
    
    finally:
        if session:
            session.release()


if __name__ == '__main__':
    main()
