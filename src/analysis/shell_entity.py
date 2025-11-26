"""
场景：马甲公司检测（集中授信爆雷风险）

识别同一控制人/电话背后隐藏的多家"马甲"公司集群，
分析集中授信风险，防止集体爆雷。

关联维度：
1. 同一法人代表
2. 同一联系电话
3. 高管交叉任职
4. 控股关系链
"""

import os
import pandas as pd
from collections import defaultdict
from tqdm import tqdm
from src.utils.nebula_utils import get_nebula_session, execute_query

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")


class UnionFind:
    """Union-Find for cluster aggregation"""

    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        px, py = self.find(x), self.find(y)
        if px == py:
            return
        if self.rank[px] < self.rank[py]:
            px, py = py, px
        self.parent[py] = px
        if self.rank[px] == self.rank[py]:
            self.rank[px] += 1

    def get_clusters(self):
        clusters = defaultdict(set)
        for node in self.parent:
            clusters[self.find(node)].add(node)
        return list(clusters.values())


def build_relation_graph(session):
    """
    Build relation graph from multiple dimensions.
    Returns: (UnionFind, link_details)
    """
    uf = UnionFind()
    link_details = defaultdict(lambda: {"legal_person": [], "phone": [], "employment": [], "controls": []})

    # Dimension 1: Same legal person
    legal_person_query = """
    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
    RETURN id(p) as person_id, p.Person.name as person_name, 
           p.Person.phone as phone, collect(id(c)) as companies
    """
    rows = execute_query(session, legal_person_query)
    for row in rows:
        companies = row.get("companies", [])
        person_name = row.get("person_name", "")
        if len(companies) >= 2:
            for i, c1 in enumerate(companies):
                for c2 in companies[i + 1:]:
                    if c1 and c2:
                        uf.union(c1, c2)
                        link_details[(c1, c2)]["legal_person"].append(person_name)
                        link_details[(c2, c1)]["legal_person"].append(person_name)

    # Dimension 2: Same phone number
    phone_query = """
    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
    WHERE p.Person.phone IS NOT NULL AND p.Person.phone <> ""
    RETURN p.Person.phone as phone, collect(DISTINCT id(c)) as companies,
           collect(DISTINCT p.Person.name) as persons
    """
    rows = execute_query(session, phone_query)
    phone_groups = defaultdict(lambda: {"companies": set(), "persons": set()})
    for row in rows:
        phone = row.get("phone", "")
        companies = row.get("companies", [])
        persons = row.get("persons", [])
        if phone:
            phone_groups[phone]["companies"].update(companies)
            phone_groups[phone]["persons"].update(persons)

    for phone, data in phone_groups.items():
        companies = list(data["companies"])
        if len(companies) >= 2:
            for i, c1 in enumerate(companies):
                for c2 in companies[i + 1:]:
                    if c1 and c2:
                        uf.union(c1, c2)
                        link_details[(c1, c2)]["phone"].append(phone)
                        link_details[(c2, c1)]["phone"].append(phone)

    # Dimension 3: Cross-employment (executives in multiple companies)
    employment_query = """
    MATCH (p:Person)-[e:EMPLOYED_BY]->(c:Company)
    RETURN id(p) as person_id, p.Person.name as person_name,
           collect({company: id(c), position: e.position}) as positions
    """
    rows = execute_query(session, employment_query)
    for row in rows:
        positions = row.get("positions", [])
        person_name = row.get("person_name", "")
        if len(positions) >= 2:
            companies = [p.get("company") for p in positions if p.get("company")]
            for i, c1 in enumerate(companies):
                for c2 in companies[i + 1:]:
                    if c1 and c2:
                        uf.union(c1, c2)
                        link_details[(c1, c2)]["employment"].append(person_name)
                        link_details[(c2, c1)]["employment"].append(person_name)

    # Dimension 4: Controls relationship
    controls_query = """
    MATCH (c1:Company)-[:CONTROLS]->(c2:Company)
    RETURN id(c1) as parent, id(c2) as child
    """
    rows = execute_query(session, controls_query)
    for row in rows:
        parent = row.get("parent", "")
        child = row.get("child", "")
        if parent and child:
            uf.union(parent, child)
            link_details[(parent, child)]["controls"].append("控股")
            link_details[(child, parent)]["controls"].append("被控股")

    return uf, link_details


def get_cluster_link_types(cluster, link_details):
    """Get link types within a cluster"""
    link_types = set()
    for i, c1 in enumerate(cluster):
        for c2 in cluster:
            if c1 != c2:
                details = link_details.get((c1, c2), {})
                if details.get("legal_person"):
                    link_types.add("法人")
                if details.get("phone"):
                    link_types.add("电话")
                if details.get("employment"):
                    link_types.add("任职")
                if details.get("controls"):
                    link_types.add("控股")
    return link_types


def get_cluster_controllers(cluster, session):
    """Get control persons for a cluster"""
    company_ids_str = ", ".join([f'"{c}"' for c in cluster])

    # Legal persons
    legal_query = f"""
    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
    WHERE id(c) IN [{company_ids_str}]
    RETURN DISTINCT id(p) as person_id, p.Person.name as name, 
           p.Person.phone as phone, collect(id(c)) as companies
    """
    rows = execute_query(session, legal_query)

    controllers = []
    for row in rows:
        companies = row.get("companies", [])
        if len(companies) >= 1:
            controllers.append({
                "person_id": row.get("person_id", ""),
                "name": row.get("name", ""),
                "phone": row.get("phone", ""),
                "controlled_count": len(companies),
                "companies": companies,
                "role": "法人代表"
            })

    # Executives with multiple positions
    exec_query = f"""
    MATCH (p:Person)-[e:EMPLOYED_BY]->(c:Company)
    WHERE id(c) IN [{company_ids_str}]
    RETURN id(p) as person_id, p.Person.name as name, p.Person.phone as phone,
           collect({{company: id(c), position: e.position}}) as positions
    """
    rows = execute_query(session, exec_query)
    for row in rows:
        positions = row.get("positions", [])
        if len(positions) >= 2:
            existing = next((c for c in controllers if c["person_id"] == row.get("person_id")), None)
            if existing:
                existing["role"] += "/高管"
            else:
                controllers.append({
                    "person_id": row.get("person_id", ""),
                    "name": row.get("name", ""),
                    "phone": row.get("phone", ""),
                    "controlled_count": len(positions),
                    "companies": [p.get("company") for p in positions],
                    "role": "高管"
                })

    return controllers


def calculate_cluster_exposure(cluster, session):
    """Calculate credit exposure for a cluster"""
    company_ids_str = ", ".join([f'"{c}"' for c in cluster])

    contract_query = f"""
    MATCH (c:Company)-[:PARTY_A|PARTY_B]->(con:Contract)
    WHERE id(c) IN [{company_ids_str}]
    RETURN DISTINCT id(con) as contract_id, con.Contract.amount as amount,
           con.Contract.contract_name as name
    """
    rows = execute_query(session, contract_query)

    total_amount = 0
    contract_count = 0
    for row in rows:
        amount = float(row.get("amount", 0) or 0)
        total_amount += amount
        contract_count += 1

    return total_amount, contract_count


def calculate_internal_trade(cluster, session):
    """Calculate internal trade amount within cluster"""
    company_ids_str = ", ".join([f'"{c}"' for c in cluster])

    trade_query = f"""
    MATCH (c1:Company)-[:TRADES_WITH]->(c2:Company)
    WHERE id(c1) IN [{company_ids_str}] AND id(c2) IN [{company_ids_str}]
    RETURN count(*) as trade_count
    """
    rows = execute_query(session, trade_query)
    trade_count = rows[0].get("trade_count", 0) if rows else 0

    # Get internal trade amount from contracts
    internal_contract_query = f"""
    MATCH (c1:Company)-[:PARTY_A]->(con:Contract)<-[:PARTY_B]-(c2:Company)
    WHERE id(c1) IN [{company_ids_str}] AND id(c2) IN [{company_ids_str}]
    RETURN sum(con.Contract.amount) as internal_amount
    """
    rows = execute_query(session, internal_contract_query)
    internal_amount = float(rows[0].get("internal_amount", 0) or 0) if rows else 0

    return internal_amount, trade_count


def get_cluster_risk_events(cluster, session):
    """Get external risk events for cluster companies"""
    company_ids_str = ", ".join([f'"{c}"' for c in cluster])

    # Admin penalties
    penalty_query = f"""
    MATCH (ap:AdminPenalty)-[:ADMIN_PENALTY_OF]->(c:Company)
    WHERE id(c) IN [{company_ids_str}]
    RETURN count(ap) as penalty_count, sum(ap.AdminPenalty.amount) as penalty_amount
    """
    rows = execute_query(session, penalty_query)
    penalty_count = rows[0].get("penalty_count", 0) if rows else 0
    penalty_amount = float(rows[0].get("penalty_amount", 0) or 0) if rows else 0

    # Business abnormal
    abnormal_query = f"""
    MATCH (ba:BusinessAbnormal)-[:BUSINESS_ABNORMAL_OF]->(c:Company)
    WHERE id(c) IN [{company_ids_str}]
    RETURN count(ba) as abnormal_count
    """
    rows = execute_query(session, abnormal_query)
    abnormal_count = rows[0].get("abnormal_count", 0) if rows else 0

    return {
        "penalty_count": penalty_count,
        "penalty_amount": penalty_amount,
        "abnormal_count": abnormal_count
    }


def calculate_risk_score(cluster_features, credit_threshold=10000000):
    """
    Calculate risk score for a shell entity cluster.

    Score components:
    - Cluster size (25%): larger clusters = higher risk
    - Credit concentration (30%): exposure / threshold
    - Concealment index (25%): fewer distinct controllers = more concealed
    - Internal trade ratio (20%): higher internal trade = higher risk
    """
    score = 0.0

    # 1. Cluster size score (25%)
    size = cluster_features["company_count"]
    if size >= 5:
        score += 0.25
    elif size >= 3:
        score += 0.15
    elif size >= 2:
        score += 0.08

    # 2. Credit concentration score (30%)
    exposure = cluster_features["total_exposure"]
    concentration = min(exposure / credit_threshold, 1.0) if credit_threshold > 0 else 0
    score += concentration * 0.30

    # 3. Concealment index (25%)
    controller_count = cluster_features["controller_count"]
    company_count = cluster_features["company_count"]
    if company_count > 0:
        concealment = 1 - (controller_count / company_count)
        concealment = max(0, min(concealment, 1.0))
        score += concealment * 0.25

    # 4. Internal trade ratio (20%)
    internal_amount = cluster_features["internal_trade_amount"]
    if exposure > 0:
        internal_ratio = min(internal_amount / exposure, 1.0)
        score += internal_ratio * 0.20

    # Bonus: External risk events
    if cluster_features.get("penalty_count", 0) > 0:
        score += 0.05
    if cluster_features.get("abnormal_count", 0) > 0:
        score += 0.05

    return min(score, 1.0)


def get_risk_level(score):
    """Get risk level from score"""
    if score >= 0.6:
        return "HIGH"
    elif score >= 0.4:
        return "MEDIUM"
    else:
        return "LOW"


def detect_shell_entity_clusters(session, min_cluster_size=2, credit_threshold=10000000, exclude_internal_orgs=True):
    """
    Detect shell entity clusters.

    Args:
        session: Nebula session
        min_cluster_size: Minimum companies in a cluster
        credit_threshold: Credit exposure warning threshold
        exclude_internal_orgs: Whether to exclude internal organization clusters (ORG_xxx)

    Returns:
        list: Shell entity cluster analysis results
    """
    # Build relation graph
    uf, link_details = build_relation_graph(session)
    clusters = uf.get_clusters()

    # Filter by size
    clusters = [c for c in clusters if len(c) >= min_cluster_size]

    # Optionally exclude internal organization clusters
    if exclude_internal_orgs:
        def is_internal_cluster(cluster):
            org_count = sum(1 for c in cluster if c.startswith("ORG_"))
            return org_count == len(cluster)
        clusters = [c for c in clusters if not is_internal_cluster(c)]

    # Get company info
    company_query = """
    MATCH (c:Company)
    RETURN id(c) as company_id, c.Company.name as name, c.Company.legal_person as legal_person
    """
    rows = execute_query(session, company_query)
    company_info = {row.get("company_id", ""): row for row in rows}

    results = []
    for idx, cluster in enumerate(tqdm(clusters, desc="分析集群")):
        cluster = list(cluster)

        # Get link types
        link_types = get_cluster_link_types(cluster, link_details)

        # Get controllers
        controllers = get_cluster_controllers(cluster, session)

        # Calculate exposure
        total_exposure, contract_count = calculate_cluster_exposure(cluster, session)

        # Calculate internal trade
        internal_amount, internal_trade_count = calculate_internal_trade(cluster, session)

        # Get risk events
        risk_events = get_cluster_risk_events(cluster, session)

        # Build features
        features = {
            "company_count": len(cluster),
            "controller_count": len(set(c["person_id"] for c in controllers)),
            "total_exposure": total_exposure,
            "internal_trade_amount": internal_amount,
            "penalty_count": risk_events["penalty_count"],
            "abnormal_count": risk_events["abnormal_count"]
        }

        # Calculate risk score
        risk_score = calculate_risk_score(features, credit_threshold)

        # Get company names
        company_names = [company_info.get(c, {}).get("name", c) for c in cluster]

        # Get controller names
        controller_names = list(set(c["name"] for c in controllers if c["name"]))

        # Get shared phones
        shared_phones = set()
        for c in controllers:
            if c.get("phone"):
                shared_phones.add(c["phone"])

        results.append({
            "cluster_id": f"SHELL_{idx + 1:03d}",
            "company_count": len(cluster),
            "companies": cluster,
            "company_names": company_names,
            "control_persons": controller_names,
            "shared_phones": list(shared_phones),
            "total_exposure": total_exposure,
            "contract_count": contract_count,
            "internal_trade_amount": internal_amount,
            "internal_trade_count": internal_trade_count,
            "link_types": list(link_types),
            "penalty_count": risk_events["penalty_count"],
            "penalty_amount": risk_events["penalty_amount"],
            "abnormal_count": risk_events["abnormal_count"],
            "risk_score": risk_score,
            "risk_level": get_risk_level(risk_score),
            "controllers": controllers
        })

    # Sort by risk score
    results.sort(key=lambda x: x["risk_score"], reverse=True)

    return results


def main():
    print("=" * 70)
    print("马甲公司检测分析（集中授信爆雷风险）")
    print("=" * 70)

    session = None
    try:
        session = get_nebula_session()

        print("\n[1/4] 构建多维度关联图...")
        results = detect_shell_entity_clusters(
            session,
            min_cluster_size=2,
            credit_threshold=10000000,
            exclude_internal_orgs=True
        )

        print(f"\n[2/4] 发现马甲公司集群数: {len(results)}")

        if len(results) == 0:
            print("\n未发现可疑的马甲公司集群")
            return

        # Statistics
        high_risk = [r for r in results if r["risk_level"] == "HIGH"]
        medium_risk = [r for r in results if r["risk_level"] == "MEDIUM"]
        low_risk = [r for r in results if r["risk_level"] == "LOW"]

        print(f"  - 高风险集群: {len(high_risk)}")
        print(f"  - 中风险集群: {len(medium_risk)}")
        print(f"  - 低风险集群: {len(low_risk)}")

        print("\n[3/4] 生成报告...")

        # Cluster report
        cluster_data = []
        for r in results:
            cluster_data.append({
                "cluster_id": r["cluster_id"],
                "company_count": r["company_count"],
                "companies": "; ".join(r["company_names"][:5]) + ("..." if len(r["company_names"]) > 5 else ""),
                "control_persons": "; ".join(r["control_persons"][:3]) + ("..." if len(r["control_persons"]) > 3 else ""),
                "shared_phones": "; ".join(r["shared_phones"]),
                "total_exposure": r["total_exposure"],
                "contract_count": r["contract_count"],
                "internal_trade_amount": r["internal_trade_amount"],
                "link_types": ", ".join(r["link_types"]),
                "penalty_count": r["penalty_count"],
                "abnormal_count": r["abnormal_count"],
                "risk_score": r["risk_score"],
                "risk_level": r["risk_level"]
            })

        os.makedirs(REPORTS_DIR, exist_ok=True)

        cluster_df = pd.DataFrame(cluster_data)
        cluster_file = os.path.join(REPORTS_DIR, "shell_entity_cluster_report.csv")
        cluster_df.to_csv(cluster_file, index=False, encoding="utf-8-sig")

        # Controller report
        controller_data = []
        for r in results:
            for ctrl in r["controllers"]:
                controller_data.append({
                    "cluster_id": r["cluster_id"],
                    "person_id": ctrl["person_id"],
                    "person_name": ctrl["name"],
                    "phone": ctrl["phone"],
                    "role": ctrl["role"],
                    "controlled_companies": ctrl["controlled_count"],
                    "companies": "; ".join(ctrl["companies"][:5])
                })

        if controller_data:
            controller_df = pd.DataFrame(controller_data)
            controller_file = os.path.join(REPORTS_DIR, "shell_entity_controller_report.csv")
            controller_df.to_csv(controller_file, index=False, encoding="utf-8-sig")

        print("\n[4/4] 分析结果...")

        print("\n" + "=" * 70)
        print("高风险马甲公司集群 TOP 5：")
        print("=" * 70)

        for r in results[:5]:
            print(f"\n{r['cluster_id']} [{r['risk_level']}]")
            print(f"  风险分数: {r['risk_score']:.4f}")
            print(f"  公司数量: {r['company_count']}")
            print(f"  关联方式: {', '.join(r['link_types'])}")
            print(f"  控制人: {', '.join(r['control_persons'][:3])}")
            print(f"  授信敞口: ¥{r['total_exposure']:,.2f}")
            print(f"  内部交易: ¥{r['internal_trade_amount']:,.2f}")
            print(f"  行政处罚: {r['penalty_count']}次")
            print(f"  经营异常: {r['abnormal_count']}次")
            print(f"  公司列表: {', '.join(r['company_names'][:3])}{'...' if len(r['company_names']) > 3 else ''}")

        print(f"\n完整报告已保存至:")
        print(f"  - reports/shell_entity_cluster_report.csv")
        print(f"  - reports/shell_entity_controller_report.csv")

    finally:
        if session:
            session.release()


if __name__ == "__main__":
    main()

