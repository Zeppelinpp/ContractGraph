"""
Generate IS_SUPPLIER and IS_CUSTOMER edges from contract data.
"""

import csv
import os
from collections import defaultdict

# Paths
BASE_DIR = os.path.join(os.path.dirname(__file__), "../..", "data")
MOCK_DIR = os.path.join(BASE_DIR, "mock_data")
GRAPH_DIR = os.path.join(BASE_DIR, "graph_data")

# ID mapping for companies
COMPANY_ID_MAP = {
    1: "ORG_001", 2: "ORG_002", 3: "ORG_003", 4: "ORG_004", 5: "ORG_005",
    6: "ORG_006", 7: "ORG_007", 8: "ORG_008", 9: "ORG_009", 10: "ORG_010",
    11: "ORG_011", 12: "ORG_012", 13: "ORG_013", 14: "ORG_014", 15: "ORG_015",
    16: "ORG_016", 17: "ORG_017", 18: "ORG_018", 19: "ORG_019", 20: "ORG_020",
}

SUPPLIER_ID_MAP = {i: f"SUP_{i:03d}" for i in range(1, 31)}
CUSTOMER_ID_MAP = {i: f"CUS_{i:03d}" for i in range(1, 31)}
COUNTERPART_ID_MAP = {i: f"CP_{i:03d}" for i in range(1, 11)}


def get_company_id(fid, company_type):
    """Get company node_id based on FID and company type."""
    fid = int(fid)
    if company_type == "bos_org":
        return COMPANY_ID_MAP.get(fid, f"ORG_{fid:03d}")
    elif company_type == "bd_supplier":
        return SUPPLIER_ID_MAP.get(fid, f"SUP_{fid:03d}")
    elif company_type == "bd_customer":
        return CUSTOMER_ID_MAP.get(fid, f"CUS_{fid:03d}")
    elif company_type == "mscon_counterpart":
        return COUNTERPART_ID_MAP.get(fid, f"CP_{fid:03d}")
    return None


def generate_company_relationship_edges():
    """Generate IS_SUPPLIER and IS_CUSTOMER edges from contracts."""
    contract_file = os.path.join(MOCK_DIR, "t_mscon_contract_虚拟数据.csv")
    
    # Track unique relationships to avoid duplicates
    supplier_relations = {}  # (from_node, to_node) -> edge_data
    customer_relations = {}  # (from_node, to_node) -> edge_data
    
    edge_id_supplier = 1
    edge_id_customer = 1
    
    with open(contract_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            party_a_id = get_company_id(row["FPARTAID"], row["FPATYPE"])
            party_a_name = row["FPARTANAME"]
            party_a_type = row["FPATYPE"]
            
            party_b_id = get_company_id(row["FPARTBID"], row["FPBTYPE"])
            party_b_name = row["FPARTBNAME"]
            party_b_type = row["FPBTYPE"]
            
            if not party_a_id or not party_b_id:
                continue
            
            # Determine relationship based on party types
            # If party B is a supplier, then party A is a customer of party B
            if party_b_type == "bd_supplier":
                # party_b IS_SUPPLIER -> party_a
                key = (party_b_id, party_a_id)
                if key not in supplier_relations:
                    supplier_relations[key] = {
                        "from_node": party_b_id,
                        "to_node": party_a_id,
                        "from_name": party_b_name,
                        "to_name": party_a_name,
                    }
            
            # If party A is a supplier, then party B is a customer of party A
            if party_a_type == "bd_supplier":
                # party_a IS_SUPPLIER -> party_b
                key = (party_a_id, party_b_id)
                if key not in supplier_relations:
                    supplier_relations[key] = {
                        "from_node": party_a_id,
                        "to_node": party_b_id,
                        "from_name": party_a_name,
                        "to_name": party_b_name,
                    }
            
            # If party B is a customer, then party A supplies to party B
            if party_b_type == "bd_customer":
                # party_a IS_CUSTOMER -> party_b (from perspective of being served)
                key = (party_b_id, party_a_id)
                if key not in customer_relations:
                    customer_relations[key] = {
                        "from_node": party_b_id,
                        "to_node": party_a_id,
                        "from_name": party_b_name,
                        "to_name": party_a_name,
                    }
            
            # If party A is a customer, then party B supplies to party A
            if party_a_type == "bd_customer":
                # party_a IS_CUSTOMER -> party_b
                key = (party_a_id, party_b_id)
                if key not in customer_relations:
                    customer_relations[key] = {
                        "from_node": party_a_id,
                        "to_node": party_b_id,
                        "from_name": party_a_name,
                        "to_name": party_b_name,
                    }
    
    # Write IS_SUPPLIER edges
    supplier_file = os.path.join(GRAPH_DIR, "edges_is_supplier.csv")
    with open(supplier_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["edge_id", "edge_type", "from_node", "to_node", "from_type", "to_type", "properties"])
        
        for (from_node, to_node), data in sorted(supplier_relations.items()):
            edge_id = f"SUP_REL_{edge_id_supplier:04d}"
            properties = f"供应商关系-{data['from_name']}为{data['to_name']}提供产品/服务"
            writer.writerow([
                edge_id, "IS_SUPPLIER", from_node, to_node,
                "Company", "Company", properties
            ])
            edge_id_supplier += 1
    
    # Write IS_CUSTOMER edges
    customer_file = os.path.join(GRAPH_DIR, "edges_is_customer.csv")
    with open(customer_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["edge_id", "edge_type", "from_node", "to_node", "from_type", "to_type", "properties"])
        
        for (from_node, to_node), data in sorted(customer_relations.items()):
            edge_id = f"CUS_REL_{edge_id_customer:04d}"
            properties = f"客户关系-{data['from_name']}是{data['to_name']}的客户"
            writer.writerow([
                edge_id, "IS_CUSTOMER", from_node, to_node,
                "Company", "Company", properties
            ])
            edge_id_customer += 1
    
    print(f"Generated {len(supplier_relations)} IS_SUPPLIER edges")
    print(f"Generated {len(customer_relations)} IS_CUSTOMER edges")


if __name__ == "__main__":
    generate_company_relationship_edges()

