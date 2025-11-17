"""
Generate Transaction nodes and edges from performplan data.
"""

import csv
import os

# Paths
BASE_DIR = os.path.join(os.path.dirname(__file__), "../..", "data")
MOCK_DIR = os.path.join(BASE_DIR, "mock_data")
GRAPH_DIR = os.path.join(BASE_DIR, "graph_data")

# ID mapping
COMPANY_ID_MAP = {
    1: "ORG_001", 2: "ORG_002", 3: "ORG_003", 4: "ORG_004", 5: "ORG_005",
    6: "ORG_006", 7: "ORG_007", 8: "ORG_008", 9: "ORG_009", 10: "ORG_010",
    11: "ORG_011", 12: "ORG_012", 13: "ORG_013", 14: "ORG_014", 15: "ORG_015",
    16: "ORG_016", 17: "ORG_017", 18: "ORG_018", 19: "ORG_019", 20: "ORG_020",
}

SUPPLIER_ID_MAP = {i: f"SUP_{i:03d}" for i in range(1, 31)}
CUSTOMER_ID_MAP = {i: f"CUS_{i:03d}" for i in range(1, 31)}
COUNTERPART_ID_MAP = {i: f"CP_{i:03d}" for i in range(1, 11)}


def get_company_id(fid):
    """Get company node_id based on FID."""
    fid = int(fid)
    # Try all mappings
    if fid in COMPANY_ID_MAP:
        return COMPANY_ID_MAP[fid]
    elif fid in SUPPLIER_ID_MAP:
        return SUPPLIER_ID_MAP[fid]
    elif fid in CUSTOMER_ID_MAP:
        return CUSTOMER_ID_MAP[fid]
    elif fid in COUNTERPART_ID_MAP:
        return COUNTERPART_ID_MAP[fid]
    return None


def generate_transaction_nodes_and_edges():
    """Generate Transaction nodes and edges from performplan in/out data."""
    
    # Read performplanin
    inflow_file = os.path.join(MOCK_DIR, "t_mscon_performplanin_虚拟数据.csv")
    outflow_file = os.path.join(MOCK_DIR, "t_mscon_performplanout_虚拟数据.csv")
    
    transactions = []
    company_transaction_edges = []
    
    edge_id = 1
    
    # Process inflow transactions
    with open(inflow_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            txn_id = f"TXN_IN_{int(row['FID']):04d}"
            party_a_id = get_company_id(row["FPARTAID"])
            party_b_id = get_company_id(row["FPARTBID"])
            
            if not party_a_id or not party_b_id:
                continue
            
            # Transaction node
            transactions.append({
                "node_id": txn_id,
                "node_type": "Transaction",
                "transaction_type": "INFLOW",
                "transaction_no": row["FBILLNO"],
                "contract_no": row["FCONTRACTNO"],
                "amount": row["FAMOUNT"],
                "transaction_date": row["FBIZTIME"],
                "status": row["FSTATUS"],
                "description": row["FDESCRIPTION"]
            })
            
            # Edge: party_b (payer) -> transaction
            company_transaction_edges.append({
                "edge_id": f"CT_{edge_id:06d}",
                "edge_type": "PAYS",
                "from_node": party_b_id,
                "to_node": txn_id,
                "from_type": "Company",
                "to_type": "Transaction",
                "properties": f"付款-{row['FPARTBNAME']}向{row['FPARTANAME']}支付"
            })
            edge_id += 1
            
            # Edge: transaction -> party_a (receiver)
            company_transaction_edges.append({
                "edge_id": f"CT_{edge_id:06d}",
                "edge_type": "RECEIVES",
                "from_node": txn_id,
                "to_node": party_a_id,
                "from_type": "Transaction",
                "to_type": "Company",
                "properties": f"收款-{row['FPARTANAME']}收到{row['FPARTBNAME']}付款"
            })
            edge_id += 1
    
    # Process outflow transactions
    with open(outflow_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            txn_id = f"TXN_OUT_{int(row['FID']):04d}"
            party_a_id = get_company_id(row["FPARTAID"])
            party_b_id = get_company_id(row["FPARTBID"])
            
            if not party_a_id or not party_b_id:
                continue
            
            # Transaction node
            transactions.append({
                "node_id": txn_id,
                "node_type": "Transaction",
                "transaction_type": "OUTFLOW",
                "transaction_no": row["FBILLNO"],
                "contract_no": row["FCONTRACTNO"],
                "amount": row["FAMOUNT"],
                "transaction_date": row["FBIZTIME"],
                "status": row["FSTATUS"],
                "description": row["FDESCRIPTION"]
            })
            
            # Edge: party_a (payer) -> transaction
            company_transaction_edges.append({
                "edge_id": f"CT_{edge_id:06d}",
                "edge_type": "PAYS",
                "from_node": party_a_id,
                "to_node": txn_id,
                "from_type": "Company",
                "to_type": "Transaction",
                "properties": f"付款-{row['FPARTANAME']}向{row['FPARTBNAME']}支付"
            })
            edge_id += 1
            
            # Edge: transaction -> party_b (receiver)
            company_transaction_edges.append({
                "edge_id": f"CT_{edge_id:06d}",
                "edge_type": "RECEIVES",
                "from_node": txn_id,
                "to_node": party_b_id,
                "from_type": "Transaction",
                "to_type": "Company",
                "properties": f"收款-{row['FPARTBNAME']}收到{row['FPARTANAME']}付款"
            })
            edge_id += 1
    
    # Write Transaction nodes
    txn_file = os.path.join(GRAPH_DIR, "nodes_transaction.csv")
    with open(txn_file, "w", encoding="utf-8", newline="") as f:
        fieldnames = ["node_id", "node_type", "transaction_type", "transaction_no", 
                      "contract_no", "amount", "transaction_date", "status", "description"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transactions)
    
    # Write Company-Transaction edges
    ct_edge_file = os.path.join(GRAPH_DIR, "edges_company_transaction.csv")
    with open(ct_edge_file, "w", encoding="utf-8", newline="") as f:
        fieldnames = ["edge_id", "edge_type", "from_node", "to_node", 
                      "from_type", "to_type", "properties"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(company_transaction_edges)
    
    print(f"Generated {len(transactions)} Transaction nodes")
    print(f"Generated {len(company_transaction_edges)} Company-Transaction edges")


if __name__ == "__main__":
    generate_transaction_nodes_and_edges()

