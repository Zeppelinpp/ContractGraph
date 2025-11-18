from __future__ import annotations

import csv
import re
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
MOCK_DIR = DATA_DIR / "mock_data"
GRAPH_DIR = DATA_DIR / "graph_data"

TYPE_PREFIX_MAP = {
    "bos_org": "ORG",
    "bd_supplier": "SUP",
    "bd_customer": "CUS",
    "mscon_counterpart": "CP",
}

PARTY_FIELDS = [
    ("A", "FPARTAID", "FPARTANAME", "FPATYPE", "甲方"),
    ("B", "FPARTBID", "FPARTBNAME", "FPBTYPE", "乙方"),
    ("C", "FPARTCID", "FPARTCNAME", "FPCTYPE", "丙方"),
    ("D", "FPARTDID", "FPARTDNAME", "FPDTYPE", "丁方"),
]


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if "." in text:
            return int(float(text))
        return int(text)
    except ValueError:
        return None


def format_amount(value: Optional[str]) -> str:
    if value is None or str(value).strip() == "":
        return "0.00"
    try:
        return f"{Decimal(str(value).strip()):.2f}"
    except (InvalidOperation, ValueError):
        return "0.00"


def split_names(name: Optional[str]) -> List[str]:
    if not name:
        return []
    parts = re.split(r"[、，,;/；\s]+", name.strip())
    return [p for p in (part.strip() for part in parts) if p]


def write_csv(path: Path, fieldnames: List[str], rows: Iterable[Dict[str, str]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") or "" for field in fieldnames})


class GraphDataGenerator:
    def __init__(self, mock_dir: Path, graph_dir: Path):
        self.mock_dir = mock_dir
        self.graph_dir = graph_dir

        self.tables: Dict[str, List[Dict[str, str]]] = {}
        self.person_nodes: List[Dict[str, str]] = []
        self.company_nodes: List[Dict[str, str]] = []
        self.contract_nodes: List[Dict[str, str]] = []
        self.legal_event_nodes: List[Dict[str, str]] = []
        self.transaction_nodes: List[Dict[str, str]] = []

        self.edges_legal_person: List[Dict[str, str]] = []
        self.edges_controls: List[Dict[str, str]] = []
        self.edges_party: List[Dict[str, str]] = []
        self.edges_trades: List[Dict[str, str]] = []
        self.edges_case_person: List[Dict[str, str]] = []
        self.edges_case_contract: List[Dict[str, str]] = []
        self.edges_dispute_contract: List[Dict[str, str]] = []
        self.edges_is_supplier: List[Dict[str, str]] = []
        self.edges_is_customer: List[Dict[str, str]] = []
        self.edges_company_transaction: List[Dict[str, str]] = []

        self.person_name_to_ids: Dict[str, List[str]] = defaultdict(list)
        self.user_id_by_fid: Dict[int, str] = {}
        self.company_lookup: Dict[Tuple[str, int], str] = {}
        self.company_name_lookup: Dict[str, str] = {}
        self.company_legal_info: List[Tuple[str, str, str]] = []
        self.contract_lookup: Dict[int, Dict[str, str]] = {}
        self.case_lookup: Dict[int, Dict[str, str]] = {}
        self.dispute_lookup: Dict[int, Dict[str, str]] = {}
        self.company_nodes_by_type: Dict[str, List[str]] = defaultdict(list)

    def run(self):
        self.load_tables()
        self.build_nodes()
        self.build_edges()
        self.write_outputs()

    def load_tables(self):
        patterns = {
            "users": "t_sec_user_*.csv",
            "orgs": "t_org_org_*.csv",
            "suppliers": "t_bd_supplier_*.csv",
            "customers": "t_bd_customer_*.csv",
            "counterparts": "t_mscon_counterpart_*.csv",
            "contracts": "t_mscon_contract_*.csv",
            "cases": "t_conl_case_*.csv",
            "disputes": "t_conl_disputeregist_*.csv",
            "plan_in": "t_mscon_performplanin_*.csv",
            "plan_out": "t_mscon_performplanout_*.csv",
        }
        for name, pattern in patterns.items():
            matches = sorted(self.mock_dir.glob(pattern))
            if not matches:
                raise FileNotFoundError(f"Mock data not found for pattern {pattern}")
            with matches[0].open("r", encoding="utf-8-sig") as f:
                self.tables[name] = list(csv.DictReader(f))

    def build_nodes(self):
        self._build_person_nodes()
        self._build_company_nodes()
        self._build_contract_nodes()
        self._build_legal_event_nodes()
        self._build_transactions()

    def _build_person_nodes(self):
        rows = sorted(self.tables["users"], key=lambda r: parse_int(r.get("FID")) or 0)
        for row in rows:
            fid = parse_int(row.get("FID"))
            if fid is None:
                continue
            node_id = f"USER_{fid:03d}"
            person = {
                "node_id": node_id,
                "node_type": "Person",
                "name": row.get("FTRUENAME", "").strip(),
                "number": row.get("FNUMBER", "").strip(),
                "id_card": row.get("FIDCARD", "").strip(),
                "gender": row.get("FGENDER", "").strip(),
                "birthday": row.get("FBIRTHDAY", "").strip(),
                "status": row.get("FSTATUS", "").strip(),
            }
            self.person_nodes.append(person)
            self.user_id_by_fid[fid] = node_id
            name = person["name"]
            if name:
                self.person_name_to_ids[name].append(node_id)

    def _register_company(
        self,
        prefix: str,
        fid: Optional[str],
        name: str,
        number: str,
        legal_person: str,
        credit_code: str,
        establish_date: str,
        status: str,
        description: str,
        type_key: str,
    ):
        fid_int = parse_int(fid)
        if fid_int is None:
            return
        node_id = f"{prefix}_{fid_int:03d}"
        node = {
            "node_id": node_id,
            "node_type": "Company",
            "name": name,
            "number": number,
            "legal_person": legal_person,
            "credit_code": credit_code,
            "establish_date": establish_date,
            "status": status,
            "description": description,
        }
        self.company_nodes.append(node)
        self.company_lookup[(prefix.lower(), fid_int)] = node_id
        if type_key:
            self.company_lookup[(type_key.lower(), fid_int)] = node_id
        self.company_name_lookup[node_id] = name or node_id
        self.company_nodes_by_type[prefix].append(node_id)
        if legal_person:
            self.company_legal_info.append((node_id, name, legal_person))

    def _build_company_nodes(self):
        org_rows = sorted(self.tables["orgs"], key=lambda r: parse_int(r.get("FID")) or 0)
        for row in org_rows:
            self._register_company(
                "ORG",
                row.get("FID"),
                row.get("FNAME", "").strip(),
                row.get("FNUMBER", "").strip(),
                row.get("FARTIFICIALPERSON", "").strip(),
                row.get("FUNIFORMSOCIALCREDITCODE", "").strip(),
                row.get("FESTABLISHMENTDATE", "").strip(),
                row.get("FSTATUS", "").strip(),
                row.get("FDESCRIPTION", "").strip(),
                "bos_org",
            )

        supplier_rows = sorted(
            self.tables["suppliers"], key=lambda r: parse_int(r.get("FID")) or 0
        )
        for row in supplier_rows:
            self._register_company(
                "SUP",
                row.get("FID"),
                row.get("FNAME", "").strip(),
                row.get("FNUMBER", "").strip(),
                row.get("FARTIFICIALPERSON", "").strip(),
                row.get("FTAXNO", "").strip(),
                row.get("FCREATETIME", "").strip(),
                row.get("FSTATUS", "").strip(),
                row.get("FBUSINESSSCOPE", "").strip(),
                "bd_supplier",
            )

        customer_rows = sorted(
            self.tables["customers"], key=lambda r: parse_int(r.get("FID")) or 0
        )
        for row in customer_rows:
            self._register_company(
                "CUS",
                row.get("FID"),
                row.get("FNAME", "").strip(),
                row.get("FNUMBER", "").strip(),
                row.get("FARTIFICIALPERSON", "").strip(),
                row.get("FTAXNO", "").strip(),
                row.get("FCREATETIME", "").strip(),
                row.get("FSTATUS", "").strip(),
                row.get("FBUSINESSSCOPE", "").strip(),
                "bd_customer",
            )

        counterpart_rows = sorted(
            self.tables["counterparts"], key=lambda r: parse_int(r.get("FID")) or 0
        )
        for row in counterpart_rows:
            self._register_company(
                "CP",
                row.get("FID"),
                row.get("FNAME", "").strip(),
                row.get("FNUMBER", "").strip(),
                row.get("FARTIFICIALPERSON", "").strip(),
                "",
                row.get("FCREATETIME", "").strip(),
                row.get("FSTATUS", "").strip(),
                row.get("FBUSINESSSCOPE", "").strip(),
                "mscon_counterpart",
            )

        self.company_nodes.sort(key=lambda n: n["node_id"])

    def _build_contract_nodes(self):
        rows = sorted(self.tables["contracts"], key=lambda r: parse_int(r.get("FID")) or 0)
        for row in rows:
            fid = parse_int(row.get("FID"))
            if fid is None:
                continue
            node_id = f"CON_{fid:03d}"
            contract = {
                "node_id": node_id,
                "node_type": "Contract",
                "contract_no": row.get("FBILLNO", "").strip(),
                "contract_name": row.get("FBILLNAME", "").strip(),
                "amount": format_amount(row.get("FSIGNALLAMOUNT")),
                "sign_date": row.get("FBIZTIME", "").strip(),
                "status": row.get("FCONTSTATUS", "").strip(),
                "description": row.get("FDESCRIPTION", "").strip(),
            }
            self.contract_nodes.append(contract)
            self.contract_lookup[fid] = {**row, "node_id": node_id, "name": contract["contract_name"]}

    def _build_legal_event_nodes(self):
        case_rows = sorted(self.tables["cases"], key=lambda r: parse_int(r.get("FID")) or 0)
        for row in case_rows:
            fid = parse_int(row.get("FID"))
            if fid is None:
                continue
            node_id = f"CASE_{fid:03d}"
            event = {
                "node_id": node_id,
                "node_type": "LegalEvent",
                "event_type": "Case",
                "event_no": row.get("FBILLNO", "").strip(),
                "event_name": row.get("FNAME", "").strip(),
                "amount": format_amount(row.get("FLAWSUITAMOUNT")),
                "status": row.get("FCASESTATUS", "").strip(),
                "register_date": row.get("FREGISTDATE", "").strip(),
                "description": row.get("FINTRODUCTION", "").strip() or row.get("FDESCRIPTION", "").strip(),
            }
            self.legal_event_nodes.append(event)
            self.case_lookup[fid] = {"node_id": node_id, **row}

        dispute_rows = sorted(self.tables["disputes"], key=lambda r: parse_int(r.get("FID")) or 0)
        for row in dispute_rows:
            fid = parse_int(row.get("FID"))
            if fid is None:
                continue
            node_id = f"DISP_{fid:03d}"
            event = {
                "node_id": node_id,
                "node_type": "LegalEvent",
                "event_type": "Dispute",
                "event_no": row.get("FBILLNO", "").strip(),
                "event_name": row.get("FNAME", "").strip(),
                "amount": format_amount(row.get("FDISPUTEAMOUNT")),
                "status": row.get("FDISPUTESTATUS", "").strip(),
                "register_date": row.get("FREGISTDATE", "").strip(),
                "description": row.get("FDISPUTEINTRODUCTION", "").strip()
                or row.get("FDESCRIPTION", "").strip(),
            }
            self.legal_event_nodes.append(event)
            self.dispute_lookup[fid] = {"node_id": node_id, **row}

        self.legal_event_nodes.sort(key=lambda n: n["node_id"])

    def _get_company_node(self, fid: Optional[str], company_type: Optional[str]) -> Optional[str]:
        fid_int = parse_int(fid)
        if fid_int is None:
            return None
        if company_type:
            type_key = company_type.strip().lower()
            if (type_key, fid_int) in self.company_lookup:
                return self.company_lookup[(type_key, fid_int)]
            prefix = TYPE_PREFIX_MAP.get(type_key)
            if prefix and (prefix.lower(), fid_int) in self.company_lookup:
                return self.company_lookup[(prefix.lower(), fid_int)]
        for key in TYPE_PREFIX_MAP.values():
            lookup_key = (key.lower(), fid_int)
            if lookup_key in self.company_lookup:
                return self.company_lookup[lookup_key]
        return None

    def _build_transactions(self):
        plan_in = sorted(self.tables["plan_in"], key=lambda r: parse_int(r.get("FID")) or 0)
        plan_out = sorted(self.tables["plan_out"], key=lambda r: parse_int(r.get("FID")) or 0)

        edge_counter = 1

        def create_edges(txn_id: str, payer: Optional[str], receiver: Optional[str], pay_desc: str, recv_desc: str):
            nonlocal edge_counter
            if payer:
                self.edges_company_transaction.append(
                    {
                        "edge_id": f"CT_{edge_counter:06d}",
                        "edge_type": "PAYS",
                        "from_node": payer,
                        "to_node": txn_id,
                        "from_type": "Company",
                        "to_type": "Transaction",
                        "properties": pay_desc,
                    }
                )
                edge_counter += 1
            if receiver:
                self.edges_company_transaction.append(
                    {
                        "edge_id": f"CT_{edge_counter:06d}",
                        "edge_type": "RECEIVES",
                        "from_node": txn_id,
                        "to_node": receiver,
                        "from_type": "Transaction",
                        "to_type": "Company",
                        "properties": recv_desc,
                    }
                )
                edge_counter += 1

        for row in plan_in:
            fid = parse_int(row.get("FID"))
            if fid is None:
                continue
            txn_id = f"TXN_IN_{fid:04d}"
            contract_id = parse_int(row.get("FCONTRACTID"))
            contract = self.contract_lookup.get(contract_id, {})
            party_a = self._get_company_node(row.get("FPARTAID"), contract.get("FPATYPE"))
            party_b = self._get_company_node(row.get("FPARTBID"), contract.get("FPBTYPE"))
            self.transaction_nodes.append(
                {
                    "node_id": txn_id,
                    "node_type": "Transaction",
                    "transaction_type": "INFLOW",
                    "transaction_no": row.get("FBILLNO", "").strip(),
                    "contract_no": row.get("FCONTRACTNO", "").strip(),
                    "amount": format_amount(row.get("FAMOUNT")),
                    "transaction_date": row.get("FBIZTIME", "").strip(),
                    "status": row.get("FSTATUS", "").strip(),
                    "description": row.get("FDESCRIPTION", "").strip(),
                }
            )
            pay_desc = f"付款-{row.get('FPARTBNAME', '').strip()}向{row.get('FPARTANAME', '').strip()}支付"
            recv_desc = f"收款-{row.get('FPARTANAME', '').strip()}收到{row.get('FPARTBNAME', '').strip()}付款"
            create_edges(txn_id, party_b, party_a, pay_desc, recv_desc)

        for row in plan_out:
            fid = parse_int(row.get("FID"))
            if fid is None:
                continue
            txn_id = f"TXN_OUT_{fid:04d}"
            contract_id = parse_int(row.get("FCONTRACTID"))
            contract = self.contract_lookup.get(contract_id, {})
            party_a = self._get_company_node(row.get("FPARTAID"), contract.get("FPATYPE"))
            party_b = self._get_company_node(row.get("FPARTBID"), contract.get("FPBTYPE"))
            self.transaction_nodes.append(
                {
                    "node_id": txn_id,
                    "node_type": "Transaction",
                    "transaction_type": "OUTFLOW",
                    "transaction_no": row.get("FBILLNO", "").strip(),
                    "contract_no": row.get("FCONTRACTNO", "").strip(),
                    "amount": format_amount(row.get("FAMOUNT")),
                    "transaction_date": row.get("FBIZTIME", "").strip(),
                    "status": row.get("FSTATUS", "").strip(),
                    "description": row.get("FDESCRIPTION", "").strip(),
                }
            )
            pay_desc = f"付款-{row.get('FPARTANAME', '').strip()}向{row.get('FPARTBNAME', '').strip()}支付"
            recv_desc = f"收款-{row.get('FPARTBNAME', '').strip()}收到{row.get('FPARTANAME', '').strip()}付款"
            create_edges(txn_id, party_a, party_b, pay_desc, recv_desc)

        self.transaction_nodes.sort(key=lambda n: n["node_id"])

    def build_edges(self):
        self._build_legal_person_edges()
        self._build_control_edges()
        self._build_party_edges()
        self._build_trade_edges()
        self._build_supplier_customer_edges()
        self._build_case_edges()
        self._build_dispute_edges()

    def _build_legal_person_edges(self):
        person_ids = [node["node_id"] for node in self.person_nodes]
        edge_counter = 1

        def assign_edges(company_ids: List[str], users: List[str]):
            nonlocal edge_counter
            for company_id, person_id in zip(company_ids, users):
                self.edges_legal_person.append(
                    {
                        "edge_id": f"LP_{edge_counter:04d}",
                        "edge_type": "LEGAL_PERSON",
                        "from_node": person_id,
                        "to_node": company_id,
                        "from_type": "Person",
                        "to_type": "Company",
                        "properties": f"法人代表-{self.company_name_lookup.get(company_id, company_id)}",
                    }
                )
                edge_counter += 1

        index = 0

        def take(count: int) -> List[str]:
            nonlocal index
            if index >= len(person_ids):
                return []
            chunk = person_ids[index : min(index + count, len(person_ids))]
            index += len(chunk)
            return chunk

        org_nodes = sorted(self.company_nodes_by_type.get("ORG", []))
        sup_nodes = sorted(self.company_nodes_by_type.get("SUP", []))
        cus_nodes = sorted(self.company_nodes_by_type.get("CUS", []))
        cp_nodes = sorted(self.company_nodes_by_type.get("CP", []))

        assign_edges(org_nodes, take(len(org_nodes)))
        assign_edges(sup_nodes, take(len(sup_nodes)))
        assign_edges(cus_nodes, take(len(cus_nodes)))

        if cp_nodes:
            cp_user_ids = person_ids[max(0, len(person_ids) - len(cp_nodes)) :]
            assign_edges(cp_nodes, cp_user_ids)

    def _build_control_edges(self):
        org_rows = sorted(self.tables["orgs"], key=lambda r: parse_int(r.get("FID")) or 0)
        edge_counter = 1
        for row in org_rows:
            parent = self._get_company_node(row.get("FPARENTORGID"), "bos_org")
            child = self._get_company_node(row.get("FID"), "bos_org")
            if parent and child:
                self.edges_controls.append(
                    {
                        "edge_id": f"CTRL_{edge_counter:04d}",
                        "edge_type": "CONTROLS",
                        "from_node": parent,
                        "to_node": child,
                        "from_type": "Company",
                        "to_type": "Company",
                        "properties": "控股关系",
                    }
                )
                edge_counter += 1

    def _build_party_edges(self):
        contracts = sorted(self.tables["contracts"], key=lambda r: parse_int(r.get("FID")) or 0)
        edge_counter = 1
        for row in contracts:
            contract_id = parse_int(row.get("FID"))
            contract_node = self.contract_lookup.get(contract_id, {}).get("node_id")
            if not contract_node:
                continue
            for suffix, id_field, name_field, type_field, label in PARTY_FIELDS:
                company_node = self._get_company_node(row.get(id_field), row.get(type_field))
                if not company_node:
                    continue
                self.edges_party.append(
                    {
                        "edge_id": f"PARTY_{edge_counter:04d}",
                        "edge_type": f"PARTY_{suffix}",
                        "from_node": company_node,
                        "to_node": contract_node,
                        "from_type": "Company",
                        "to_type": "Contract",
                        "properties": f"{label}-{row.get(name_field, '').strip()}",
                    }
                )
                edge_counter += 1

    def _build_trade_edges(self):
        contracts = sorted(self.tables["contracts"], key=lambda r: parse_int(r.get("FID")) or 0)
        edge_counter = 1
        for row in contracts:
            party_a = self._get_company_node(row.get("FPARTAID"), row.get("FPATYPE"))
            party_b = self._get_company_node(row.get("FPARTBID"), row.get("FPBTYPE"))
            if not party_a or not party_b:
                continue
            self.edges_trades.append(
                {
                    "edge_id": f"TRADE_{edge_counter:04d}",
                    "edge_type": "TRADES_WITH",
                    "from_node": party_a,
                    "to_node": party_b,
                    "from_type": "Company",
                    "to_type": "Company",
                    "properties": f"交易金额:{format_amount(row.get('FSIGNALLAMOUNT'))},合同:{row.get('FBILLNO', '').strip()}",
                }
            )
            edge_counter += 1

    def _build_supplier_customer_edges(self):
        supplier_relations = set()
        customer_relations = set()
        supplier_counter = 1
        customer_counter = 1
        for row in self.tables["contracts"]:
            party_a = self._get_company_node(row.get("FPARTAID"), row.get("FPATYPE"))
            party_b = self._get_company_node(row.get("FPARTBID"), row.get("FPBTYPE"))
            party_a_name = row.get("FPARTANAME", "").strip() or self.company_name_lookup.get(party_a, "")
            party_b_name = row.get("FPARTBNAME", "").strip() or self.company_name_lookup.get(party_b, "")

            if row.get("FPBTYPE") == "bd_supplier" and party_a and party_b:
                key = (party_b, party_a)
                if key not in supplier_relations:
                    supplier_relations.add(key)
                    self.edges_is_supplier.append(
                        {
                            "edge_id": f"SUP_REL_{supplier_counter:04d}",
                            "edge_type": "IS_SUPPLIER",
                            "from_node": party_b,
                            "to_node": party_a,
                            "from_type": "Company",
                            "to_type": "Company",
                            "properties": f"供应商关系-{party_b_name}为{party_a_name}提供服务",
                        }
                    )
                    supplier_counter += 1

            if row.get("FPATYPE") == "bd_supplier" and party_a and party_b:
                key = (party_a, party_b)
                if key not in supplier_relations:
                    supplier_relations.add(key)
                    self.edges_is_supplier.append(
                        {
                            "edge_id": f"SUP_REL_{supplier_counter:04d}",
                            "edge_type": "IS_SUPPLIER",
                            "from_node": party_a,
                            "to_node": party_b,
                            "from_type": "Company",
                            "to_type": "Company",
                            "properties": f"供应商关系-{party_a_name}为{party_b_name}提供服务",
                        }
                    )
                    supplier_counter += 1

            if row.get("FPBTYPE") == "bd_customer" and party_a and party_b:
                key = (party_b, party_a)
                if key not in customer_relations:
                    customer_relations.add(key)
                    self.edges_is_customer.append(
                        {
                            "edge_id": f"CUS_REL_{customer_counter:04d}",
                            "edge_type": "IS_CUSTOMER",
                            "from_node": party_b,
                            "to_node": party_a,
                            "from_type": "Company",
                            "to_type": "Company",
                            "properties": f"客户关系-{party_b_name}是{party_a_name}的客户",
                        }
                    )
                    customer_counter += 1

            if row.get("FPATYPE") == "bd_customer" and party_a and party_b:
                key = (party_a, party_b)
                if key not in customer_relations:
                    customer_relations.add(key)
                    self.edges_is_customer.append(
                        {
                            "edge_id": f"CUS_REL_{customer_counter:04d}",
                            "edge_type": "IS_CUSTOMER",
                            "from_node": party_a,
                            "to_node": party_b,
                            "from_type": "Company",
                            "to_type": "Company",
                            "properties": f"客户关系-{party_a_name}是{party_b_name}的客户",
                        }
                    )
                    customer_counter += 1

    def _build_case_edges(self):
        case_rows = sorted(self.tables["cases"], key=lambda r: parse_int(r.get("FID")) or 0)
        edge_case_person_counter = 1
        edge_case_contract_counter = 1
        for row in case_rows:
            case_id = parse_int(row.get("FID"))
            case_node = self.case_lookup.get(case_id, {}).get("node_id")
            if not case_node:
                continue
            operator = self.user_id_by_fid.get(parse_int(row.get("FOPERATORID")))
            if operator:
                self.edges_case_person.append(
                    {
                        "edge_id": f"CASE_P_{edge_case_person_counter:04d}",
                        "edge_type": "INVOLVED_IN",
                        "from_node": operator,
                        "to_node": case_node,
                        "from_type": "Person",
                        "to_type": "LegalEvent",
                        "properties": f"{row.get('FOPERATORNAME', '').strip()}办理{row.get('FNAME', '').strip()}",
                    }
                )
                edge_case_person_counter += 1

            contract_id = parse_int(row.get("FRELATECONTRACTID"))
            contract_node = self.contract_lookup.get(contract_id, {}).get("node_id")
            if contract_node:
                self.edges_case_contract.append(
                    {
                        "edge_id": f"CASE_C_{edge_case_contract_counter:04d}",
                        "edge_type": "RELATED_TO",
                        "from_node": contract_node,
                        "to_node": case_node,
                        "from_type": "Contract",
                        "to_type": "LegalEvent",
                        "properties": f"关联合同-{self.contract_lookup[contract_id].get('name', '')}",
                    }
                )
                edge_case_contract_counter += 1

    def _build_dispute_edges(self):
        dispute_rows = sorted(self.tables["disputes"], key=lambda r: parse_int(r.get("FID")) or 0)
        edge_counter = 1
        for row in dispute_rows:
            dispute_id = parse_int(row.get("FID"))
            dispute_node = self.dispute_lookup.get(dispute_id, {}).get("node_id")
            if not dispute_node:
                continue
            contract_id = parse_int(row.get("FRELATECONTRACTID"))
            contract_node = self.contract_lookup.get(contract_id, {}).get("node_id")
            if contract_node:
                self.edges_dispute_contract.append(
                    {
                        "edge_id": f"DISP_C_{edge_counter:04d}",
                        "edge_type": "RELATED_TO",
                        "from_node": contract_node,
                        "to_node": dispute_node,
                        "from_type": "Contract",
                        "to_type": "LegalEvent",
                        "properties": f"关联合同-{self.contract_lookup[contract_id].get('name', '')}",
                    }
                )
                edge_counter += 1

    def write_outputs(self):
        write_csv(
            self.graph_dir / "nodes_person.csv",
            ["node_id", "node_type", "name", "number", "id_card", "gender", "birthday", "status"],
            self.person_nodes,
        )
        write_csv(
            self.graph_dir / "nodes_company.csv",
            [
                "node_id",
                "node_type",
                "name",
                "number",
                "legal_person",
                "credit_code",
                "establish_date",
                "status",
                "description",
            ],
            self.company_nodes,
        )
        write_csv(
            self.graph_dir / "nodes_contract.csv",
            [
                "node_id",
                "node_type",
                "contract_no",
                "contract_name",
                "amount",
                "sign_date",
                "status",
                "description",
            ],
            self.contract_nodes,
        )
        write_csv(
            self.graph_dir / "nodes_legal_event.csv",
            [
                "node_id",
                "node_type",
                "event_type",
                "event_no",
                "event_name",
                "amount",
                "status",
                "register_date",
                "description",
            ],
            self.legal_event_nodes,
        )
        write_csv(
            self.graph_dir / "nodes_transaction.csv",
            [
                "node_id",
                "node_type",
                "transaction_type",
                "transaction_no",
                "contract_no",
                "amount",
                "transaction_date",
                "status",
                "description",
            ],
            self.transaction_nodes,
        )
        write_csv(
            self.graph_dir / "edges_legal_person.csv",
            ["edge_id", "edge_type", "from_node", "to_node", "from_type", "to_type", "properties"],
            self.edges_legal_person,
        )
        write_csv(
            self.graph_dir / "edges_controls.csv",
            ["edge_id", "edge_type", "from_node", "to_node", "from_type", "to_type", "properties"],
            self.edges_controls,
        )
        write_csv(
            self.graph_dir / "edges_party.csv",
            ["edge_id", "edge_type", "from_node", "to_node", "from_type", "to_type", "properties"],
            self.edges_party,
        )
        write_csv(
            self.graph_dir / "edges_trades_with.csv",
            ["edge_id", "edge_type", "from_node", "to_node", "from_type", "to_type", "properties"],
            self.edges_trades,
        )
        write_csv(
            self.graph_dir / "edges_case_person.csv",
            ["edge_id", "edge_type", "from_node", "to_node", "from_type", "to_type", "properties"],
            self.edges_case_person,
        )
        write_csv(
            self.graph_dir / "edges_case_contract.csv",
            ["edge_id", "edge_type", "from_node", "to_node", "from_type", "to_type", "properties"],
            self.edges_case_contract,
        )
        write_csv(
            self.graph_dir / "edges_dispute_contract.csv",
            ["edge_id", "edge_type", "from_node", "to_node", "from_type", "to_type", "properties"],
            self.edges_dispute_contract,
        )
        write_csv(
            self.graph_dir / "edges_is_supplier.csv",
            ["edge_id", "edge_type", "from_node", "to_node", "from_type", "to_type", "properties"],
            self.edges_is_supplier,
        )
        write_csv(
            self.graph_dir / "edges_is_customer.csv",
            ["edge_id", "edge_type", "from_node", "to_node", "from_type", "to_type", "properties"],
            self.edges_is_customer,
        )
        write_csv(
            self.graph_dir / "edges_company_transaction.csv",
            ["edge_id", "edge_type", "from_node", "to_node", "from_type", "to_type", "properties"],
            self.edges_company_transaction,
        )


def main():
    generator = GraphDataGenerator(MOCK_DIR, GRAPH_DIR)
    generator.run()
    print("Graph data regenerated from mock data.")


if __name__ == "__main__":
    main()
