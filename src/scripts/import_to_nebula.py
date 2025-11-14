import psycopg2
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
from src.settings import settings
from nebula3.gclient.net import ConnectionPool, Session
from nebula3.Config import Config


def get_nebula_session() -> Session:
    """Create and return Nebula session"""
    config = Config()
    config.max_connection_pool_size = 10
    
    connection_pool = ConnectionPool()
    ok = connection_pool.init(
        [(settings.nebula_config["host"], settings.nebula_config["port"])], 
        config
    )
    if not ok:
        raise Exception("Failed to initialize connection pool")
    
    session = connection_pool.get_session(
        settings.nebula_config["user"], 
        settings.nebula_config["password"]
    )
    result = session.execute(f"USE {settings.nebula_config['space']}")
    if not result.is_succeeded():
        raise Exception(f"Failed to use space: {result.error_msg()}")
    return session


def ensure_space(session: Session):
    """Ensure space is selected"""
    result = session.execute(f"USE {settings.nebula_config['space']}")
    if not result.is_succeeded():
        raise Exception(f"Failed to use space: {result.error_msg()}")


def get_pg_connection(dbname: str = "postgres"):
    """Create PostgreSQL connection"""
    return psycopg2.connect(
        host=settings.pg_config["local"]["host"],
        port=settings.pg_config["local"]["port"],
        user=settings.pg_config["local"]["user"],
        password=settings.pg_config["local"]["password"],
        dbname=dbname
    )


def escape_string(value: Optional[str]) -> str:
    """Escape string for Nebula Graph"""
    if value is None:
        return ""
    return str(value).replace("'", "\\'").replace('"', '\\"')


def format_property(value) -> str:
    """Format property value for Nebula Graph"""
    if value is None:
        return '""'
    if isinstance(value, str):
        escaped = escape_string(value)
        return f'"{escaped}"'
    return str(value)


def import_person_nodes(session: Session, pg_conn, dbname: str):
    """Import Person nodes from t_sec_user"""
    ensure_space(session)
    cur = pg_conn.cursor()
    
    query = """
    SELECT 
        fid,
        fnumber,
        ftruename,
        fidcard,
        fgender,
        fbirthday,
        fstatus
    FROM t_sec_user
    WHERE fstatus = 'C'
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    if not rows:
        return 0
    
    batch_size = 100
    count = 0
    
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        statements = []
        
        for row in batch:
            fid, number, name, id_card, gender, birthday, status = row
            
            node_id = f"USER_{fid}"
            name_val = format_property(name)
            number_val = format_property(number)
            id_card_val = format_property(id_card)
            gender_val = format_property(gender)
            birthday_val = format_property(str(birthday) if birthday else None)
            status_val = format_property(status)
            
            statement = f'INSERT VERTEX Person(name, number, id_card, gender, birthday, status) VALUES "{node_id}":({name_val}, {number_val}, {id_card_val}, {gender_val}, {birthday_val}, {status_val})'
            statements.append(statement)
        
        batch_query = "; ".join(statements)
        result = session.execute(batch_query)
        if result.is_succeeded():
            count += len(batch)
        else:
            print(f"Error inserting Person batch: {result.error_msg()}")
    
    cur.close()
    return count


def import_company_nodes(session: Session, pg_conn, dbname: str):
    """Import Company nodes from multiple tables"""
    ensure_space(session)
    cur = pg_conn.cursor()
    
    companies = []
    
    # From t_org_org
    query_org = """
    SELECT 
        fid,
        fnumber,
        fname,
        funiformsocialcreditcode,
        fartificialperson,
        festablishmentdate,
        fstatus,
        forgtype
    FROM t_org_org
    WHERE fstatus = 'C'
    """
    cur.execute(query_org)
    for row in cur.fetchall():
        fid, number, name, credit_code, legal_person, establish_date, status, org_type = row
        companies.append({
            'node_id': f"ORG_{fid}",
            'company_type': 'Organization',
            'name': name,
            'number': number,
            'legal_person': legal_person,
            'credit_code': credit_code,
            'establish_date': establish_date,
            'status': status,
            'description': f"{name}的详细描述"
        })
    
    # From t_bd_supplier
    query_supplier = """
    SELECT 
        fid,
        fnumber,
        fname,
        fartificialperson,
        ftaxno,
        fstatus,
        fbusinessscope
    FROM t_bd_supplier
    WHERE fstatus = 'C'
    """
    cur.execute(query_supplier)
    for row in cur.fetchall():
        fid, number, name, legal_person, taxno, status, business_scope = row
        companies.append({
            'node_id': f"SUP_{fid}",
            'company_type': 'Supplier',
            'name': name,
            'number': number,
            'legal_person': legal_person,
            'credit_code': taxno or "",
            'establish_date': None,
            'status': status,
            'description': business_scope or f"{name}的详细描述"
        })
    
    # From t_bd_customer
    query_customer = """
    SELECT 
        fid,
        fnumber,
        fname,
        fartificialperson,
        ftaxno,
        fstatus,
        fbusinessscope
    FROM t_bd_customer
    WHERE fstatus = 'C'
    """
    cur.execute(query_customer)
    for row in cur.fetchall():
        fid, number, name, legal_person, taxno, status, business_scope = row
        companies.append({
            'node_id': f"CUS_{fid}",
            'company_type': 'Customer',
            'name': name,
            'number': number,
            'legal_person': legal_person,
            'credit_code': taxno or "",
            'establish_date': None,
            'status': status,
            'description': business_scope or f"{name}的详细描述"
        })
    
    # From t_mscon_counterpart
    query_counterpart = """
    SELECT 
        fid,
        fnumber,
        fname,
        fartificialperson,
        fstatus,
        fbusinessscope
    FROM t_mscon_counterpart
    WHERE fstatus = 'C'
    """
    cur.execute(query_counterpart)
    for row in cur.fetchall():
        fid, number, name, legal_person, status, business_scope = row
        companies.append({
            'node_id': f"CP_{fid}",
            'company_type': 'Counterpart',
            'name': name,
            'number': number,
            'legal_person': legal_person,
            'credit_code': "",
            'establish_date': None,
            'status': status,
            'description': business_scope or f"{name}的详细描述"
        })
    
    if not companies:
        cur.close()
        return 0
    
    batch_size = 100
    count = 0
    
    for i in range(0, len(companies), batch_size):
        batch = companies[i:i + batch_size]
        statements = []
        
        for company in batch:
            node_id = company['node_id']
            company_type = format_property(company['company_type'])
            name = format_property(company['name'])
            number = format_property(company['number'])
            legal_person = format_property(company['legal_person'])
            credit_code = format_property(company['credit_code'])
            establish_date = format_property(str(company['establish_date']) if company['establish_date'] else None)
            status = format_property(company['status'])
            description = format_property(company['description'])
            
            statement = f'INSERT VERTEX Company(company_type, name, number, legal_person, credit_code, establish_date, status, description) VALUES "{node_id}":({company_type}, {name}, {number}, {legal_person}, {credit_code}, {establish_date}, {status}, {description})'
            statements.append(statement)
        
        batch_query = "; ".join(statements)
        result = session.execute(batch_query)
        if result.is_succeeded():
            count += len(batch)
        else:
            print(f"Error inserting Company batch: {result.error_msg()}")
    
    cur.close()
    return count


def import_contract_nodes(session: Session, pg_conn, dbname: str):
    """Import Contract nodes from t_mscon_contract"""
    ensure_space(session)
    cur = pg_conn.cursor()
    
    query = """
    SELECT 
        fid,
        fbillno,
        fbillname,
        fsignallamount,
        fbiztime,
        fbillstatus,
        fcontstatus
    FROM t_mscon_contract
    WHERE fbillstatus = 'C'
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    if not rows:
        cur.close()
        return 0
    
    batch_size = 100
    count = 0
    
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        statements = []
        
        for row in batch:
            fid, billno, billname, amount, biztime, billstatus, contstatus = row
            
            node_id = f"CON_{fid}"
            contract_no = format_property(billno)
            contract_name = format_property(billname)
            amount_val = format_property(str(amount) if amount else None)
            sign_date = format_property(str(biztime) if biztime else None)
            status = format_property(contstatus or billstatus)
            description = format_property(f"{billname}的详细描述")
            
            statement = f'INSERT VERTEX Contract(contract_no, contract_name, amount, sign_date, status, description) VALUES "{node_id}":({contract_no}, {contract_name}, {amount_val}, {sign_date}, {status}, {description})'
            statements.append(statement)
        
        batch_query = "; ".join(statements)
        result = session.execute(batch_query)
        if result.is_succeeded():
            count += len(batch)
        else:
            print(f"Error inserting Contract batch: {result.error_msg()}")
    
    cur.close()
    return count


def import_legal_event_nodes(session: Session, pg_conn, dbname: str):
    """Import LegalEvent nodes from t_conl_case and t_conl_disputeregist"""
    ensure_space(session)
    cur = pg_conn.cursor()
    
    events = []
    
    # From t_conl_case
    query_case = """
    SELECT 
        fid,
        fbillno,
        fname,
        flawsuamount,
        fcasestatus,
        fregistdate,
        fbillstatus,
        fintroduction
    FROM t_conl_case
    WHERE fbillstatus = 'C'
    """
    cur.execute(query_case)
    for row in cur.fetchall():
        fid, billno, name, amount, status, regist_date, billstatus, introduction = row
        events.append({
            'node_id': f"CASE_{fid}",
            'event_type': 'Case',
            'event_no': billno,
            'event_name': name,
            'amount': amount,
            'status': status or billstatus,
            'register_date': regist_date,
            'description': introduction or f"{name}的详细描述"
        })
    
    # From t_conl_disputeregist
    query_dispute = """
    SELECT 
        fid,
        fbillno,
        fname,
        fdisputeamount,
        fdisputestatus,
        fregistdate,
        fbillstatus,
        fdisputeintroduction
    FROM t_conl_disputeregist
    WHERE fbillstatus = 'C'
    """
    cur.execute(query_dispute)
    for row in cur.fetchall():
        fid, billno, name, amount, status, regist_date, billstatus, introduction = row
        events.append({
            'node_id': f"DISP_{fid}",
            'event_type': 'Dispute',
            'event_no': billno,
            'event_name': name,
            'amount': amount,
            'status': status or billstatus,
            'register_date': regist_date,
            'description': introduction or f"{name}的详细描述"
        })
    
    if not events:
        cur.close()
        return 0
    
    batch_size = 100
    count = 0
    
    for i in range(0, len(events), batch_size):
        batch = events[i:i + batch_size]
        statements = []
        
        for event in batch:
            node_id = event['node_id']
            event_type = format_property(event['event_type'])
            event_no = format_property(event['event_no'])
            event_name = format_property(event['event_name'])
            amount = format_property(str(event['amount']) if event['amount'] else None)
            status = format_property(event['status'])
            register_date = format_property(str(event['register_date']) if event['register_date'] else None)
            description = format_property(event['description'])
            
            statement = f'INSERT VERTEX LegalEvent(event_type, event_no, event_name, amount, status, register_date, description) VALUES "{node_id}":({event_type}, {event_no}, {event_name}, {amount}, {status}, {register_date}, {description})'
            statements.append(statement)
        
        batch_query = "; ".join(statements)
        result = session.execute(batch_query)
        if result.is_succeeded():
            count += len(batch)
        else:
            print(f"Error inserting LegalEvent batch: {result.error_msg()}")
    
    cur.close()
    return count


def import_legal_person_edges(session: Session, pg_conn, dbname: str):
    """Import LEGAL_PERSON edges from company tables"""
    ensure_space(session)
    cur = pg_conn.cursor()
    
    # Build person name to node ID mapping
    person_name_map = {}
    query_person = "SELECT fid, ftruename FROM t_sec_user WHERE fstatus = 'C'"
    cur.execute(query_person)
    for row in cur.fetchall():
        fid, name = row
        if name:
            person_name_map[name] = f"USER_{fid}"
    
    edges = []
    
    queries = [
        ("ORG", "SELECT fid, fartificialperson, fname FROM t_org_org WHERE fstatus = 'C' AND fartificialperson IS NOT NULL AND fartificialperson != ''"),
        ("SUP", "SELECT fid, fartificialperson, fname FROM t_bd_supplier WHERE fstatus = 'C' AND fartificialperson IS NOT NULL AND fartificialperson != ''"),
        ("CUS", "SELECT fid, fartificialperson, fname FROM t_bd_customer WHERE fstatus = 'C' AND fartificialperson IS NOT NULL AND fartificialperson != ''"),
        ("CP", "SELECT fid, fartificialperson, fname FROM t_mscon_counterpart WHERE fstatus = 'C' AND fartificialperson IS NOT NULL AND fartificialperson != ''"),
    ]
    
    for prefix, query in queries:
        cur.execute(query)
        for row in cur.fetchall():
            fid, legal_person, name = row
            if legal_person and legal_person in person_name_map:
                edges.append({
                    'from_node': person_name_map[legal_person],
                    'to_node': f"{prefix}_{fid}",
                    'properties': f"法人代表关系-{name}"
                })
    
    if not edges:
        cur.close()
        return 0
    
    batch_size = 100
    count = 0
    
    for i in range(0, len(edges), batch_size):
        batch = edges[i:i + batch_size]
        statements = []
        
        for edge in batch:
            from_node = edge['from_node']
            to_node = edge['to_node']
            properties = format_property(edge['properties'])
            
            statement = f'INSERT EDGE LEGAL_PERSON(properties) VALUES "{from_node}" -> "{to_node}":({properties})'
            statements.append(statement)
        
        batch_query = "; ".join(statements)
        result = session.execute(batch_query)
        if result.is_succeeded():
            count += len(batch)
        else:
            print(f"Error inserting LEGAL_PERSON batch: {result.error_msg()}")
    
    cur.close()
    return count


def import_controls_edges(session: Session, pg_conn, dbname: str):
    """Import CONTROLS edges from t_org_org FPARENTORGID"""
    ensure_space(session)
    cur = pg_conn.cursor()
    
    query = """
    SELECT fid, fparentorgid
    FROM t_org_org
    WHERE fstatus = 'C' AND fparentorgid IS NOT NULL AND fparentorgid != ''
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    if not rows:
        cur.close()
        return 0
    
    edges = []
    for row in rows:
        fid, parent_org_id = row
        try:
            # FPARENTORGID is numeric
            parent_id = int(parent_org_id) if parent_org_id else None
            if parent_id:
                from_node = f"ORG_{parent_id}"
                to_node = f"ORG_{fid}"
                edges.append({
                    'from_node': from_node,
                    'to_node': to_node,
                    'properties': '控股关系'
                })
        except (ValueError, TypeError):
            continue
    
    if not edges:
        cur.close()
        return 0
    
    batch_size = 100
    count = 0
    
    for i in range(0, len(edges), batch_size):
        batch = edges[i:i + batch_size]
        statements = []
        
        for edge in batch:
            from_node = edge['from_node']
            to_node = edge['to_node']
            properties = format_property(edge['properties'])
            
            statement = f'INSERT EDGE CONTROLS(properties) VALUES "{from_node}" -> "{to_node}":({properties})'
            statements.append(statement)
        
        batch_query = "; ".join(statements)
        result = session.execute(batch_query)
        if result.is_succeeded():
            count += len(batch)
        else:
            print(f"Error inserting CONTROLS batch: {result.error_msg()}")
    
    cur.close()
    return count


def import_party_edges(session: Session, pg_conn, dbname: str):
    """Import PARTY_A/B/C/D edges from t_mscon_contract"""
    ensure_space(session)
    cur = pg_conn.cursor()
    
    query = """
    SELECT 
        fid,
        fpartaid, fpatype, fpartaname,
        fpartbid, fpbtype, fpartbname,
        fpartcid, fpctype, fpartcname,
        fpartdid, fpdtype, fpartdname
    FROM t_mscon_contract
    WHERE fbillstatus = 'C'
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    if not rows:
        cur.close()
        return 0
    
    edges = []
    prefix_map = {
        'bos_org': 'ORG',
        'bd_supplier': 'SUP',
        'bd_customer': 'CUS',
        'mscon_counterpart': 'CP'
    }
    
    for row in rows:
        fid, parta_id, parta_type, parta_name, partb_id, partb_type, partb_name, \
            partc_id, partc_type, partc_name, partd_id, partd_type, partd_name = row
        
        contract_id = f"CON_{fid}"
        
        # PARTY_A
        if parta_id is not None and parta_type:
            try:
                parta_id_int = int(parta_id)
                prefix = prefix_map.get(parta_type, 'ORG')
                company_id = f"{prefix}_{parta_id_int}"
                edges.append({
                    'edge_type': 'PARTY_A',
                    'from_node': company_id,
                    'to_node': contract_id,
                    'properties': f"甲方-{parta_name or ''}"
                })
            except (ValueError, TypeError):
                pass
        
        # PARTY_B
        if partb_id is not None and partb_type:
            try:
                partb_id_int = int(partb_id)
                prefix = prefix_map.get(partb_type, 'ORG')
                company_id = f"{prefix}_{partb_id_int}"
                edges.append({
                    'edge_type': 'PARTY_B',
                    'from_node': company_id,
                    'to_node': contract_id,
                    'properties': f"乙方-{partb_name or ''}"
                })
            except (ValueError, TypeError):
                pass
        
        # PARTY_C
        if partc_id is not None and partc_type:
            try:
                partc_id_int = int(partc_id)
                prefix = prefix_map.get(partc_type, 'ORG')
                company_id = f"{prefix}_{partc_id_int}"
                edges.append({
                    'edge_type': 'PARTY_C',
                    'from_node': company_id,
                    'to_node': contract_id,
                    'properties': f"丙方-{partc_name or ''}"
                })
            except (ValueError, TypeError):
                pass
        
        # PARTY_D
        if partd_id is not None and partd_type:
            try:
                partd_id_int = int(partd_id)
                prefix = prefix_map.get(partd_type, 'ORG')
                company_id = f"{prefix}_{partd_id_int}"
                edges.append({
                    'edge_type': 'PARTY_D',
                    'from_node': company_id,
                    'to_node': contract_id,
                    'properties': f"丁方-{partd_name or ''}"
                })
            except (ValueError, TypeError):
                pass
    
    if not edges:
        cur.close()
        return 0
    
    batch_size = 100
    count = 0
    
    for i in range(0, len(edges), batch_size):
        batch = edges[i:i + batch_size]
        statements = []
        
        for edge in batch:
            edge_type = edge['edge_type']
            from_node = edge['from_node']
            to_node = edge['to_node']
            properties = format_property(edge['properties'])
            
            statement = f'INSERT EDGE {edge_type}(properties) VALUES "{from_node}" -> "{to_node}":({properties})'
            statements.append(statement)
        
        batch_query = "; ".join(statements)
        result = session.execute(batch_query)
        if result.is_succeeded():
            count += len(batch)
        else:
            print(f"Error inserting PARTY batch: {result.error_msg()}")
    
    cur.close()
    return count


def import_trades_with_edges(session: Session, pg_conn, dbname: str):
    """Import TRADES_WITH edges derived from contracts (PARTY_A -> PARTY_B)"""
    ensure_space(session)
    cur = pg_conn.cursor()
    
    query = """
    SELECT 
        fid,
        fpartaid, fpatype, fpartaname,
        fpartbid, fpbtype, fpartbname,
        fsignallamount,
        fbillno
    FROM t_mscon_contract
    WHERE fbillstatus = 'C' AND fpartaid IS NOT NULL AND fpartbid IS NOT NULL
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    if not rows:
        cur.close()
        return 0
    
    edges = []
    prefix_map = {
        'bos_org': 'ORG',
        'bd_supplier': 'SUP',
        'bd_customer': 'CUS',
        'mscon_counterpart': 'CP'
    }
    
    for row in rows:
        fid, parta_id, parta_type, parta_name, partb_id, partb_type, partb_name, amount, billno = row
        
        try:
            parta_id_int = int(parta_id) if parta_id is not None else None
            partb_id_int = int(partb_id) if partb_id is not None else None
            
            if parta_id_int is None or partb_id_int is None:
                continue
            
            prefix_a = prefix_map.get(parta_type, 'ORG')
            prefix_b = prefix_map.get(partb_type, 'ORG')
            
            from_node = f"{prefix_a}_{parta_id_int}"
            to_node = f"{prefix_b}_{partb_id_int}"
            
            amount_str = str(amount) if amount else ""
            properties = f"交易金额:{amount_str},合同:{billno or ''}"
            
            edges.append({
                'from_node': from_node,
                'to_node': to_node,
                'properties': properties
            })
        except (ValueError, TypeError):
            continue
    
    if not edges:
        cur.close()
        return 0
    
    batch_size = 100
    count = 0
    
    for i in range(0, len(edges), batch_size):
        batch = edges[i:i + batch_size]
        statements = []
        
        for edge in batch:
            from_node = edge['from_node']
            to_node = edge['to_node']
            properties = format_property(edge['properties'])
            
            statement = f'INSERT EDGE TRADES_WITH(properties) VALUES "{from_node}" -> "{to_node}":({properties})'
            statements.append(statement)
        
        batch_query = "; ".join(statements)
        result = session.execute(batch_query)
        if result.is_succeeded():
            count += len(batch)
        else:
            print(f"Error inserting TRADES_WITH batch: {result.error_msg()}")
    
    cur.close()
    return count


def import_involved_in_edges(session: Session, pg_conn, dbname: str):
    """Import INVOLVED_IN edges from t_conl_case FOPERATORID"""
    ensure_space(session)
    cur = pg_conn.cursor()
    
    query = """
    SELECT fid, foperatorid, foperatorname
    FROM t_conl_case
    WHERE fbillstatus = 'C' AND foperatorid IS NOT NULL
    """
    
    cur.execute(query)
    rows = cur.fetchall()
    
    if not rows:
        cur.close()
        return 0
    
    edges = []
    for row in rows:
        fid, operator_id, operator_name = row
        try:
            operator_id_int = int(operator_id) if operator_id is not None else None
            if operator_id_int is None:
                continue
            
            person_id = f"USER_{operator_id_int}"
            event_id = f"CASE_{fid}"
            
            edges.append({
                'from_node': person_id,
                'to_node': event_id,
                'properties': f"经办人-{operator_name or ''}"
            })
        except (ValueError, TypeError):
            continue
    
    if not edges:
        cur.close()
        return 0
    
    batch_size = 100
    count = 0
    
    for i in range(0, len(edges), batch_size):
        batch = edges[i:i + batch_size]
        statements = []
        
        for edge in batch:
            from_node = edge['from_node']
            to_node = edge['to_node']
            properties = format_property(edge['properties'])
            
            statement = f'INSERT EDGE INVOLVED_IN(properties) VALUES "{from_node}" -> "{to_node}":({properties})'
            statements.append(statement)
        
        batch_query = "; ".join(statements)
        result = session.execute(batch_query)
        if result.is_succeeded():
            count += len(batch)
        else:
            print(f"Error inserting INVOLVED_IN batch: {result.error_msg()}")
    
    cur.close()
    return count


def import_related_to_edges(session: Session, pg_conn, dbname: str):
    """Import RELATED_TO edges from t_conl_case and t_conl_disputeregist FRELATECONTRACTID"""
    ensure_space(session)
    cur = pg_conn.cursor()
    
    edges = []
    
    # From t_conl_case
    query_case = """
    SELECT fid, frelatecontractid, frelatecontractname
    FROM t_conl_case
    WHERE fbillstatus = 'C' AND frelatecontractid IS NOT NULL
    """
    cur.execute(query_case)
    for row in cur.fetchall():
        fid, contract_id, contract_name = row
        try:
            contract_id_int = int(contract_id) if contract_id is not None else None
            if contract_id_int is None:
                continue
            
            event_id = f"CASE_{fid}"
            contract_node_id = f"CON_{contract_id_int}"
            
            edges.append({
                'from_node': contract_node_id,
                'to_node': event_id,
                'properties': f"关联合同-{contract_name or ''}"
            })
        except (ValueError, TypeError):
            continue
    
    # From t_conl_disputeregist
    query_dispute = """
    SELECT fid, frelatecontractid, frelatecontractname
    FROM t_conl_disputeregist
    WHERE fbillstatus = 'C' AND frelatecontractid IS NOT NULL
    """
    cur.execute(query_dispute)
    for row in cur.fetchall():
        fid, contract_id, contract_name = row
        try:
            contract_id_int = int(contract_id) if contract_id is not None else None
            if contract_id_int is None:
                continue
            
            event_id = f"DISP_{fid}"
            contract_node_id = f"CON_{contract_id_int}"
            
            edges.append({
                'from_node': contract_node_id,
                'to_node': event_id,
                'properties': f"关联合同-{contract_name or ''}"
            })
        except (ValueError, TypeError):
            continue
    
    if not edges:
        cur.close()
        return 0
    
    batch_size = 100
    count = 0
    
    for i in range(0, len(edges), batch_size):
        batch = edges[i:i + batch_size]
        statements = []
        
        for edge in batch:
            from_node = edge['from_node']
            to_node = edge['to_node']
            properties = format_property(edge['properties'])
            
            statement = f'INSERT EDGE RELATED_TO(properties) VALUES "{from_node}" -> "{to_node}":({properties})'
            statements.append(statement)
        
        batch_query = "; ".join(statements)
        result = session.execute(batch_query)
        if result.is_succeeded():
            count += len(batch)
        else:
            print(f"Error inserting RELATED_TO batch: {result.error_msg()}")
    
    cur.close()
    return count


def main():
    """Main function to import all data to Nebula Graph"""
    dbname = "postgres"
    
    print("Connecting to PostgreSQL...")
    pg_conn = get_pg_connection(dbname)
    
    print("Connecting to Nebula Graph...")
    session = get_nebula_session()
    
    print("\nStarting data import...")
    
    results = {}
    
    # Import nodes
    print("\n[1/4] Importing nodes...")
    with tqdm(total=4, desc="Nodes") as pbar:
        count = import_person_nodes(session, pg_conn, dbname)
        results['Person'] = count
        pbar.set_postfix_str(f"Person: {count}")
        pbar.update(1)
        
        count = import_company_nodes(session, pg_conn, dbname)
        results['Company'] = count
        pbar.set_postfix_str(f"Company: {count}")
        pbar.update(1)
        
        count = import_contract_nodes(session, pg_conn, dbname)
        results['Contract'] = count
        pbar.set_postfix_str(f"Contract: {count}")
        pbar.update(1)
        
        count = import_legal_event_nodes(session, pg_conn, dbname)
        results['LegalEvent'] = count
        pbar.set_postfix_str(f"LegalEvent: {count}")
        pbar.update(1)
    
    # Import edges
    print("\n[2/4] Importing edges...")
    with tqdm(total=6, desc="Edges") as pbar:
        count = import_legal_person_edges(session, pg_conn, dbname)
        results['LEGAL_PERSON'] = count
        pbar.set_postfix_str(f"LEGAL_PERSON: {count}")
        pbar.update(1)
        
        count = import_controls_edges(session, pg_conn, dbname)
        results['CONTROLS'] = count
        pbar.set_postfix_str(f"CONTROLS: {count}")
        pbar.update(1)
        
        count = import_party_edges(session, pg_conn, dbname)
        results['PARTY'] = count
        pbar.set_postfix_str(f"PARTY: {count}")
        pbar.update(1)
        
        count = import_trades_with_edges(session, pg_conn, dbname)
        results['TRADES_WITH'] = count
        pbar.set_postfix_str(f"TRADES_WITH: {count}")
        pbar.update(1)
        
        count = import_involved_in_edges(session, pg_conn, dbname)
        results['INVOLVED_IN'] = count
        pbar.set_postfix_str(f"INVOLVED_IN: {count}")
        pbar.update(1)
        
        count = import_related_to_edges(session, pg_conn, dbname)
        results['RELATED_TO'] = count
        pbar.set_postfix_str(f"RELATED_TO: {count}")
        pbar.update(1)
    
    # Print summary
    print("\n" + "="*50)
    print("Import Summary")
    print("="*50)
    print("\nNodes:")
    for node_type, count in results.items():
        if node_type in ['Person', 'Company', 'Contract', 'LegalEvent']:
            print(f"  {node_type}: {count}")
    
    print("\nEdges:")
    for edge_type, count in results.items():
        if edge_type not in ['Person', 'Company', 'Contract', 'LegalEvent']:
            print(f"  {edge_type}: {count}")
    
    total_nodes = sum(count for k, count in results.items() if k in ['Person', 'Company', 'Contract', 'LegalEvent'])
    total_edges = sum(count for k, count in results.items() if k not in ['Person', 'Company', 'Contract', 'LegalEvent'])
    
    print(f"\nTotal: {total_nodes} nodes, {total_edges} edges")
    print("="*50)
    
    pg_conn.close()
    session.release()


if __name__ == "__main__":
    main()

