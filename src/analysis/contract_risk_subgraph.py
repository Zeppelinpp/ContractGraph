"""
合同风险子图分析

以合同为入口，递归获取关联法律事件及其传导路径的子图空间
"""

import os
import json
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, asdict
from src.utils.nebula_utils import get_nebula_session, execute_query

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")


@dataclass
class SubGraphNode:
    """子图节点"""
    id: str
    type: str  # Contract, LegalEvent, Company, Person
    label: str
    properties: Dict = None
    
    def to_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "label": self.label,
            "properties": self.properties or {}
        }


@dataclass
class SubGraphEdge:
    """子图边"""
    source: str
    target: str
    type: str
    properties: Dict = None
    
    def to_dict(self):
        return {
            "source": self.source,
            "target": self.target,
            "type": self.type,
            "properties": self.properties or {}
        }


@dataclass
class ContractRiskSubGraph:
    """合同风险子图"""
    root_contract_id: str
    nodes: List[SubGraphNode]
    edges: List[SubGraphEdge]
    depth: int
    
    def to_dict(self):
        return {
            "root_contract_id": self.root_contract_id,
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "depth": self.depth
        }


def get_contract_risk_subgraph(
    contract_id: str,
    max_depth: int = 3,
    session=None
) -> ContractRiskSubGraph:
    """
    以合同ID为入参，递归获取法律事件关联的子图空间
    
    递归逻辑：
    1. 从合同出发，找到关联的法律事件
    2. 找到合同的相对方（甲方、乙方）
    3. 对于每个相对方，找到其涉及的其他合同
    4. 对于这些合同，检查是否关联法律事件
    5. 递归直到达到最大深度
    
    Args:
        contract_id: 合同ID（Nebula Graph 中的节点ID）
        max_depth: 递归深度，默认3
        session: Nebula Graph session，如果为None则创建新的
    
    Returns:
        ContractRiskSubGraph: 包含节点、边的子图数据
    """
    should_release_session = session is None
    if session is None:
        session = get_nebula_session()
    
    try:
        nodes_map: Dict[str, SubGraphNode] = {}
        edges_set: Set[Tuple[str, str, str]] = set()
        edges_list: List[SubGraphEdge] = []
        
        visited_contracts: Set[str] = set()
        
        def add_node(node_id: str, node_type: str, label: str, properties: Dict = None):
            """添加节点到子图"""
            if node_id not in nodes_map:
                nodes_map[node_id] = SubGraphNode(
                    id=node_id,
                    type=node_type,
                    label=label,
                    properties=properties or {}
                )
        
        def add_edge(source: str, target: str, edge_type: str, properties: Dict = None):
            """添加边到子图"""
            edge_key = (source, target, edge_type)
            if edge_key not in edges_set:
                edges_set.add(edge_key)
                edges_list.append(SubGraphEdge(
                    source=source,
                    target=target,
                    type=edge_type,
                    properties=properties or {}
                ))
        
        def explore_contract(con_id: str, current_depth: int):
            """递归探索合同及其关联实体"""
            if current_depth > max_depth or con_id in visited_contracts:
                return
            
            visited_contracts.add(con_id)
            
            # Step 1: 获取合同基本信息
            contract_query = f"""
            MATCH (con:Contract)
            WHERE id(con) == "{con_id}"
            RETURN id(con) as contract_id,
                   con.Contract.contract_no as contract_no,
                   con.Contract.contract_name as contract_name,
                   con.Contract.amount as amount,
                   con.Contract.sign_date as sign_date,
                   con.Contract.status as status
            """
            contract_rows = execute_query(session, contract_query)
            if not contract_rows:
                return
            
            con_info = contract_rows[0]
            add_node(
                con_id,
                "Contract",
                con_info.get("contract_name", con_id) or con_id,
                {
                    "contract_no": con_info.get("contract_no", ""),
                    "amount": con_info.get("amount", 0),
                    "sign_date": con_info.get("sign_date", ""),
                    "status": con_info.get("status", "")
                }
            )
            
            # Step 2: 获取合同关联的法律事件
            legal_event_query = f"""
            MATCH (con:Contract)-[:RELATED_TO]->(le:LegalEvent)
            WHERE id(con) == "{con_id}"
            RETURN id(le) as event_id,
                   le.LegalEvent.event_type as event_type,
                   le.LegalEvent.event_no as event_no,
                   le.LegalEvent.event_name as event_name,
                   le.LegalEvent.amount as amount,
                   le.LegalEvent.status as status,
                   le.LegalEvent.register_date as register_date
            """
            legal_events = execute_query(session, legal_event_query)
            
            for event in legal_events:
                event_id = event.get("event_id", "")
                if event_id:
                    add_node(
                        event_id,
                        "LegalEvent",
                        event.get("event_name", event_id) or event_id,
                        {
                            "event_type": event.get("event_type", ""),
                            "event_no": event.get("event_no", ""),
                            "amount": event.get("amount", 0),
                            "status": event.get("status", ""),
                            "register_date": event.get("register_date", "")
                        }
                    )
                    add_edge(con_id, event_id, "RELATED_TO")
                    
                    # 获取涉及该法律事件的人员
                    person_query = f"""
                    MATCH (p:Person)-[:INVOLVED_IN]->(le:LegalEvent)
                    WHERE id(le) == "{event_id}"
                    RETURN id(p) as person_id,
                           p.Person.name as name,
                           p.Person.number as number
                    """
                    persons = execute_query(session, person_query)
                    for person in persons:
                        person_id = person.get("person_id", "")
                        if person_id:
                            add_node(
                                person_id,
                                "Person",
                                person.get("name", person_id) or person_id,
                                {"number": person.get("number", "")}
                            )
                            add_edge(person_id, event_id, "INVOLVED_IN")
            
            # Step 3: 获取合同的甲方和乙方
            party_query = f"""
            MATCH (c:Company)-[e:PARTY_A|PARTY_B]->(con:Contract)
            WHERE id(con) == "{con_id}"
            RETURN id(c) as company_id,
                   c.Company.name as name,
                   c.Company.number as number,
                   c.Company.credit_code as credit_code,
                   type(e) as party_type
            """
            parties = execute_query(session, party_query)
            
            counterparty_ids = []
            for party in parties:
                company_id = party.get("company_id", "")
                if company_id:
                    add_node(
                        company_id,
                        "Company",
                        party.get("name", company_id) or company_id,
                        {
                            "number": party.get("number", ""),
                            "credit_code": party.get("credit_code", "")
                        }
                    )
                    party_type = party.get("party_type", "PARTY")
                    add_edge(company_id, con_id, party_type)
                    counterparty_ids.append(company_id)
            
            # Step 4: 如果未达到最大深度，继续探索相对方的其他合同
            if current_depth < max_depth:
                for company_id in counterparty_ids:
                    # 获取该公司的法人代表
                    legal_person_query = f"""
                    MATCH (p:Person)-[:LEGAL_PERSON]->(c:Company)
                    WHERE id(c) == "{company_id}"
                    RETURN id(p) as person_id,
                           p.Person.name as name,
                           p.Person.number as number
                    """
                    legal_persons = execute_query(session, legal_person_query)
                    for lp in legal_persons:
                        lp_id = lp.get("person_id", "")
                        if lp_id:
                            add_node(
                                lp_id,
                                "Person",
                                lp.get("name", lp_id) or lp_id,
                                {"number": lp.get("number", "")}
                            )
                            add_edge(lp_id, company_id, "LEGAL_PERSON")
                    
                    # 获取该公司涉及的其他合同（有法律事件关联的）
                    other_contracts_query = f"""
                    MATCH (c:Company)-[:PARTY_A|PARTY_B]->(con:Contract)-[:RELATED_TO]->(le:LegalEvent)
                    WHERE id(c) == "{company_id}" AND id(con) != "{con_id}"
                    RETURN DISTINCT id(con) as contract_id
                    """
                    other_contracts = execute_query(session, other_contracts_query)
                    
                    for other_con in other_contracts:
                        other_con_id = other_con.get("contract_id", "")
                        if other_con_id and other_con_id not in visited_contracts:
                            explore_contract(other_con_id, current_depth + 1)
        
        # 开始递归探索
        explore_contract(contract_id, 1)
        
        return ContractRiskSubGraph(
            root_contract_id=contract_id,
            nodes=list(nodes_map.values()),
            edges=edges_list,
            depth=max_depth
        )
    
    finally:
        if should_release_session and session:
            session.release()


def generate_subgraph_html(
    subgraph: ContractRiskSubGraph,
    output_filename: str = None
) -> str:
    """
    生成子图的交互式HTML页面
    
    Args:
        subgraph: ContractRiskSubGraph 子图数据
        output_filename: 输出文件名，默认为 contract_risk_subgraph_{contract_id}.html
    
    Returns:
        str: 生成的HTML文件路径
    """
    if output_filename is None:
        safe_id = subgraph.root_contract_id.replace('"', '').replace("'", "")
        output_filename = f"contract_risk_subgraph_{safe_id}.html"
    
    os.makedirs(REPORTS_DIR, exist_ok=True)
    output_path = os.path.join(REPORTS_DIR, output_filename)
    
    # 准备节点和边数据
    nodes_json = json.dumps([n.to_dict() for n in subgraph.nodes], ensure_ascii=False)
    edges_json = json.dumps([e.to_dict() for e in subgraph.edges], ensure_ascii=False)
    
    html_content = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>合同风险子图 - {subgraph.root_contract_id}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 
                         'Hiragino Sans GB', 'Microsoft YaHei', sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            min-height: 100vh;
            color: #e8e8e8;
        }}
        
        .container {{
            max-width: 1600px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        header {{
            text-align: center;
            padding: 30px 0;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
            margin-bottom: 20px;
        }}
        
        header h1 {{
            font-size: 2.2em;
            font-weight: 600;
            background: linear-gradient(135deg, #00d9ff 0%, #00ff88 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 10px;
        }}
        
        header p {{
            color: #8892b0;
            font-size: 1.1em;
        }}
        
        .stats-bar {{
            display: flex;
            justify-content: center;
            gap: 40px;
            margin: 20px 0;
        }}
        
        .stat-item {{
            text-align: center;
        }}
        
        .stat-value {{
            font-size: 2em;
            font-weight: 700;
            color: #00d9ff;
        }}
        
        .stat-label {{
            font-size: 0.9em;
            color: #8892b0;
            margin-top: 5px;
        }}
        
        .main-content {{
            display: grid;
            grid-template-columns: 300px 1fr;
            gap: 20px;
        }}
        
        .sidebar {{
            background: rgba(255, 255, 255, 0.03);
            border-radius: 16px;
            padding: 20px;
            border: 1px solid rgba(255, 255, 255, 0.08);
            backdrop-filter: blur(10px);
        }}
        
        .sidebar h3 {{
            font-size: 1.1em;
            color: #00d9ff;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        }}
        
        .legend {{
            margin-bottom: 25px;
        }}
        
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 10px;
            font-size: 0.9em;
        }}
        
        .legend-dot {{
            width: 16px;
            height: 16px;
            border-radius: 50%;
            flex-shrink: 0;
        }}
        
        .node-list {{
            max-height: 400px;
            overflow-y: auto;
        }}
        
        .node-item {{
            padding: 10px 12px;
            margin-bottom: 8px;
            background: rgba(255, 255, 255, 0.03);
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s;
            border-left: 3px solid transparent;
        }}
        
        .node-item:hover {{
            background: rgba(255, 255, 255, 0.08);
            transform: translateX(3px);
        }}
        
        .node-item.active {{
            background: rgba(0, 217, 255, 0.1);
            border-left-color: #00d9ff;
        }}
        
        .node-item-type {{
            font-size: 0.75em;
            color: #8892b0;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .node-item-label {{
            font-size: 0.95em;
            color: #e8e8e8;
            margin-top: 3px;
            word-break: break-word;
        }}
        
        .graph-panel {{
            background: rgba(255, 255, 255, 0.03);
            border-radius: 16px;
            border: 1px solid rgba(255, 255, 255, 0.08);
            overflow: hidden;
        }}
        
        .graph-toolbar {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 15px 20px;
            background: rgba(0, 0, 0, 0.2);
            border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        }}
        
        .graph-toolbar h3 {{
            color: #e8e8e8;
            font-size: 1em;
        }}
        
        .toolbar-buttons {{
            display: flex;
            gap: 10px;
        }}
        
        .btn {{
            padding: 8px 16px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 0.9em;
            transition: all 0.2s;
            background: rgba(255, 255, 255, 0.1);
            color: #e8e8e8;
        }}
        
        .btn:hover {{
            background: rgba(255, 255, 255, 0.2);
        }}
        
        .btn-primary {{
            background: linear-gradient(135deg, #00d9ff 0%, #00ff88 100%);
            color: #1a1a2e;
            font-weight: 600;
        }}
        
        .btn-primary:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 20px rgba(0, 217, 255, 0.3);
        }}
        
        #graph-svg {{
            width: 100%;
            height: 700px;
            background: radial-gradient(circle at center, rgba(0, 217, 255, 0.03) 0%, transparent 70%);
        }}
        
        .node circle {{
            stroke-width: 3px;
            filter: drop-shadow(0 2px 8px rgba(0, 0, 0, 0.3));
        }}
        
        .node text {{
            font-size: 11px;
            fill: #e8e8e8;
            pointer-events: none;
            text-shadow: 0 1px 3px rgba(0, 0, 0, 0.8);
        }}
        
        .link {{
            stroke-opacity: 0.6;
        }}
        
        .link-label {{
            font-size: 9px;
            fill: #8892b0;
            text-shadow: 0 1px 2px rgba(0, 0, 0, 0.8);
        }}
        
        .tooltip {{
            position: absolute;
            background: rgba(26, 26, 46, 0.95);
            border: 1px solid rgba(0, 217, 255, 0.3);
            border-radius: 12px;
            padding: 15px;
            font-size: 0.9em;
            pointer-events: none;
            z-index: 1000;
            max-width: 350px;
            backdrop-filter: blur(10px);
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
        }}
        
        .tooltip h4 {{
            color: #00d9ff;
            margin-bottom: 10px;
            font-size: 1.1em;
        }}
        
        .tooltip-row {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 6px;
        }}
        
        .tooltip-key {{
            color: #8892b0;
        }}
        
        .tooltip-value {{
            color: #e8e8e8;
            text-align: right;
            max-width: 200px;
            word-break: break-word;
        }}
        
        .detail-panel {{
            position: fixed;
            right: 20px;
            top: 100px;
            width: 350px;
            background: rgba(26, 26, 46, 0.95);
            border: 1px solid rgba(0, 217, 255, 0.2);
            border-radius: 16px;
            padding: 20px;
            display: none;
            z-index: 100;
            backdrop-filter: blur(10px);
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.5);
        }}
        
        .detail-panel.show {{
            display: block;
        }}
        
        .detail-panel h4 {{
            color: #00d9ff;
            margin-bottom: 15px;
            font-size: 1.1em;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .detail-panel .close-btn {{
            cursor: pointer;
            color: #8892b0;
            font-size: 1.5em;
            line-height: 1;
        }}
        
        .detail-panel .close-btn:hover {{
            color: #e8e8e8;
        }}
        
        .detail-content {{
            max-height: 400px;
            overflow-y: auto;
        }}
        
        .detail-row {{
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }}
        
        .detail-row:last-child {{
            border-bottom: none;
        }}
        
        ::-webkit-scrollbar {{
            width: 6px;
        }}
        
        ::-webkit-scrollbar-track {{
            background: rgba(255, 255, 255, 0.05);
            border-radius: 3px;
        }}
        
        ::-webkit-scrollbar-thumb {{
            background: rgba(0, 217, 255, 0.3);
            border-radius: 3px;
        }}
        
        ::-webkit-scrollbar-thumb:hover {{
            background: rgba(0, 217, 255, 0.5);
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔗 合同风险传导子图</h1>
            <p>以合同为起点的法律事件风险传导路径分析</p>
        </header>
        
        <div class="stats-bar">
            <div class="stat-item">
                <div class="stat-value" id="node-count">0</div>
                <div class="stat-label">节点数量</div>
            </div>
            <div class="stat-item">
                <div class="stat-value" id="edge-count">0</div>
                <div class="stat-label">关系数量</div>
            </div>
            <div class="stat-item">
                <div class="stat-value" id="depth-value">{subgraph.depth}</div>
                <div class="stat-label">探索深度</div>
            </div>
        </div>
        
        <div class="main-content">
            <div class="sidebar">
                <div class="legend">
                    <h3>图例说明</h3>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #00ff88;"></div>
                        <span>合同 (Contract)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #ff6b6b;"></div>
                        <span>法律事件 (LegalEvent)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #a855f7;"></div>
                        <span>公司 (Company)</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-dot" style="background: #00d9ff;"></div>
                        <span>人员 (Person)</span>
                    </div>
                </div>
                
                <h3>节点列表</h3>
                <div class="node-list" id="node-list"></div>
            </div>
            
            <div class="graph-panel">
                <div class="graph-toolbar">
                    <h3>关系图谱可视化</h3>
                    <div class="toolbar-buttons">
                        <button class="btn" onclick="zoomIn()">🔍 放大</button>
                        <button class="btn" onclick="zoomOut()">🔍 缩小</button>
                        <button class="btn" onclick="resetView()">↺ 重置</button>
                        <button class="btn btn-primary" onclick="exportData()">📥 导出数据</button>
                    </div>
                </div>
                <svg id="graph-svg"></svg>
            </div>
        </div>
    </div>
    
    <div class="detail-panel" id="detail-panel">
        <h4>
            <span id="detail-title">节点详情</span>
            <span class="close-btn" onclick="closeDetailPanel()">×</span>
        </h4>
        <div class="detail-content" id="detail-content"></div>
    </div>
    
    <div class="tooltip" id="tooltip" style="display: none;"></div>

    <script>
        // 图数据
        const graphData = {{
            nodes: {nodes_json},
            edges: {edges_json},
            rootContractId: "{subgraph.root_contract_id}"
        }};
        
        // 更新统计
        document.getElementById('node-count').textContent = graphData.nodes.length;
        document.getElementById('edge-count').textContent = graphData.edges.length;
        
        // 颜色映射
        const colorMap = {{
            'Contract': '#00ff88',
            'LegalEvent': '#ff6b6b',
            'Company': '#a855f7',
            'Person': '#00d9ff'
        }};
        
        // 渲染节点列表
        function renderNodeList() {{
            const listEl = document.getElementById('node-list');
            const grouped = {{}};
            
            graphData.nodes.forEach(node => {{
                if (!grouped[node.type]) grouped[node.type] = [];
                grouped[node.type].push(node);
            }});
            
            let html = '';
            for (const [type, nodes] of Object.entries(grouped)) {{
                nodes.forEach(node => {{
                    html += `
                        <div class="node-item" data-id="${{node.id}}" onclick="focusNode('${{node.id}}')">
                            <div class="node-item-type" style="color: ${{colorMap[type]}}">${{type}}</div>
                            <div class="node-item-label">${{node.label}}</div>
                        </div>
                    `;
                }});
            }}
            
            listEl.innerHTML = html;
        }}
        
        renderNodeList();
        
        // D3 图谱
        const svg = d3.select('#graph-svg');
        const width = svg.node().getBoundingClientRect().width;
        const height = 700;
        
        svg.attr('viewBox', [0, 0, width, height]);
        
        const g = svg.append('g');
        
        // 缩放
        const zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', (event) => {{
                g.attr('transform', event.transform);
            }});
        
        svg.call(zoom);
        
        // 准备数据
        const nodes = graphData.nodes.map(n => ({{...n}}));
        const links = graphData.edges.map(e => ({{
            source: e.source,
            target: e.target,
            type: e.type,
            properties: e.properties
        }}));
        
        // 力导向
        const simulation = d3.forceSimulation(nodes)
            .force('link', d3.forceLink(links).id(d => d.id).distance(120))
            .force('charge', d3.forceManyBody().strength(-400))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(40));
        
        // 箭头
        svg.append('defs').selectAll('marker')
            .data(['arrow'])
            .join('marker')
            .attr('id', 'arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 28)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('fill', '#8892b0')
            .attr('d', 'M0,-5L10,0L0,5');
        
        // 边
        const link = g.append('g')
            .selectAll('line')
            .data(links)
            .join('line')
            .attr('class', 'link')
            .attr('stroke', '#4a5568')
            .attr('stroke-width', 2)
            .attr('marker-end', 'url(#arrow)');
        
        // 边标签
        const linkLabel = g.append('g')
            .selectAll('text')
            .data(links)
            .join('text')
            .attr('class', 'link-label')
            .text(d => d.type);
        
        // 节点
        const node = g.append('g')
            .selectAll('g')
            .data(nodes)
            .join('g')
            .attr('class', 'node')
            .call(d3.drag()
                .on('start', dragstarted)
                .on('drag', dragged)
                .on('end', dragended));
        
        node.append('circle')
            .attr('r', d => d.id === graphData.rootContractId ? 25 : 18)
            .attr('fill', d => colorMap[d.type] || '#999')
            .attr('stroke', d => d.id === graphData.rootContractId ? '#fff' : 'rgba(255,255,255,0.3)')
            .attr('stroke-width', d => d.id === graphData.rootContractId ? 4 : 2);
        
        node.append('text')
            .attr('dy', 35)
            .attr('text-anchor', 'middle')
            .text(d => d.label.length > 12 ? d.label.substring(0, 12) + '...' : d.label);
        
        // 悬停提示
        const tooltip = d3.select('#tooltip');
        
        node.on('mouseover', (event, d) => {{
            let html = `<h4>${{d.label}}</h4>`;
            html += `<div class="tooltip-row"><span class="tooltip-key">类型</span><span class="tooltip-value">${{d.type}}</span></div>`;
            html += `<div class="tooltip-row"><span class="tooltip-key">ID</span><span class="tooltip-value">${{d.id}}</span></div>`;
            
            if (d.properties) {{
                for (const [key, value] of Object.entries(d.properties)) {{
                    if (value) {{
                        html += `<div class="tooltip-row"><span class="tooltip-key">${{key}}</span><span class="tooltip-value">${{value}}</span></div>`;
                    }}
                }}
            }}
            
            tooltip.html(html)
                .style('display', 'block')
                .style('left', (event.pageX + 15) + 'px')
                .style('top', (event.pageY - 10) + 'px');
        }})
        .on('mouseout', () => {{
            tooltip.style('display', 'none');
        }})
        .on('click', (event, d) => {{
            showDetailPanel(d);
        }});
        
        // 更新位置
        simulation.on('tick', () => {{
            link
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);
            
            linkLabel
                .attr('x', d => (d.source.x + d.target.x) / 2)
                .attr('y', d => (d.source.y + d.target.y) / 2);
            
            node.attr('transform', d => `translate(${{d.x}},${{d.y}})`);
        }});
        
        function dragstarted(event, d) {{
            if (!event.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
        }}
        
        function dragged(event, d) {{
            d.fx = event.x;
            d.fy = event.y;
        }}
        
        function dragended(event, d) {{
            if (!event.active) simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
        }}
        
        // 缩放控制
        function zoomIn() {{
            svg.transition().call(zoom.scaleBy, 1.3);
        }}
        
        function zoomOut() {{
            svg.transition().call(zoom.scaleBy, 0.7);
        }}
        
        function resetView() {{
            svg.transition().call(zoom.transform, d3.zoomIdentity);
        }}
        
        // 聚焦节点
        function focusNode(nodeId) {{
            const targetNode = nodes.find(n => n.id === nodeId);
            if (targetNode) {{
                const transform = d3.zoomIdentity
                    .translate(width / 2 - targetNode.x, height / 2 - targetNode.y);
                svg.transition().duration(500).call(zoom.transform, transform);
                
                document.querySelectorAll('.node-item').forEach(el => el.classList.remove('active'));
                document.querySelector(`.node-item[data-id="${{nodeId}}"]`)?.classList.add('active');
                
                showDetailPanel(targetNode);
            }}
        }}
        
        // 详情面板
        function showDetailPanel(node) {{
            const panel = document.getElementById('detail-panel');
            const title = document.getElementById('detail-title');
            const content = document.getElementById('detail-content');
            
            title.textContent = node.label;
            
            let html = `
                <div class="detail-row">
                    <span class="tooltip-key">类型</span>
                    <span class="tooltip-value">${{node.type}}</span>
                </div>
                <div class="detail-row">
                    <span class="tooltip-key">ID</span>
                    <span class="tooltip-value">${{node.id}}</span>
                </div>
            `;
            
            if (node.properties) {{
                for (const [key, value] of Object.entries(node.properties)) {{
                    if (value) {{
                        html += `
                            <div class="detail-row">
                                <span class="tooltip-key">${{key}}</span>
                                <span class="tooltip-value">${{value}}</span>
                            </div>
                        `;
                    }}
                }}
            }}
            
            content.innerHTML = html;
            panel.classList.add('show');
        }}
        
        function closeDetailPanel() {{
            document.getElementById('detail-panel').classList.remove('show');
        }}
        
        // 导出数据
        function exportData() {{
            const data = JSON.stringify(graphData, null, 2);
            const blob = new Blob([data], {{ type: 'application/json' }});
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'contract_risk_subgraph.json';
            a.click();
            URL.revokeObjectURL(url);
        }}
    </script>
</body>
</html>
'''
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    return output_path


def get_contract_risk_subgraph_with_html(
    contract_id: str,
    max_depth: int = 3,
    session=None
) -> Dict:
    """
    获取合同风险子图并生成交互式HTML页面
    
    Args:
        contract_id: 合同ID
        max_depth: 递归深度
        session: Nebula Graph session
    
    Returns:
        dict: {
            "html_url": str,  # HTML文件路径
            "subgraph": dict  # 子图数据（节点、边）
        }
    """
    # 获取子图
    subgraph = get_contract_risk_subgraph(contract_id, max_depth, session)
    
    # 生成HTML
    html_path = generate_subgraph_html(subgraph)
    
    return {
        "html_url": html_path,
        "subgraph": subgraph.to_dict()
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="合同风险子图分析")
    parser.add_argument(
        "--contract-id",
        type=str,
        required=True,
        help="合同ID（Nebula Graph 中的节点ID）"
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=3,
        help="递归深度，默认3"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("合同风险子图分析")
    print("=" * 60)
    print(f"  合同ID: {args.contract_id}")
    print(f"  递归深度: {args.max_depth}")
    
    result = get_contract_risk_subgraph_with_html(
        contract_id=args.contract_id,
        max_depth=args.max_depth
    )
    
    print(f"\n分析完成！")
    print(f"  节点数: {len(result['subgraph']['nodes'])}")
    print(f"  边数: {len(result['subgraph']['edges'])}")
    print(f"  HTML文件: {result['html_url']}")

