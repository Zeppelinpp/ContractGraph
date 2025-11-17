// 全局变量
let currentPersonId = null;
let currentCycleIndex = null;
let legalGraphSvg = null;
let circularGraphSvg = null;
let legalZoom = null;
let circularZoom = null;

// Tab切换
function switchTab(tabName) {
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
    
    document.querySelector(`[onclick="switchTab('${tabName}')"]`).classList.add('active');
    document.getElementById(`${tabName}-tab`).classList.add('active');
}

// ============================================================================
// 法律事件监测功能
// ============================================================================

async function loadLegalEventPersons() {
    const statusEl = document.getElementById('legal-status');
    statusEl.textContent = '加载中...';
    
    try {
        const response = await fetch('/api/legal-events/persons');
        const data = await response.json();
        
        if (data.error) {
            statusEl.textContent = `错误: ${data.error}`;
            return;
        }
        
        const persons = data.persons;
        statusEl.textContent = `找到 ${persons.length} 个涉及法律事件的人员`;
        
        const listEl = document.getElementById('persons-list');
        if (persons.length === 0) {
            listEl.innerHTML = '<div class="empty-state">暂无数据</div>';
            return;
        }
        
        listEl.innerHTML = persons.map(person => `
            <div class="person-item" onclick="traceLegalRisk('${person.person_id}', '${person.person_name}')">
                <div class="person-name">${person.person_name || person.person_id}</div>
                <div class="person-event">${person.event_name}</div>
                <div class="event-badge">${person.event_type} - ${person.event_status}</div>
            </div>
        `).join('');
        
    } catch (error) {
        statusEl.textContent = `错误: ${error.message}`;
    }
}

async function traceLegalRisk(personId, personName) {
    currentPersonId = personId;
    
    // 高亮选中的人员
    document.querySelectorAll('.person-item').forEach(el => el.classList.remove('active'));
    event.target.closest('.person-item').classList.add('active');
    
    const statusEl = document.getElementById('legal-status');
    statusEl.textContent = `正在追踪 ${personName} 的风险传导路径...`;
    
    try {
        const response = await fetch(`/api/legal-events/trace/${personId}`);
        const data = await response.json();
        
        if (data.error) {
            statusEl.textContent = `错误: ${data.error}`;
            return;
        }
        
        const paths = data.risk_paths;
        statusEl.textContent = `找到 ${paths.length} 条风险传导路径`;
        
        displayRiskPaths(paths);
        visualizeLegalGraph(paths);
        
    } catch (error) {
        statusEl.textContent = `错误: ${error.message}`;
    }
}

function displayRiskPaths(paths) {
    const pathsEl = document.getElementById('risk-paths');
    
    if (paths.length === 0) {
        pathsEl.innerHTML = '<div class="empty-state">未发现风险传导路径</div>';
        return;
    }
    
    pathsEl.innerHTML = paths.map((path, index) => {
        const chain = path.nodes.map(node => {
            const nodeClass = `node-${node.type}`;
            const label = node.label || node.id;
            return `<span class="node-badge ${nodeClass}">${label}</span>`;
        }).join(' → ');
        
        return `
            <div class="path-item">
                <div style="font-weight: bold; margin-bottom: 8px;">路径 ${index + 1}</div>
                <div class="path-chain">${chain}</div>
            </div>
        `;
    }).join('');
}

function visualizeLegalGraph(paths) {
    const width = document.getElementById('legal-graph').clientWidth;
    const height = 600;
    
    // 清空SVG
    d3.select('#legal-graph').selectAll('*').remove();
    
    if (paths.length === 0) return;
    
    // 提取节点和边
    const nodesMap = new Map();
    const links = [];
    
    paths.forEach(path => {
        path.nodes.forEach(node => {
            if (!nodesMap.has(node.id)) {
                nodesMap.set(node.id, {
                    id: node.id,
                    type: node.type,
                    label: node.label || node.id
                });
            }
        });
        
        path.edges.forEach(edge => {
            links.push({
                source: edge.source,
                target: edge.target,
                type: edge.type
            });
        });
    });
    
    const nodes = Array.from(nodesMap.values());
    
    // 创建SVG
    const svg = d3.select('#legal-graph')
        .attr('viewBox', [0, 0, width, height]);
    
    const g = svg.append('g');
    
    // 缩放功能
    legalZoom = d3.zoom()
        .scaleExtent([0.1, 4])
        .on('zoom', (event) => {
            g.attr('transform', event.transform);
        });
    
    svg.call(legalZoom);
    legalGraphSvg = svg;
    
    // 力导向图
    const simulation = d3.forceSimulation(nodes)
        .force('link', d3.forceLink(links).id(d => d.id).distance(150))
        .force('charge', d3.forceManyBody().strength(-300))
        .force('center', d3.forceCenter(width / 2, height / 2))
        .force('collision', d3.forceCollide().radius(50));
    
    // 绘制边
    const link = g.append('g')
        .selectAll('line')
        .data(links)
        .join('line')
        .attr('class', 'link')
        .attr('stroke-width', 2);
    
    // 边标签
    const linkLabel = g.append('g')
        .selectAll('text')
        .data(links)
        .join('text')
        .attr('class', 'link-label')
        .text(d => d.type);
    
    // 节点颜色
    const colorMap = {
        'Person': '#1976d2',
        'Company': '#7b1fa2',
        'Contract': '#388e3c',
        'LegalEvent': '#d32f2f',
        'Transaction': '#f57c00'
    };
    
    // 绘制节点
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
        .attr('r', 20)
        .attr('fill', d => colorMap[d.type] || '#999');
    
    node.append('text')
        .attr('dy', 35)
        .attr('text-anchor', 'middle')
        .text(d => d.label.length > 10 ? d.label.substring(0, 10) + '...' : d.label)
        .style('fill', '#333')
        .style('font-size', '12px');
    
    // 更新位置
    simulation.on('tick', () => {
        link
            .attr('x1', d => d.source.x)
            .attr('y1', d => d.source.y)
            .attr('x2', d => d.target.x)
            .attr('y2', d => d.target.y);
        
        linkLabel
            .attr('x', d => (d.source.x + d.target.x) / 2)
            .attr('y', d => (d.source.y + d.target.y) / 2);
        
        node.attr('transform', d => `translate(${d.x},${d.y})`);
    });
    
    function dragstarted(event, d) {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }
    
    function dragged(event, d) {
        d.fx = event.x;
        d.fy = event.y;
    }
    
    function dragended(event, d) {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
    }
}

// ============================================================================
// 循环交易检测功能
// ============================================================================

async function detectCircularTrades() {
    const threshold = document.getElementById('similarity-threshold').value;
    const statusEl = document.getElementById('circular-status');
    statusEl.textContent = '检测中...';
    
    try {
        const response = await fetch(`/api/circular-trades/detect?threshold=${threshold}`);
        const data = await response.json();
        
        if (data.error) {
            statusEl.textContent = `错误: ${data.error}`;
            return;
        }
        
        const cycles = data.cycles;
        statusEl.textContent = `检测到 ${cycles.length} 个疑似循环交易`;
        
        displayCycles(cycles);
        
        if (cycles.length > 0) {
            showCycleDetails(cycles[0], 0);
        }
        
    } catch (error) {
        statusEl.textContent = `错误: ${error.message}`;
    }
}

function displayCycles(cycles) {
    const listEl = document.getElementById('cycles-list');
    
    if (cycles.length === 0) {
        listEl.innerHTML = '<div class="empty-state">未检测到循环交易</div>';
        return;
    }
    
    listEl.innerHTML = cycles.map((cycle, index) => {
        const similarityClass = cycle.similarity >= 95 ? 'high' : 'medium';
        return `
            <div class="cycle-item" onclick="showCycleDetails(${JSON.stringify(cycle).replace(/"/g, '&quot;')}, ${index})">
                <div class="cycle-summary">循环 ${index + 1}: ${cycle.start_company_name}</div>
                <div class="cycle-info">路径长度: ${cycle.path.length} 个节点</div>
                <div class="similarity-badge ${similarityClass}">相似度: ${cycle.similarity.toFixed(2)}%</div>
            </div>
        `;
    }).join('');
}

function showCycleDetails(cycle, index) {
    currentCycleIndex = index;
    
    // 高亮选中
    document.querySelectorAll('.cycle-item').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.cycle-item')[index]?.classList.add('active');
    
    const detailsEl = document.getElementById('cycle-details');
    
    const amountsStr = cycle.amounts.map(amt => 
        `¥${amt.toLocaleString('zh-CN', {minimumFractionDigits: 2})}`
    ).join(' → ');
    
    detailsEl.innerHTML = `
        <div class="path-item">
            <h4 style="margin-bottom: 10px;">循环详情</h4>
            <p><strong>起始公司:</strong> ${cycle.start_company_name}</p>
            <p><strong>路径长度:</strong> ${cycle.path.length} 个节点</p>
            <p><strong>金额相似度:</strong> <span style="color: #d32f2f; font-weight: bold;">${cycle.similarity.toFixed(2)}%</span></p>
            <p><strong>交易金额:</strong> ${amountsStr}</p>
            ${cycle.similarity >= 90 ? '<p style="color: #d32f2f; font-weight: bold; margin-top: 10px;">⚠️  高风险预警：金额相似度超过90%！</p>' : ''}
        </div>
    `;
    
    visualizeCircularGraph(cycle);
}

function visualizeCircularGraph(cycle) {
    const width = document.getElementById('circular-graph').clientWidth;
    const height = 600;
    
    // 清空SVG
    d3.select('#circular-graph').selectAll('*').remove();
    
    // 创建节点（循环路径）
    const nodes = cycle.path.map((nodeId, index) => ({
        id: nodeId,
        index: index,
        amount: cycle.amounts[index] || 0
    }));
    
    // 创建边
    const links = [];
    for (let i = 0; i < nodes.length - 1; i++) {
        links.push({
            source: nodes[i].id,
            target: nodes[i + 1].id,
            amount: cycle.amounts[i] || 0
        });
    }
    
    // 创建SVG
    const svg = d3.select('#circular-graph')
        .attr('viewBox', [0, 0, width, height]);
    
    const g = svg.append('g');
    
    // 缩放功能
    circularZoom = d3.zoom()
        .scaleExtent([0.1, 4])
        .on('zoom', (event) => {
            g.attr('transform', event.transform);
        });
    
    svg.call(circularZoom);
    circularGraphSvg = svg;
    
    // 圆形布局
    const radius = Math.min(width, height) / 3;
    const angleStep = (2 * Math.PI) / nodes.length;
    
    nodes.forEach((node, i) => {
        node.x = width / 2 + radius * Math.cos(i * angleStep - Math.PI / 2);
        node.y = height / 2 + radius * Math.sin(i * angleStep - Math.PI / 2);
        node.fx = node.x;
        node.fy = node.y;
    });
    
    // 绘制边
    const link = g.append('g')
        .selectAll('path')
        .data(links)
        .join('path')
        .attr('class', 'link')
        .attr('stroke', '#ff5722')
        .attr('stroke-width', 3)
        .attr('fill', 'none')
        .attr('marker-end', 'url(#arrowhead)');
    
    // 箭头
    svg.append('defs').append('marker')
        .attr('id', 'arrowhead')
        .attr('viewBox', '-0 -5 10 10')
        .attr('refX', 25)
        .attr('refY', 0)
        .attr('orient', 'auto')
        .attr('markerWidth', 6)
        .attr('markerHeight', 6)
        .append('svg:path')
        .attr('d', 'M 0,-5 L 10 ,0 L 0,5')
        .attr('fill', '#ff5722');
    
    // 边标签（金额）
    const linkLabel = g.append('g')
        .selectAll('text')
        .data(links)
        .join('text')
        .attr('class', 'link-label')
        .attr('text-anchor', 'middle')
        .style('fill', '#d32f2f')
        .style('font-weight', 'bold')
        .text(d => `¥${(d.amount / 10000).toFixed(1)}万`);
    
    // 绘制节点
    const node = g.append('g')
        .selectAll('g')
        .data(nodes)
        .join('g')
        .attr('class', 'node');
    
    node.append('circle')
        .attr('r', 25)
        .attr('fill', '#7b1fa2')
        .attr('stroke', '#fff')
        .attr('stroke-width', 3);
    
    node.append('text')
        .attr('dy', 4)
        .attr('text-anchor', 'middle')
        .text((d, i) => i + 1)
        .style('fill', 'white')
        .style('font-weight', 'bold');
    
    node.append('text')
        .attr('dy', 40)
        .attr('text-anchor', 'middle')
        .text(d => d.id.length > 8 ? d.id.substring(0, 8) + '...' : d.id)
        .style('fill', '#333')
        .style('font-size', '11px');
    
    // 更新位置
    link.attr('d', d => {
        const sourceNode = nodes.find(n => n.id === d.source);
        const targetNode = nodes.find(n => n.id === d.target);
        return `M${sourceNode.x},${sourceNode.y} L${targetNode.x},${targetNode.y}`;
    });
    
    linkLabel.attr('x', d => {
        const sourceNode = nodes.find(n => n.id === d.source);
        const targetNode = nodes.find(n => n.id === d.target);
        return (sourceNode.x + targetNode.x) / 2;
    }).attr('y', d => {
        const sourceNode = nodes.find(n => n.id === d.source);
        const targetNode = nodes.find(n => n.id === d.target);
        return (sourceNode.y + targetNode.y) / 2;
    });
    
    node.attr('transform', d => `translate(${d.x},${d.y})`);
}

// ============================================================================
// 缩放控制
// ============================================================================

function zoomIn() {
    if (legalGraphSvg && legalZoom) {
        legalGraphSvg.transition().call(legalZoom.scaleBy, 1.3);
    }
}

function zoomOut() {
    if (legalGraphSvg && legalZoom) {
        legalGraphSvg.transition().call(legalZoom.scaleBy, 0.7);
    }
}

function resetView() {
    if (legalGraphSvg && legalZoom) {
        legalGraphSvg.transition().call(legalZoom.transform, d3.zoomIdentity);
    }
}

function zoomInCircular() {
    if (circularGraphSvg && circularZoom) {
        circularGraphSvg.transition().call(circularZoom.scaleBy, 1.3);
    }
}

function zoomOutCircular() {
    if (circularGraphSvg && circularZoom) {
        circularGraphSvg.transition().call(circularZoom.scaleBy, 0.7);
    }
}

function resetViewCircular() {
    if (circularGraphSvg && circularZoom) {
        circularGraphSvg.transition().call(circularZoom.transform, d3.zoomIdentity);
    }
}

// 初始化
document.addEventListener('DOMContentLoaded', () => {
    console.log('交互式Demo已加载');
});

