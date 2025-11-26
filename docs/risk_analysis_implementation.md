# 风险分析场景技术实现文档

本文档详细描述了知识图谱风险分析场景的技术实现逻辑，包括算法原理、数据流和核心代码引用。

---

## 一、风险传导类算法概述

本项目中有两个基于 **PageRank 变体算法** 的风险传导分析模块，它们遵循相同的技术架构：

```
风险源节点 → 初始化风险分数 → 加载图结构 → PageRank 迭代传播 → 输出风险排名
```

### 1.1 FraudRank - 诉讼传染风险 (`fraud_rank.py`)

**风险传导路径**：
```
LegalEvent → (RELATED_TO) → Contract → (PARTY_A/B 反向) → Company → [CONTROLS/TRADES_WITH/...] → Company
```

**实现逻辑**：

1. **定位风险源**：查询 `LegalEvent` 节点（案件、纠纷）
2. **关联合同**：通过 `RELATED_TO` 边找到涉及的 `Contract`
3. **初始化风险分数**：给 `Contract` 节点分配初始风险分（非 Company）
4. **反向构建边**：将 `Company → Contract` 的边反向为 `Contract → Company`，使风险能从合同传导给公司
5. **PageRank 传播**：风险沿着企业关系网络（控股、交易、供应商等）传导

**核心代码**：

```python
# 初始化风险种子 - 给合同分配初始风险分数
def initialize_risk_seeds(session):
    contract_event_query = """
    MATCH (con:Contract)-[:RELATED_TO]->(le:LegalEvent)
    RETURN id(con) as contract_id, ...
    """
    # 给合同分配初始风险分数（而非直接给公司）
    init_scores[contract_id] = calculate_init_score(event)
```

```python
# 反向构建 Contract -> Company 边，使风险能传导给公司
party_query = """
MATCH (c:Company)-[e:PARTY_A|PARTY_B]->(con:Contract)
RETURN id(c) as company_id, id(con) as contract_id
"""
# 注意：这里我们将 Contract 作为源节点，Company 作为目标节点
from_node = contract_id  # 反向！
to_node = company_id
graph["edges"][from_node].append((to_node, weight))
```

**初始分数计算因子**：
- 事件类型权重：Case (0.8) > Dispute (0.5)
- 金额权重：归一化到 0-1（1000万为上限）
- 状态权重：已立案 (0.9) > 一审 (0.8) > 执行 (0.7) > 已结案 (0.4)

---

### 1.2 External Risk Rank - 外部风险事件传导 (`external_risk_rank.py`)

**风险传导路径**：
```
AdminPenalty/BusinessAbnormal → (ADMIN_PENALTY_OF/BUSINESS_ABNORMAL_OF) → Company → [CONTROLS/TRADES_WITH/...] → Company
```

**实现逻辑**：

1. **定位风险源**：查询 `AdminPenalty`（行政处罚）和 `BusinessAbnormal`（经营异常）节点
2. **直接关联公司**：通过边直接找到被处罚/异常的 `Company`
3. **初始化风险分数**：给直接关联的 `Company` 分配初始风险分
4. **PageRank 传播**：风险沿着企业关系网络传导给关联方

**核心代码**：

```python
# 初始化风险种子 - 直接给公司分配初始风险分数
def initialize_external_risk_seeds(session, risk_type="all"):
    penalty_query = """
    MATCH (pen:AdminPenalty)-[:ADMIN_PENALTY_OF]->(c:Company)
    RETURN id(c) as company_id, ...
    """
    # 直接给公司分配初始风险分数
    init_scores[company_id] = calculate_admin_penalty_score(event)
```

**初始分数计算因子**：

行政处罚：
- 金额因子 (40%)：归一化到 0-1（100万为上限）
- 状态因子 (30%)：进行中 (0.9) > 已完成 (0.7)
- 严重度因子 (30%)：安全类 (0.9) > 罚款类 (0.7) > 警告类 (0.4)

经营异常：
- 状态因子 (60%)：仍在异常名录 (0.9) > 已移出 (0.3)
- 原因因子 (40%)：弄虚作假 (0.9) > 无法联系 (0.7) > 年报问题 (0.4)

---

### 1.3 两者的关键区别

| 维度 | FraudRank | External Risk Rank |
|------|-----------|-------------------|
| 风险源 | LegalEvent (案件/纠纷) | AdminPenalty / BusinessAbnormal |
| 初始分数分配对象 | Contract (合同) | Company (公司) |
| 需要反向边 | 是（Contract → Company） | 否（直接关联公司） |
| 传导路径 | 更长（需经过合同） | 更短（直接从公司开始） |

---

### 1.4 PageRank 传播算法

两个模块使用相同的 PageRank 变体算法：

```python
def compute_risk_rank(graph, init_scores, damping=0.85, max_iter=100, tolerance=1e-6):
    scores = {node: init_scores.get(node, 0.0) for node in graph["nodes"]}
    
    for iteration in range(max_iter):
        new_scores = {}
        for node in graph["nodes"]:
            # 基础分数（保留初始风险）
            base_score = (1 - damping) * init_scores.get(node, 0.0)
            
            # 从入边传播来的分数
            propagated_score = 0.0
            for neighbor, neighbors_list in graph["edges"].items():
                for target, weight in neighbors_list:
                    if target == node:
                        out_deg = graph["out_degree"][neighbor]
                        propagated_score += weight * scores[neighbor] / out_deg
            
            new_scores[node] = base_score + damping * propagated_score
        
        scores = new_scores
        if converged: break
    
    return scores
```

**关键参数**：
- `damping = 0.85`：阻尼系数，控制传播衰减
- `tolerance = 1e-6`：收敛阈值
- 边权重：不同关系类型有不同权重（CONTROLS: 0.8, TRADES_WITH: 0.5 等）

---

## 二、边权重计算与持久化 (`embedding.py`)

### 2.1 Node2Vec 图嵌入

使用 Node2Vec 算法学习节点的向量表示，然后通过余弦相似度计算边权重：

```python
# Node2Vec 配置
n2v = Node2Vec(G, dimensions=32, walk_length=10, num_walks=20, p=1, q=0.5)

# 边权重 = 业务权重 * 0.7 + AI相似度 * 0.3
final_weight = business_weight * base_weight + ai_weight * cosine_similarity(u, v)
```

### 2.2 缓存机制

为避免每次运行都重新计算 embedding，实现了基于图结构哈希的缓存机制：

```python
def get_or_compute_edge_weights(session, cache_dir, force_recompute=False):
    # 1. 计算当前图结构的 MD5 哈希
    current_hash = compute_graph_hash(session)
    
    # 2. 检查缓存是否有效
    if cached_hash == current_hash:
        return load_edge_weights(cache_file)  # 直接返回缓存
    
    # 3. 重新计算并保存
    weights = compute_edge_weights(session)
    save_edge_weights(weights, cache_file)
    return weights
```

**缓存文件**：
- `cache/edge_weights.json`：边权重数据
- `cache/graph_hash.txt`：图结构哈希值

---

## 三、履约关联风险检测 (`perform_risk.py`)

**业务场景**：逾期链式损失风险 - 对方一旦逾期，同一批"标的"合同可能集体违约。

### 3.1 实现逻辑

1. **查找逾期交易**：
   - 收款逾期：到期日 < 当前日期 且 总金额 > 已付金额
   - 交货逾期：到期日 < 当前日期 且 履约状态 ≠ "C"

2. **查找关联相对方**：
   - 通过 TRADES_WITH、IS_SUPPLIER、IS_CUSTOMER、CONTROLS 关系扩展

3. **查找风险合同**：
   - 提取标的名称（如"建材采购合同-公司名" → "建材采购合同"）
   - 找出相同标的名称的合同
   - 仅保留来自相同或关联相对方的合同

4. **计算风险分数**：

```python
# 风险分数 = 逾期基础分(0.5) + 风险合同比例分(0.3) + 金额分(0.2)
score = min(overdue_count * 0.15 * severity_multiplier, 0.5)  # 逾期基础分
score += (contracts_with_overdue / total_risk_contracts) * 0.3  # 风险合同比例
score += min(total_amount / 10000000, 1.0) * 0.2  # 金额分
```

---

## 四、循环交易检测 (`circular_trade.py`)

**业务场景**：虚假贸易自融风险 - 资金先转出再原路回流，构造虚假贸易背景。

### 4.1 分散-汇聚模式检测

```
核心公司 → 分散节点A、B、C → 中间交易 → 汇聚回核心公司或关联方
```

**实现逻辑**：

1. **识别资金流出**：从核心公司流出到多个分散节点（金额 >= 50万）
2. **检测中间交易**：分散节点之间是否有交易
3. **检测资金回流**：是否汇聚回核心公司或其关联方（共同法人、控股关系）
4. **计算风险分数**：

```python
# 风险分数 = 金额相似度(40%) + 分散节点数(30%) + 中间交易密度(30%)
risk_score = similarity * 0.4 + min(num_dispersed/10, 1.0) * 0.3 + min(num_inter_trades/20, 1.0) * 0.3
```

**触发条件**：流入流出金额相似度 >= 70%

---

## 五、关联方串通网络分析 (`collusion.py`)

**业务场景**：围标串标风险 - 多家关联公司轮流中标、金额雷同且卡审批阈值。

### 5.1 实现逻辑

1. **识别关联公司集群**：
   - 共享法人的公司
   - 存在控股关系的公司
   - 使用 BFS 找出连通分量（最小集群：3家公司）

2. **分析串通行为模式**：

```python
# 综合风险分数
risk_score = (
    rotation_score * 0.3      # 轮换分数：检测规律的轮流中标
    + amount_similarity * 0.2  # 金额相似度：合同金额的变异系数
    + threshold_ratio * 0.2    # 卡阈值比例：刻意卡在审批阈值附近
    + density * 0.2            # 网络密度：关联关系的紧密程度
    + (0.1 if has_strong_relation else 0)  # 关联强度
)
```

**审批阈值**：100万、300万、500万、1000万（上下浮动 5%）

---

## 六、技术架构总结

```
┌─────────────────────────────────────────────────────────────────┐
│                      风险分析技术架构                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │ LegalEvent  │    │AdminPenalty │    │ Transaction │         │
│  │   (案件)    │    │  (行政处罚) │    │   (交易)    │         │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘         │
│         │                  │                  │                 │
│         ▼                  ▼                  ▼                 │
│  ┌─────────────────────────────────────────────────────┐       │
│  │              初始风险分数计算                         │       │
│  │  - 事件类型/金额/状态加权                             │       │
│  │  - 严重程度评估                                      │       │
│  └──────────────────────┬──────────────────────────────┘       │
│                         │                                       │
│                         ▼                                       │
│  ┌─────────────────────────────────────────────────────┐       │
│  │              图结构加载与边权重计算                    │       │
│  │  - Node2Vec 图嵌入                                   │       │
│  │  - 业务权重 + AI相似度                               │       │
│  │  - 缓存机制（图结构哈希校验）                         │       │
│  └──────────────────────┬──────────────────────────────┘       │
│                         │                                       │
│                         ▼                                       │
│  ┌─────────────────────────────────────────────────────┐       │
│  │              风险传导/模式检测                        │       │
│  │  - PageRank 变体算法（传导类）                       │       │
│  │  - 分散-汇聚模式检测（循环交易）                      │       │
│  │  - 社区检测+行为分析（串通网络）                      │       │
│  └──────────────────────┬──────────────────────────────┘       │
│                         │                                       │
│                         ▼                                       │
│  ┌─────────────────────────────────────────────────────┐       │
│  │              风险报告生成                             │       │
│  │  - 风险等级划分                                      │       │
│  │  - CSV 报告输出                                      │       │
│  └─────────────────────────────────────────────────────┘       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 七、使用方式

```bash
# 诉讼传染风险分析
uv run python -m src.analysis.fraud_rank
uv run python -m src.analysis.fraud_rank --force-recompute  # 强制重算embedding

# 外部风险事件传导分析
uv run python -m src.analysis.external_risk_rank --risk-type all
uv run python -m src.analysis.external_risk_rank --risk-type admin_penalty
uv run python -m src.analysis.external_risk_rank --risk-type business_abnormal

# 履约关联风险检测
uv run python -m src.analysis.perform_risk
uv run python -m src.analysis.perform_risk --date 2025-11-01

# 循环交易检测
uv run python -m src.analysis.circular_trade

# 关联方串通网络分析
uv run python -m src.analysis.collusion
```

