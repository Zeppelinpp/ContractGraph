## 场景概述
- 履约关联风险分析
- 履约能力风险分析（外部风险事件传导分析）
- 法务风险分析
- 义务履约风险分析
- 循环交易检测
- 关联方串通网络分析

## 多场景统一的请求参数
```json
{
    "orgs": [...], // 组织id列表
    "period": [], // 一个指定期间或者期间范围
    "params": {
        "type": string, // 标记场景类型，不同场景类型的算法参数不同，详细参考参数清单
    } // 算法参数
}
```
## 返回结构
### 统一字段
```json
{
    "type": string, // 场景类型
    "count": int,  // 期间，组织范围内该场景的风险合同数量
    "contract_ids": [...], // 合同id
    "details": {} // 场景相关的具体数据：路径，pattern源数据，不同场景details下的结构不同
}
```

### 法务风险分析（诉讼传染风险）- FraudRank

场景类型：`fraud_rank`

返回结构示例：
```json
{
    "type": "fraud_rank",
    "count": 10,
    "contract_ids": ["CON_001", "CON_002", ...],
    "details": {
        "contract_list": [
            {
                "contract_id": "CON_001",
                "contract_no": "HT-2024-001",
                "contract_name": "采购合同",
                "risk_score": 0.85,
                "risk_level": "高风险",
                "amount": 1000000.0,
                "sign_date": "2024-01-15",
                "status": "执行中",
                "party_a_id": "COMP_001",
                "party_a_name": "甲方公司名称",
                "party_b_id": "COMP_002",
                "party_b_name": "乙方公司名称"
            }
        ],
        "metadata": {
            "node_count": 150,
            "edge_count": 320,
            "seed_count": 25,
            "company_count": 50,
            "contract_count": 10,
            "timestamp": "2024-01-20T10:30:00.000000",
            "execution_time": 2.35
        }
    }
}
```

字段说明：
- `contract_list`: 合同风险列表，按风险分数倒序排列
  - `contract_id`: 合同ID（Nebula Graph 节点ID）
  - `contract_no`: 合同编号
  - `contract_name`: 合同名称
  - `risk_score`: 风险分数（0-1，越高风险越大）
  - `risk_level`: 风险等级（高风险/中风险/低风险）
  - `amount`: 签约金额（元）
  - `sign_date`: 签订日期
  - `status`: 合同状态
  - `party_a_id`: 甲方公司ID
  - `party_a_name`: 甲方公司名称
  - `party_b_id`: 乙方公司ID
  - `party_b_name`: 乙方公司名称
- `metadata`: 分析元数据
  - `node_count`: 图谱节点数量
  - `edge_count`: 图谱边数量
  - `seed_count`: 风险种子节点数量
  - `company_count`: 涉及公司数量
  - `contract_count`: 风险合同数量
  - `timestamp`: 分析时间戳
  - `execution_time`: 执行耗时（秒）

#### 法务风险子图可视化

接口：`POST /api/contract-risk/subgraph`

以合同ID为入口，递归获取关联的法律事件、相对方公司、以及这些公司涉及的其他有法律事件的合同，生成交互式HTML可视化页面。

**POST 请求参数：**
```json
{
    "contract_id": "Contract_CON_001",
    "max_depth": 3
}
```

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `contract_id` | string | 必填 | 合同ID（Nebula Graph 节点ID） |
| `max_depth` | int | `3` | 递归深度，范围 1-5 |

**返回结构示例：**
```json
{
    "success": true,
    "contract_id": "Contract_CON_001",
    "max_depth": 3,
    "html_url": "/api/contract-risk/view/contract_risk_subgraph_Contract_CON_001.html",
    "node_count": 15,
    "edge_count": 22,
    "nodes": [
        {
            "id": "Contract_CON_001",
            "type": "Contract",
            "label": "采购合同-华信建材",
            "properties": {"contract_no": "HT-2024-001", "amount": 1000000}
        },
        {
            "id": "LegalEvent_CASE_001",
            "type": "LegalEvent",
            "label": "合同纠纷案件-1",
            "properties": {"event_type": "Case", "status": "F"}
        }
    ],
    "edges": [
        {
            "source": "Contract_CON_001",
            "target": "LegalEvent_CASE_001",
            "type": "RELATED_TO",
            "properties": {}
        }
    ]
}
```

**字段说明：**
- `success`: 是否成功
- `contract_id`: 入口合同ID
- `max_depth`: 递归深度
- `html_url`: 可视化HTML页面URL（可嵌入iframe）
- `node_count`: 子图节点数量
- `edge_count`: 子图边数量
- `nodes`: 节点列表
  - `id`: 节点ID
  - `type`: 节点类型（Contract/LegalEvent/Company/Person）
  - `label`: 节点标签
  - `properties`: 节点属性
- `edges`: 边列表
  - `source`: 源节点ID
  - `target`: 目标节点ID
  - `type`: 边类型（RELATED_TO/PARTY_A/PARTY_B/LEGAL_PERSON等）
  - `properties`: 边属性

**查看HTML页面：**
```bash
open "http://localhost:8000/api/contract-risk/view/contract_risk_subgraph_Contract_CON_001.html"
```

### 循环交易检测

场景类型：`circular_trade`

返回结构示例：
```json
{
    "type": "circular_trade",
    "count": 3,
    "contract_ids": ["CON_001", "CON_005", "CON_012"],
    "details": {
        "pattern_list": [
            {
                "central_company": "Company_ORG002",
                "central_company_name": "中建华东分公司",
                "dispersed_companies": ["Company_SUP001", "Company_SUP002", "Company_SUP003"],
                "related_companies": ["Company_ORG002", "Company_ORG001"],
                "total_outflow": 5000000.0,
                "total_inflow": 4800000.0,
                "similarity": 0.96,
                "inter_trade_count": 5,
                "time_span_days": 120,
                "risk_score": 0.68,
                "transaction_ids": ["TXN_001", "TXN_002", "TXN_003"],
                "contract_ids": ["CON_001", "CON_005"]
            }
        ],
        "metadata": {
            "pattern_count": 3,
            "contract_count": 3,
            "time_window_days": 180,
            "amount_threshold": 500000.0,
            "timestamp": "2024-01-20T10:30:00.000000",
            "execution_time": 1.85
        }
    }
}
```

字段说明：
- `pattern_list`: 可疑循环交易模式列表，按风险分数倒序排列
  - `central_company`: 核心公司ID（资金流出的源头）
  - `central_company_name`: 核心公司名称
  - `dispersed_companies`: 分散节点公司ID列表（资金流向的中间节点）
  - `related_companies`: 关联公司ID列表（通过法人或控股关系关联的公司）
  - `total_outflow`: 流出金额（元）
  - `total_inflow`: 流入金额（元）
  - `similarity`: 流入流出金额相似度（0-1，越高越可疑）
  - `inter_trade_count`: 分散节点之间的交易数量
  - `time_span_days`: 资金循环的时间跨度（天）
  - `risk_score`: 风险分数（0-1，越高风险越大）
  - `transaction_ids`: 涉及的交易ID列表
  - `contract_ids`: 涉及的合同ID列表
- `metadata`: 分析元数据
  - `pattern_count`: 检测到的可疑模式数量
  - `contract_count`: 涉及的合同数量
  - `time_window_days`: 检测使用的时间窗口
  - `amount_threshold`: 检测使用的金额阈值
  - `timestamp`: 分析时间戳
  - `execution_time`: 执行耗时（秒）

#### 循环交易子图可视化

接口：`POST /api/circular-trade/subgraph`

以合同ID为入口，查找合同甲/乙方公司，检测以该公司为核心的循环交易模式，并生成交互式HTML可视化页面。

请求参数：
```json
{
    "contract_id": "Contract_CON_001",
    "time_window_days": 180,
    "amount_threshold": 500000.0
}
```

返回结构示例：
```json
{
    "success": true,
    "central_company": "Company_ORG002",
    "html_url": "/api/circular-trade/view/circular_trade_pattern_Company_ORG002.html",
    "node_count": 8,
    "edge_count": 12,
    "contract_ids": ["CON_001", "CON_005"]
}
```

字段说明：
- `success`: 是否成功检测到循环交易模式
- `central_company`: 检测到的核心公司ID
- `html_url`: 可视化HTML页面URL（可嵌入iframe）
- `node_count`: 子图节点数量
- `edge_count`: 子图边数量
- `contract_ids`: 涉及的合同ID列表

### 履约能力风险分析（外部风险事件传导分析）

场景类型：`external_risk_rank`

基于 PageRank 算法，计算企业因行政处罚、经营异常等外部风险事件的风险传导分数。
风险传导路径：AdminPenalty/BusinessAbnormal -> Company -> [CONTROLS/TRADES_WITH/...] -> Company

返回结构示例：
```json
{
    "type": "external_risk_rank",
    "count": 25,
    "contract_ids": ["CON_001", "CON_002", "CON_003", ...],
    "details": {
        "company_list": [
            {
                "company_id": "Company_ORG001",
                "company_name": "中建华东分公司",
                "risk_score": 0.72,
                "risk_level": "高风险",
                "risk_source": "直接关联",
                "risk_events": "AdminPenalty(AP_2024_001...); BusinessAbnormal(BA_2024_003...)",
                "legal_person": "张三",
                "credit_code": "91310000XXX"
            },
            {
                "company_id": "Company_ORG002",
                "company_name": "华信建材有限公司",
                "risk_score": 0.45,
                "risk_level": "中风险",
                "risk_source": "传导",
                "risk_events": "传导风险",
                "legal_person": "李四",
                "credit_code": "91310000YYY"
            }
        ],
        "metadata": {
            "node_count": 150,
            "edge_count": 320,
            "seed_count": 15,
            "company_count": 20,
            "contract_count": 25,
            "risk_type": "all",
            "timestamp": "2024-01-20T10:30:00.000000",
            "execution_time": 2.35
        }
    }
}
```

字段说明：
- `company_list`: 公司风险列表，按风险分数倒序排列
  - `company_id`: 公司ID（Nebula Graph 节点ID）
  - `company_name`: 公司名称
  - `risk_score`: 风险分数（0-1，越高风险越大）
  - `risk_level`: 风险等级（高风险/中风险/低风险/正常）
  - `risk_source`: 风险来源（直接关联 = 公司直接关联行政处罚/经营异常；传导 = 通过关联关系传导）
  - `risk_events`: 关联的风险事件描述
  - `legal_person`: 法人代表
  - `credit_code`: 信用代码
- `metadata`: 分析元数据
  - `node_count`: 图谱节点数量
  - `edge_count`: 图谱边数量
  - `seed_count`: 风险种子节点数量（直接关联风险事件的公司）
  - `company_count`: 涉及公司数量
  - `contract_count`: 风险合同数量
  - `risk_type`: 风险类型
  - `timestamp`: 分析时间戳
  - `execution_time`: 执行耗时（秒）

#### 外部风险子图可视化

接口：`POST /api/external-risk-rank/subgraph`

以合同ID为入口，查找合同的相关方中存在经营异常/行政处罚的公司，获取这些公司的风险事件以及涉及的其他合同，递归展开生成交互式HTML可视化页面。

**POST 请求参数：**
```json
{
    "contract_id": "Contract_CON_001",
    "max_depth": 2,
    "risk_type": "all"
}
```

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `contract_id` | string | 必填 | 合同ID（Nebula Graph 节点ID） |
| `max_depth` | int | `2` | 递归深度，范围 1-4 |
| `risk_type` | string | `"all"` | 风险类型：`admin_penalty`、`business_abnormal`、`all` |

**返回结构示例：**
```json
{
    "success": true,
    "contract_id": "Contract_CON_001",
    "html_url": "/api/external-risk-rank/view/external_risk_subgraph_Contract_CON_001.html",
    "max_depth": 2,
    "node_count": 18,
    "edge_count": 25,
    "company_count": 5,
    "risk_event_count": 8,
    "contract_ids": ["CON_001", "CON_002", "CON_005"],
    "nodes": [
        {
            "id": "Contract_CON_001",
            "type": "Contract",
            "label": "采购合同-华信建材",
            "properties": {"contract_no": "HT-2024-001", "amount": 1000000, "depth": 0}
        },
        {
            "id": "Company_ORG001",
            "type": "Company",
            "label": "中建华东分公司",
            "properties": {"name": "中建华东分公司", "credit_code": "91310000XXX"}
        },
        {
            "id": "AdminPenalty_AP001",
            "type": "AdminPenalty",
            "label": "行政处罚-AP_2024_001",
            "properties": {"event_no": "AP_2024_001", "amount": 50000, "risk_score": 0.65}
        },
        {
            "id": "BusinessAbnormal_BA001",
            "type": "BusinessAbnormal",
            "label": "经营异常-BA_2024_003",
            "properties": {"event_no": "BA_2024_003", "risk_score": 0.72}
        }
    ],
    "edges": [
        {
            "source": "Company_ORG001",
            "target": "Contract_CON_001",
            "type": "PARTY_A",
            "properties": {}
        },
        {
            "source": "AdminPenalty_AP001",
            "target": "Company_ORG001",
            "type": "ADMIN_PENALTY_OF",
            "properties": {}
        },
        {
            "source": "BusinessAbnormal_BA001",
            "target": "Company_ORG001",
            "type": "BUSINESS_ABNORMAL_OF",
            "properties": {}
        }
    ]
}
```

**字段说明：**
- `success`: 是否成功
- `contract_id`: 入口合同ID
- `html_url`: 可视化HTML页面URL（可嵌入iframe）
- `max_depth`: 递归深度
- `node_count`: 子图节点数量
- `edge_count`: 子图边数量
- `company_count`: 相关公司数量
- `risk_event_count`: 风险事件数量（行政处罚 + 经营异常）
- `contract_ids`: 关联的风险合同ID列表
- `nodes`: 节点列表
  - `id`: 节点ID
  - `type`: 节点类型（Contract/Company/AdminPenalty/BusinessAbnormal）
  - `label`: 节点标签
  - `properties`: 节点属性
- `edges`: 边列表
  - `source`: 源节点ID
  - `target`: 目标节点ID
  - `type`: 边类型（PARTY_A/PARTY_B/ADMIN_PENALTY_OF/BUSINESS_ABNORMAL_OF）
  - `properties`: 边属性

**查看HTML页面：**
```bash
open "http://localhost:8000/api/external-risk-rank/view/external_risk_subgraph_Contract_CON_001.html"
```

### 履约关联风险分析

场景类型：`perform_risk`

返回结构示例：
```json
{
    "type": "perform_risk",
    "count": 15,
    "contract_ids": ["CON_001", "CON_002", "CON_003", ...],
    "details": {
        "company_list": [
            {
                "company_id": "Company_ORG001",
                "company_name": "中建华东分公司",
                "risk_score": 0.72,
                "overdue_count": 3,
                "risk_contract_count": 5,
                "legal_person": "张三",
                "credit_code": "91310000XXX",
                "risk_contracts": ["HT-2024-001(采购合同)", "HT-2024-005(建材合同)"]
            }
        ],
        "metadata": {
            "company_count": 10,
            "contract_count": 15,
            "overdue_transaction_count": 25,
            "current_date": "2024-01-20",
            "timestamp": "2024-01-20T10:30:00.000000",
            "execution_time": 1.85
        }
    }
}
```

字段说明：
- `company_list`: 公司风险列表，按风险分数倒序排列
  - `company_id`: 公司ID（Nebula Graph 节点ID）
  - `company_name`: 公司名称
  - `risk_score`: 风险分数（0-1，越高风险越大）
  - `overdue_count`: 逾期交易数量
  - `risk_contract_count`: 风险合同数量
  - `legal_person`: 法人代表
  - `credit_code`: 信用代码
  - `risk_contracts`: 风险合同列表（合同编号+名称）
- `metadata`: 分析元数据
  - `company_count`: 涉及公司数量
  - `contract_count`: 风险合同数量
  - `overdue_transaction_count`: 逾期交易总数
  - `current_date`: 分析基准日期
  - `timestamp`: 分析时间戳
  - `execution_time`: 执行耗时（秒）

#### 履约风险子图可视化

接口：`POST /api/perform-risk/subgraph`

以风险合同ID为入口，查找合同的相关方，获取这些相关方的逾期交易以及涉及的合同，生成交互式HTML可视化页面。

**POST 请求参数：**
```json
{
    "contract_id": "Contract_CON_001",
    "current_date": "2024-01-20"
}
```

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `contract_id` | string | 必填 | 风险合同ID（Nebula Graph 节点ID） |
| `current_date` | string | 今天 | 当前日期，格式：YYYY-MM-DD |

**返回结构示例：**
```json
{
    "success": true,
    "contract_id": "Contract_CON_001",
    "html_url": "/api/perform-risk/view/perform_risk_subgraph_Contract_CON_001.html",
    "node_count": 12,
    "edge_count": 18,
    "overdue_transaction_count": 5,
    "related_contract_count": 3,
    "company_count": 2,
    "contract_ids": ["CON_002", "CON_003", "CON_005"],
    "nodes": [
        {
            "id": "Contract_CON_001",
            "type": "Contract",
            "label": "采购合同-华信建材",
            "properties": {"contract_no": "HT-2024-001", "is_input": true}
        },
        {
            "id": "Company_ORG001",
            "type": "Company",
            "label": "中建华东分公司",
            "properties": {"name": "中建华东分公司", "credit_code": "91310000XXX"}
        },
        {
            "id": "TXN_001",
            "type": "Transaction",
            "label": "逾期15天",
            "properties": {"overdue_type": "收款逾期", "overdue_days": 15, "amount": 500000}
        }
    ],
    "edges": [
        {
            "source": "Company_ORG001",
            "target": "Contract_CON_001",
            "type": "PARTY",
            "properties": {}
        },
        {
            "source": "TXN_001",
            "target": "Company_ORG001",
            "type": "OVERDUE_FOR",
            "properties": {"overdue_type": "收款逾期"}
        }
    ]
}
```

**字段说明：**
- `success`: 是否成功
- `contract_id`: 入口合同ID
- `html_url`: 可视化HTML页面URL（可嵌入iframe）
- `node_count`: 子图节点数量
- `edge_count`: 子图边数量
- `overdue_transaction_count`: 逾期交易数量
- `related_contract_count`: 关联的逾期合同数量
- `company_count`: 相关公司数量
- `contract_ids`: 关联的逾期合同ID列表
- `nodes`: 节点列表
  - `id`: 节点ID
  - `type`: 节点类型（Contract/Company/Transaction）
  - `label`: 节点标签
  - `properties`: 节点属性
- `edges`: 边列表
  - `source`: 源节点ID
  - `target`: 目标节点ID
  - `type`: 边类型（PARTY/OVERDUE_FOR/BELONGS_TO）
  - `properties`: 边属性

**查看HTML页面：**
```bash
open "http://localhost:8000/api/perform-risk/view/perform_risk_subgraph_Contract_CON_001.html"
```

### 关联方串通网络分析

场景类型：`collusion`

检测关联方串通网络，包括轮流中标、围标等模式。通过分析公司之间的法人关系、控股关系，识别可能存在串通行为的公司集群。

返回结构示例：
```json
{
    "type": "collusion",
    "count": 15,
    "contract_ids": ["CON_001", "CON_002", "CON_003", ...],
    "details": {
        "network_list": [
            {
                "network_id": "NETWORK_1",
                "companies": ["Company_ORG001", "Company_ORG002", "Company_SUP001"],
                "size": 3,
                "risk_score": 0.72,
                "rotation_score": 0.85,
                "amount_similarity": 0.78,
                "threshold_ratio": 0.25,
                "network_density": 0.67,
                "contract_count": 8,
                "total_amount": 5000000.0,
                "avg_amount": 625000.0,
                "contract_ids": ["CON_001", "CON_002", "CON_005"]
            }
        ],
        "metadata": {
            "network_count": 5,
            "contract_count": 15,
            "min_cluster_size": 3,
            "risk_score_threshold": 0.5,
            "timestamp": "2024-01-20T10:30:00.000000",
            "execution_time": 1.85
        }
    }
}
```

字段说明：
- `network_list`: 可疑串通网络列表，按风险分数倒序排列
  - `network_id`: 网络ID
  - `companies`: 网络中的公司ID列表
  - `size`: 网络中公司数量
  - `risk_score`: 综合风险分数（0-1，越高风险越大）
  - `rotation_score`: 轮换分数（0-1，越高表示中标越均匀，越像轮流中标）
  - `amount_similarity`: 金额相似度（0-1，越高表示合同金额越相近）
  - `threshold_ratio`: 卡阈值比例（合同金额刻意卡在审批阈值附近的比例）
  - `network_density`: 网络密度（公司之间关联关系的紧密程度）
  - `contract_count`: 涉及合同数量
  - `total_amount`: 涉及金额总计（元）
  - `avg_amount`: 平均合同金额（元）
  - `contract_ids`: 涉及的合同ID列表
- `metadata`: 分析元数据
  - `network_count`: 检测到的可疑网络数量
  - `contract_count`: 涉及的合同数量
  - `min_cluster_size`: 检测使用的最小集群大小
  - `risk_score_threshold`: 检测使用的风险分数阈值
  - `timestamp`: 分析时间戳
  - `execution_time`: 执行耗时（秒）

#### 串通网络子图可视化

接口：`POST /api/collusion/subgraph`

以合同ID为入口，查找合同甲/乙方公司，检测这些公司所在的串通网络，并生成交互式HTML可视化页面。

**POST 请求参数：**
```json
{
    "contract_id": "Contract_CON_001",
    "min_cluster_size": 3,
    "risk_score_threshold": 0.5
}
```

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `contract_id` | string | 必填 | 合同ID（Nebula Graph 节点ID） |
| `min_cluster_size` | int | `3` | 最小集群大小，范围 2-10 |
| `risk_score_threshold` | number | `0.5` | 风险分数阈值，范围 0-1 |

**返回结构示例：**
```json
{
    "success": true,
    "contract_id": "Contract_CON_001",
    "html_url": "/api/collusion/view/collusion_network_NETWORK_1.html",
    "network_id": "NETWORK_1",
    "node_count": 12,
    "edge_count": 18,
    "company_count": 3,
    "contract_ids": ["CON_001", "CON_002", "CON_005"],
    "nodes": [],
    "edges": []
}
```

**字段说明：**
- `success`: 是否成功检测到串通网络
- `contract_id`: 入口合同ID
- `html_url`: 可视化HTML页面URL（可嵌入iframe）
- `network_id`: 检测到的最高风险网络ID
- `node_count`: 子图节点数量
- `edge_count`: 子图边数量
- `company_count`: 网络中公司数量
- `contract_ids`: 涉及的合同ID列表
- `nodes`: 节点列表（详细数据在HTML中）
- `edges`: 边列表（详细数据在HTML中）

**查看HTML页面：**
```bash
open "http://localhost:8000/api/collusion/view/collusion_network_NETWORK_1.html"
```

## 参数清单

### 法务风险分析（诉讼传染风险）- 参数配置

场景类型：`fraud_rank`

**params 参数说明：**
| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `type` | string | `"fraud_rank"` | 场景类型标识 |
| `top_n` | int | `50` | 返回 top N 结果 |
| `force_recompute` | bool | `false` | 是否强制重新计算 embedding 权重 |
| `edge_weights` | object | 见下方 | 边权重配置 |
| `event_type_weights` | object | `{"Case": 0.8, "Dispute": 0.5}` | 法律事件类型权重 |
| `event_type_default_weight` | number | `0.3` | 未知事件类型默认权重 |
| `status_weights` | object | `{"F": 0.9, "I": 0.8, "J": 0.7, "N": 0.4}` | 事件状态权重 |
| `status_default_weight` | number | `0.5` | 未知状态默认权重 |
| `amount_threshold` | number | `10000000.0` | 金额归一化上限（元） |

**edge_weights 默认值：**
```json
{
    "CONTROLS": 0.8, "LEGAL_PERSON": 0.75, "PAYS": 0.65, "RECEIVES": 0.60,
    "TRADES_WITH": 0.50, "IS_SUPPLIER": 0.45, "IS_CUSTOMER": 0.40,
    "PARTY_A": 0.50, "PARTY_B": 0.50
}
```

**请求示例（使用默认参数）：**
```json
{
    "orgs": ["org_001", "org_002"],
    "period": ["2024-01-01", "2024-12-31"],
    "params": { "type": "fraud_rank" }
}
```

**请求示例（自定义参数）：**
```json
{
    "orgs": ["org_001"],
    "period": ["2024-01-01", "2024-12-31"],
    "params": {
        "type": "fraud_rank",
        "top_n": 100,
        "edge_weights": { "CONTROLS": 0.9, "LEGAL_PERSON": 0.8 },
        "amount_threshold": 5000000.0
    }
}
```

### 循环交易检测 - 参数配置

场景类型：`circular_trade`

**params 参数说明：**
| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `type` | string | `"circular_trade"` | 场景类型标识 |
| `time_window_days` | int | `180` | 时间窗口（天），范围 1-365 |
| `amount_threshold` | number | `500000.0` | 金额阈值（元），低于此金额的交易不纳入检测 |

**请求示例（使用默认参数）：**
```json
{
    "orgs": ["org_001", "org_002"],
    "period": ["2024-01-01", "2024-12-31"],
    "params": { "type": "circular_trade" }
}
```

**请求示例（自定义参数）：**
```json
{
    "orgs": ["org_001"],
    "period": ["2024-01-01", "2024-06-30"],
    "params": {
        "type": "circular_trade",
        "time_window_days": 90,
        "amount_threshold": 1000000.0
    }
}
```

### 履约关联风险分析 - 参数配置

场景类型：`perform_risk`

**params 参数说明：**
| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `type` | string | `"perform_risk"` | 场景类型标识 |
| `top_n` | int | `50` | 返回 top N 结果 |
| `current_date` | string | 今天 | 当前日期，格式：YYYY-MM-DD |
| `overdue_days_max` | int | `30` | 逾期天数归一化上限 |
| `severity_power` | number | `0.7` | 逾期天数严重程度计算指数 |
| `overdue_base_weight` | number | `0.15` | 每笔逾期交易的基础风险权重 |
| `severity_multiplier_max` | number | `0.5` | 严重程度对基础权重的最大额外乘数 |
| `overdue_score_cap` | number | `0.5` | 逾期交易部分的最大风险分数贡献 |
| `risk_contract_weight` | number | `0.3` | 风险合同比例对总分的权重 |
| `amount_threshold` | number | `10000000.0` | 金额归一化上限（元） |
| `amount_weight` | number | `0.2` | 金额部分对总分的权重 |

**请求示例（使用默认参数）：**
```json
{
    "orgs": ["org_001", "org_002"],
    "period": ["2024-01-01", "2024-12-31"],
    "params": { "type": "perform_risk" }
}
```

**请求示例（自定义参数）：**
```json
{
    "orgs": ["org_001"],
    "period": ["2024-01-01", "2024-06-30"],
    "params": {
        "type": "perform_risk",
        "top_n": 100,
        "current_date": "2024-06-30",
        "overdue_days_max": 60,
        "amount_threshold": 5000000.0
    }
}
```

### 履约能力风险分析（外部风险事件传导分析）- 参数配置

场景类型：`external_risk_rank`

基于 PageRank 算法，计算企业因行政处罚、经营异常等外部风险事件的风险传导分数。

**params 参数说明：**
| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `type` | string | `"external_risk_rank"` | 场景类型标识 |
| `top_n` | int | `50` | 返回 top N 结果 |
| `risk_type` | string | `"all"` | 风险类型：`admin_penalty`(行政处罚)、`business_abnormal`(经营异常)、`all`(全部) |
| `use_cached_embedding` | bool | `true` | 是否使用缓存的 embedding 权重 |
| `damping` | number | `0.65` | PageRank 阻尼系数，范围 0-1 |
| `edge_weights` | object | 见下方 | 边权重配置 |
| `admin_penalty_weights` | object | `{"amount": 0.4, "status": 0.3, "severity": 0.3}` | 行政处罚风险评分各因子权重 |
| `admin_penalty_status_weights` | object | `{"C": 0.7, "P": 0.9}` | 行政处罚状态权重 |
| `admin_penalty_amount_max` | number | `1000000.0` | 行政处罚金额归一化上限（元） |
| `business_abnormal_weights` | object | `{"status": 0.6, "reason": 0.4}` | 经营异常风险评分各因子权重 |
| `business_abnormal_status_weights` | object | `{"C": 0.3}` | 经营异常状态权重，非C状态默认0.9 |
| `risk_level_thresholds` | object | `{"high": 0.6, "medium": 0.3, "low": 0.1}` | 风险等级划分阈值 |

**edge_weights 默认值：**
```json
{
    "CONTROLS": 0.85, "LEGAL_PERSON": 0.75, "TRADES_WITH": 0.50,
    "IS_SUPPLIER": 0.45, "IS_CUSTOMER": 0.40,
    "ADMIN_PENALTY_OF": 0.90, "BUSINESS_ABNORMAL_OF": 0.70,
    "PARTY_A": 0.50, "PARTY_B": 0.50
}
```

**请求示例（使用默认参数）：**
```json
{
    "orgs": ["org_001", "org_002"],
    "period": ["2024-01-01", "2024-12-31"],
    "params": { "type": "external_risk_rank" }
}
```

**请求示例（自定义参数）：**
```json
{
    "orgs": ["org_001"],
    "period": ["2024-01-01", "2024-06-30"],
    "params": {
        "type": "external_risk_rank",
        "top_n": 100,
        "risk_type": "admin_penalty",
        "damping": 0.85,
        "edge_weights": { "CONTROLS": 0.9, "ADMIN_PENALTY_OF": 0.95 },
        "admin_penalty_amount_max": 500000.0
    }
}
```

### 关联方串通网络分析 - 参数配置

场景类型：`collusion`

检测关联方串通网络，包括轮流中标、围标等模式。

**params 参数说明：**
| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `type` | string | `"collusion"` | 场景类型标识 |
| `top_n` | int | `50` | 返回 top N 结果 |
| `min_cluster_size` | int | `3` | 最小集群大小，至少需要这么多公司才能形成可疑网络，范围 2-10 |
| `risk_score_threshold` | number | `0.5` | 风险分数阈值，高于此值的网络被标记为可疑，范围 0-1 |
| `approval_thresholds` | array | `[1000000, 3000000, 5000000, 10000000]` | 审批金额阈值列表（元），用于检测刻意卡阈值的行为 |
| `threshold_margin` | number | `0.05` | 阈值检测边距比例，金额在 threshold*(1-margin) 到 threshold 之间视为卡阈值 |
| `feature_weights` | object | 见下方 | 串通风险评分各特征的权重配置 |

**feature_weights 默认值：**
```json
{
    "rotation": 0.3,           // 轮换分数权重
    "amount_similarity": 0.2,  // 金额相似度权重
    "threshold_ratio": 0.2,    // 卡阈值比例权重
    "network_density": 0.2,    // 网络密度权重
    "strong_relation": 0.1     // 强关联关系权重
}
```

**请求示例（使用默认参数）：**
```json
{
    "orgs": ["org_001", "org_002"],
    "period": ["2024-01-01", "2024-12-31"],
    "params": { "type": "collusion" }
}
```

**请求示例（自定义参数）：**
```json
{
    "orgs": ["org_001"],
    "period": ["2024-01-01", "2024-06-30"],
    "params": {
        "type": "collusion",
        "top_n": 100,
        "min_cluster_size": 2,
        "risk_score_threshold": 0.4,
        "approval_thresholds": [500000, 1000000, 3000000, 5000000],
        "threshold_margin": 0.08,
        "feature_weights": {
            "rotation": 0.35,
            "amount_similarity": 0.25,
            "threshold_ratio": 0.15,
            "network_density": 0.15,
            "strong_relation": 0.1
        }
    }
}
```