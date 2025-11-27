## 场景概述
- 履约关联风险分析
- 履约能力风险分析
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

### 履约关联 & 履约能力 风险分析
> details结构, 待定
... 相关的场景的details数据结构定义

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

### 场景2
...