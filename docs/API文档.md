# 知识图谱高级分析 API 文档

## 概述

本文档描述了知识图谱高级分析模块的 API 接口。这些接口基于 FastAPI 框架，提供以下四种高级分析功能：

1. **FraudRank 欺诈风险传导分析** - 基于图算法的风险传播分析
2. **循环交易检测** - 检测分散-汇聚模式的循环交易
3. **空壳公司识别** - 识别具有空壳公司特征的企业
4. **关联方串通网络分析** - 检测关联公司之间的串通行为

## 基础信息

- **Base URL**: `http://localhost:8000` (默认)
- **Content-Type**: `application/json`
- **请求格式**: POST JSON

## API 接口

### 1. FraudRank 欺诈风险传导分析

#### 接口信息

- **路径**: `/api/fraud-rank`
- **方法**: `POST`
- **描述**: 基于图算法的风险传播分析，计算公司的欺诈风险分数

#### 请求参数

```json
{
  "damping": 0.85,        // 可选，阻尼系数，默认 0.85，范围 0-1
  "max_iter": 100,        // 可选，最大迭代次数，默认 100
  "top_n": 50             // 可选，返回前N个高风险公司，默认 50
}
```

**参数说明**:
- `damping`: 阻尼系数，控制风险传播的衰减程度，值越大传播越远
- `max_iter`: 算法最大迭代次数
- `top_n`: 返回结果数量，按风险分数降序排列

#### 请求示例

```bash
curl -X POST "http://localhost:8000/api/fraud-rank" \
  -H "Content-Type: application/json" \
  -d '{
    "damping": 0.85,
    "max_iter": 100,
    "top_n": 50
  }'
```

#### 返回数据结构

```json
{
  "success": true,
  "message": "分析完成",
  "data": {
    "total_companies": 150,
    "risk_seed_count": 25,
    "results": [
      {
        "company_id": "company_123",
        "company_name": "XX有限公司",
        "risk_score": 0.8234,
        "risk_level": "高风险",
        "legal_person": "张三",
        "credit_code": "91110000MA01234567"
      }
    ]
  },
  "metadata": {
    "execution_time": 2.34,
    "iteration_count": 15
  }
}
```

**返回字段说明**:
- `success`: 请求是否成功
- `message`: 响应消息
- `data.total_companies`: 分析的公司总数
- `data.risk_seed_count`: 风险种子节点数量
- `data.results`: 公司风险分析结果列表
  - `company_id`: 公司ID
  - `company_name`: 公司名称
  - `risk_score`: 风险分数 (0-1)，越高风险越大
  - `risk_level`: 风险等级 ("高风险" >= 0.7, "中风险" >= 0.4, "低风险" >= 0.2, "正常" < 0.2)
  - `legal_person`: 法人代表
  - `credit_code`: 统一社会信用代码
- `metadata.execution_time`: 执行时间（秒）
- `metadata.iteration_count`: 实际迭代次数

---

### 2. 循环交易检测

#### 接口信息

- **路径**: `/api/circular-trades`
- **方法**: `POST`
- **描述**: 检测分散-汇聚模式的循环交易，识别可疑的资金流转模式

#### 请求参数

```json
{
  "time_window_days": 180,    // 可选，时间窗口（天），默认 180
  "amount_threshold": 500000   // 可选，金额阈值（元），默认 500000
}
```

**参数说明**:
- `time_window_days`: 分析的时间窗口，单位天
- `amount_threshold`: 交易金额阈值，低于此金额的交易不参与分析

#### 请求示例

```bash
curl -X POST "http://localhost:8000/api/circular-trades" \
  -H "Content-Type: application/json" \
  -d '{
    "time_window_days": 180,
    "amount_threshold": 500000
  }'
```

#### 返回数据结构

```json
{
  "success": true,
  "message": "分析完成",
  "data": {
    "total_patterns": 12,
    "patterns": [
      {
        "central_company": "company_456",
        "dispersed_companies": ["company_789", "company_101", "company_102"],
        "related_companies": ["company_456", "company_457"],
        "total_outflow": 5000000.0,
        "total_inflow": 4800000.0,
        "similarity": 0.96,
        "inter_trade_count": 5,
        "time_span_days": 120,
        "risk_score": 0.7823
      }
    ]
  },
  "metadata": {
    "execution_time": 3.45
  }
}
```

**返回字段说明**:
- `data.total_patterns`: 发现的可疑模式总数
- `data.patterns`: 可疑循环交易模式列表
  - `central_company`: 核心公司ID（资金流出的中心公司）
  - `dispersed_companies`: 分散节点公司ID列表（资金分散到的公司）
  - `related_companies`: 关联公司ID列表（与核心公司有关联关系的公司）
  - `total_outflow`: 总流出金额（元）
  - `total_inflow`: 总流入金额（元）
  - `similarity`: 流入流出金额相似度 (0-1)，>= 0.7 才被认为是可疑
  - `inter_trade_count`: 分散节点之间的交易数量
  - `time_span_days`: 时间跨度（天）
  - `risk_score`: 风险分数 (0-1)，综合考虑相似度、分散节点数、中间交易密度

---

### 3. 空壳公司识别

#### 接口信息

- **路径**: `/api/shell-company`
- **方法**: `POST`
- **描述**: 识别具有空壳公司特征的企业，基于多维度特征分析

#### 请求参数

```json
{
  "min_score": 0.6,           // 可选，最低嫌疑分数阈值，默认 0.6
  "include_networks": true     // 可选，是否包含空壳公司网络分析，默认 true
}
```

**参数说明**:
- `min_score`: 最低嫌疑分数，只返回分数 >= min_score 的公司
- `include_networks`: 是否进行空壳公司网络分析（共享法人的公司网络）

#### 请求示例

```bash
curl -X POST "http://localhost:8000/api/shell-company" \
  -H "Content-Type: application/json" \
  -d '{
    "min_score": 0.6,
    "include_networks": true
  }'
```

#### 返回数据结构

```json
{
  "success": true,
  "message": "分析完成",
  "data": {
    "total_companies": 200,
    "high_risk_count": 15,
    "high_risk_ratio": 0.075,
    "companies": [
      {
        "company_id": "company_789",
        "company_name": "XX贸易有限公司",
        "legal_person": "李四",
        "shell_score": 0.75,
        "pass_through_ratio": 0.95,
        "transaction_velocity_days": 3.5,
        "partner_diversity": 0.15,
        "total_transaction_count": 20,
        "total_inflow": 10000000.0,
        "total_outflow": 9500000.0,
        "degree_centrality": 3,
        "legal_person_company_count": 8,
        "contract_count": 2
      }
    ],
    "networks": [
      {
        "legal_person": "李四",
        "person_id": "person_123",
        "companies": ["company_789", "company_790", "company_791"],
        "network_size": 3
      }
    ]
  },
  "metadata": {
    "execution_time": 5.67
  }
}
```

**返回字段说明**:
- `data.total_companies`: 分析的公司总数
- `data.high_risk_count`: 高嫌疑空壳公司数量
- `data.high_risk_ratio`: 高嫌疑公司占比
- `data.companies`: 公司分析结果列表
  - `company_id`: 公司ID
  - `company_name`: 公司名称
  - `legal_person`: 法人代表
  - `shell_score`: 空壳公司嫌疑分数 (0-1)，越高越可疑
  - `pass_through_ratio`: 资金穿透率 (0-1)，接近1表示资金几乎完全穿透
  - `transaction_velocity_days`: 平均交易速度（天），越小表示交易越快
  - `partner_diversity`: 交易对手多样性 (0-1)，越小表示对手越单一
  - `total_transaction_count`: 总交易次数
  - `total_inflow`: 总流入金额（元）
  - `total_outflow`: 总流出金额（元）
  - `degree_centrality`: 网络中心度（交易对手数量）
  - `legal_person_company_count`: 同一法人的公司数量
  - `contract_count`: 合同数量
- `data.networks`: 空壳公司网络列表（仅当 include_networks=true 时返回）
  - `legal_person`: 共同法人姓名
  - `person_id`: 法人ID
  - `companies`: 该法人关联的高嫌疑公司ID列表
  - `network_size`: 网络规模（公司数量）

---

### 4. 关联方串通网络分析

#### 接口信息

- **路径**: `/api/collusion`
- **方法**: `POST`
- **描述**: 检测关联公司之间的串通行为，识别可疑的串通网络

#### 请求参数

```json
{
  "min_cluster_size": 3,      // 可选，最小集群大小，默认 3
  "min_risk_score": 0.5       // 可选，最低风险分数阈值，默认 0.5
}
```

**参数说明**:
- `min_cluster_size`: 最小集群大小，只返回公司数量 >= min_cluster_size 的网络
- `min_risk_score`: 最低风险分数，只返回风险分数 >= min_risk_score 的网络

#### 请求示例

```bash
curl -X POST "http://localhost:8000/api/collusion" \
  -H "Content-Type: application/json" \
  -d '{
    "min_cluster_size": 3,
    "min_risk_score": 0.5
  }'
```

#### 返回数据结构

```json
{
  "success": true,
  "message": "分析完成",
  "data": {
    "total_networks": 8,
    "networks": [
      {
        "network_id": "NETWORK_1",
        "company_count": 5,
        "companies": ["company_111", "company_112", "company_113", "company_114", "company_115"],
        "company_names": "XX公司A, XX公司B, XX公司C, XX公司D, XX公司E",
        "risk_score": 0.7234,
        "rotation_score": 0.85,
        "amount_similarity": 0.78,
        "threshold_ratio": 0.60,
        "network_density": 0.65,
        "contract_count": 25,
        "total_amount": 50000000.0,
        "avg_amount": 2000000.0
      }
    ]
  },
  "metadata": {
    "execution_time": 4.12
  }
}
```

**返回字段说明**:
- `data.total_networks`: 发现的可疑串通网络总数
- `data.networks`: 可疑串通网络列表
  - `network_id`: 网络ID（格式：NETWORK_N）
  - `company_count`: 网络中的公司数量
  - `companies`: 公司ID列表
  - `company_names`: 公司名称列表（字符串，逗号分隔）
  - `risk_score`: 综合风险分数 (0-1)，越高越可疑
  - `rotation_score`: 轮换分数 (0-1)，衡量中标轮换的均匀程度，越高越可疑
  - `amount_similarity`: 金额相似度 (0-1)，合同金额的相似程度
  - `threshold_ratio`: 卡阈值比例 (0-1)，合同金额刻意卡在审批阈值附近的比例
  - `network_density`: 网络密度 (0-1)，公司之间关联关系的紧密程度
  - `contract_count`: 合同总数
  - `total_amount`: 涉及总金额（元）
  - `avg_amount`: 平均合同金额（元）

---

## 错误响应

所有接口在发生错误时返回统一的错误格式：

```json
{
  "success": false,
  "message": "错误描述信息",
  "error_code": "ERROR_CODE",
  "data": null
}
```

**常见错误码**:
- `INVALID_PARAMETER`: 参数错误
- `DATABASE_ERROR`: 数据库连接或查询错误
- `ANALYSIS_ERROR`: 分析过程错误
- `INTERNAL_ERROR`: 服务器内部错误

**错误响应示例**:

```json
{
  "success": false,
  "message": "参数 time_window_days 必须大于 0",
  "error_code": "INVALID_PARAMETER",
  "data": null
}
```

---

## 注意事项

1. **性能考虑**:
   - 所有分析接口都是计算密集型操作，可能需要较长时间（几秒到几十秒）
   - 建议设置合理的超时时间（建议 60-120 秒）
   - 大数据量情况下，建议使用异步调用或后台任务

2. **参数建议**:
   - `time_window_days`: 建议范围 30-365 天，过大会增加计算时间
   - `amount_threshold`: 根据实际业务调整，建议不低于 10 万
   - `top_n`: 建议不超过 100，避免返回过多数据

3. **数据一致性**:
   - 所有分析基于当前知识图谱数据库的快照
   - 分析结果可能因数据更新而变化
   - 建议对重要分析结果进行缓存

4. **错误处理**:
   - 所有接口都可能因为数据库连接问题而失败
   - 建议实现重试机制
   - 监控接口响应时间和错误率

---

## 版本信息

- **API 版本**: v0.1.0
- **文档更新日期**: 2025-11-18
- **FastAPI 版本**: 0.104.0+

