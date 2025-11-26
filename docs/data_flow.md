# Data Flow Diagram - Contract Graph System

## 系统数据流图

### 完整数据流程

```
┌─────────────────────────────────────────────────────────────────────┐
│                          Data Sources (数据源)                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────────────┐           ┌───────────────────────────┐  │
│  │   mock_data/         │           │  enhanced_mock_data/      │  │
│  │  ├─ t_sec_user_*.csv │           │  ├─ t_sec_user_*.csv      │  │
│  │  ├─ t_org_org_*.csv  │           │  ├─ t_org_org_*.csv       │  │
│  │  ├─ t_bd_supplier_*  │           │  ├─ t_bd_supplier_*       │  │
│  │  ├─ t_bd_customer_*  │           │  ├─ t_bd_customer_*       │  │
│  │  ├─ t_mscon_contract_*│          │  ├─ t_mscon_contract_*    │  │
│  │  ├─ t_mscon_performplanin_*│     │  ├─ t_mscon_performplanin_*│ │
│  │  ├─ t_mscon_performplanout_*│    │  ├─ t_mscon_performplanout_*││
│  │  └─ ...              │           │  └─ ...                   │  │
│  │                      │           │                           │  │
│  │  (原始测试数据)       │           │  (增强业务数据) ⭐         │  │
│  └──────────────────────┘           └───────────────────────────┘  │
│             │                                      │                │
└─────────────┼──────────────────────────────────────┼────────────────┘
              │                                      │
              ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Data Generation (数据生成)                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────────────┐           ┌───────────────────────────┐  │
│  │ generate_graph_data  │           │ generate_enhanced_graph   │  │
│  │      .py             │           │      _data.py             │  │
│  │                      │           │                           │  │
│  │  读取 mock_data      │           │  读取 enhanced_mock_data  │  │
│  │  ↓                   │           │  ↓                        │  │
│  │  解析 CSV 数据       │           │  解析 CSV 数据            │  │
│  │  ↓                   │           │  ↓                        │  │
│  │  构建节点和边        │           │  构建节点和边             │  │
│  │  ↓                   │           │  ↓                        │  │
│  │  输出图格式 CSV      │           │  输出图格式 CSV           │  │
│  └──────────────────────┘           └───────────────────────────┘  │
│             │                                      │                │
└─────────────┼──────────────────────────────────────┼────────────────┘
              │                                      │
              ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Graph Data (图数据)                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────────────┐           ┌───────────────────────────┐  │
│  │   graph_data/        │           │  enhanced_graph_data/     │  │
│  │  ├─ nodes_person.csv │           │  ├─ nodes_person.csv      │  │
│  │  ├─ nodes_company.csv│           │  ├─ nodes_company.csv     │  │
│  │  ├─ nodes_contract.csv│          │  ├─ nodes_contract.csv    │  │
│  │  ├─ nodes_transaction.csv│       │  ├─ nodes_transaction.csv │  │
│  │  ├─ nodes_legal_event.csv│       │  ├─ nodes_legal_event.csv │  │
│  │  │                    │           │  ├─ nodes_admin_penalty.csv│ │
│  │  │                    │           │  ├─ nodes_business_abnormal│ │
│  │  ├─ edges_legal_*    │           │  ├─ edges_legal_*          │  │
│  │  ├─ edges_party.csv  │           │  ├─ edges_party.csv        │  │
│  │  ├─ edges_controls.csv│          │  ├─ edges_controls.csv     │  │
│  │  │                    │           │  ├─ edges_employment.csv   │  │
│  │  │                    │           │  ├─ edges_admin_penalty_*  │  │
│  │  │                    │           │  ├─ edges_business_abnormal│  │
│  │  └─ ...              │           │  └─ ...                    │  │
│  │                      │           │                            │  │
│  │  356 节点, 671 边    │           │  507 节点, 873 边          │  │
│  └──────────────────────┘           └───────────────────────────┘  │
│             │                                      │                │
│             └──────────────┬───────────────────────┘                │
└────────────────────────────┼────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Data Import (数据导入)                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│                   ┌─────────────────────────┐                        │
│                   │   nebula_import.py      │                        │
│                   │                         │                        │
│                   │  --data-dir graph_data  │ ◄─── 默认              │
│                   │  --data-dir enhanced_   │ ◄─── 增强数据          │
│                   │         graph_data      │                        │
│                   │  --data-dir /custom/path│ ◄─── 自定义            │
│                   └─────────────────────────┘                        │
│                              │                                       │
│                              ▼                                       │
│                   ┌─────────────────────────┐                        │
│                   │  1. 连接 Nebula Graph   │                        │
│                   │  2. 创建 Schema          │                        │
│                   │  3. 创建索引             │                        │
│                   │  4. 导入节点             │                        │
│                   │  5. 导入边               │                        │
│                   └─────────────────────────┘                        │
│                              │                                       │
└──────────────────────────────┼───────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Nebula Graph (图数据库)                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │  Graph Space: contract_graph                                │    │
│  │                                                              │    │
│  │  Tags (节点类型):                                            │    │
│  │    • Person           - 人员 (165个)                        │    │
│  │    • Company          - 公司 (114个)                        │    │
│  │    • Contract         - 合同 (100个)                        │    │
│  │    • LegalEvent       - 法律事件 (20个)                     │    │
│  │    • Transaction      - 交易 (60个)                         │    │
│  │    • AdminPenalty     - 行政处罚 (12个) 【新增】            │    │
│  │    • BusinessAbnormal - 经营异常 (27个) 【新增】            │    │
│  │                                                              │    │
│  │  Edges (关系类型):                                           │    │
│  │    • LEGAL_PERSON       - 法人关系 (90条)                   │    │
│  │    • CONTROLS           - 控股关系 (15条)                   │    │
│  │    • PARTY_A/B/C/D      - 合同参与方 (200条)                │    │
│  │    • TRADES_WITH        - 交易关系 (200条)                  │    │
│  │    • IS_SUPPLIER        - 供应商关系 (43条)                 │    │
│  │    • IS_CUSTOMER        - 客户关系 (73条)                   │    │
│  │    • PAYS               - 支付关系 (60条)                   │    │
│  │    • RECEIVES           - 收款关系 (60条)                   │    │
│  │    • INVOLVED_IN        - 参与事件 (10条)                   │    │
│  │    • RELATED_TO         - 关联关系 (20条)                   │    │
│  │    • HAS_PARTY          - 合同参与方反向边 (200条)          │    │
│  │    • EMPLOYED_BY        - 雇佣关系 (80条) 【新增】          │    │
│  │    • ADMIN_PENALTY_OF   - 行政处罚关系 (12条) 【新增】      │    │
│  │    • BUSINESS_ABNORMAL_OF - 经营异常关系 (26条) 【新增】    │    │
│  └────────────────────────────────────────────────────────────┘    │
│                              │                                       │
└──────────────────────────────┼───────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Analysis (分析应用)                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐  │
│  │  FraudRank       │  │  循环交易检测     │  │  空壳公司识别    │  │
│  │  风险传导分析     │  │  Circular Trade  │  │  Shell Company  │  │
│  └──────────────────┘  └──────────────────┘  └─────────────────┘  │
│                                                                       │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐  │
│  │  关联方串通       │  │  履约风险检测     │  │  Web Demo        │  │
│  │  Collusion       │  │  Perform Risk    │  │  可视化系统       │  │
│  └──────────────────┘  └──────────────────┘  └─────────────────┘  │
│                              │                                       │
└──────────────────────────────┼───────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Reports (分析报告)                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  reports/                                                             │
│  ├─ fraud_rank_report.csv                                            │
│  ├─ circular_trade_detection_report.csv                              │
│  ├─ shell_company_detection_report.csv                               │
│  ├─ collusion_network_report.csv                                     │
│  └─ perform_risk_report.csv                                          │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

## 一键导入流程

### 方案一：增强数据一键导入

```
bash src/scripts/import_enhanced_data.sh
         │
         ├─► Step 1: 运行 generate_enhanced_graph_data.py
         │           读取 enhanced_mock_data/
         │           生成 enhanced_graph_data/
         │
         └─► Step 2: 运行 nebula_import.py --data-dir enhanced_graph_data
                     连接 Nebula Graph
                     创建 Schema
                     导入数据
```

### 方案二：原始数据一键导入

```
bash src/scripts/import_graph_data.sh
         │
         ├─► Step 1: 运行 generate_graph_data.py
         │           读取 mock_data/
         │           生成 graph_data/
         │
         └─► Step 2: 运行 nebula_import.py --data-dir graph_data
                     连接 Nebula Graph
                     创建 Schema
                     导入数据
```

## 数据格式转换

### CSV 源数据 → 图数据

```
t_mscon_contract_虚拟数据.csv              nodes_contract.csv
┌────────────────────────────┐           ┌─────────────────────────┐
│ FID, FBILLNO, FBILLNAME,   │           │ node_id, node_type,     │
│ FSIGNALLAMOUNT, FBIZTIME,  │  ──────►  │ contract_no, contract_  │
│ FPARTAID, FPARTANAME, ...  │           │ name, amount, sign_date │
└────────────────────────────┘           └─────────────────────────┘

                                         edges_party.csv
                                         ┌─────────────────────────┐
                                         │ edge_id, edge_type,     │
                                         │ from_node, to_node,     │
                                         │ from_type, to_type,     │
                                         │ properties              │
                                         └─────────────────────────┘
```

## 数据流对比

| 阶段 | graph_data | enhanced_graph_data |
|------|------------|---------------------|
| **数据源** | mock_data | enhanced_mock_data |
| **生成脚本** | generate_graph_data.py | generate_enhanced_graph_data.py |
| **节点数** | 356 | 507 |
| **边数** | 671 | 873 |
| **适用场景** | 开发测试 | 生产演示 |
| **业务覆盖** | 基础场景 | 完整场景（含风险事件） |

## 关键路径

### 快速开发路径（开发测试）
```
mock_data → generate_graph_data.py → graph_data → nebula_import.py → 分析
   1秒              < 1秒              存储        < 10秒          < 1秒
```

### 生产部署路径（完整功能）
```
enhanced_mock_data → generate_enhanced_graph_data.py → enhanced_graph_data → nebula_import.py → 分析
      1秒                     < 1秒                          存储              < 10秒         < 1秒
```

### 一键操作路径（推荐）
```
import_enhanced_data.sh → [自动执行上述步骤] → 完成
        < 15秒
```

## 目录映射

```
contract-graph/
├── data/
│   ├── mock_data/              ──┐
│   │   └─► CSV 源数据            │
│   │                             │  数据生成
│   ├── graph_data/             ◄─┘
│   │   └─► 图格式 CSV            │
│   │                             │
│   ├── enhanced_mock_data/     ──┤
│   │   └─► 增强 CSV 源数据       │
│   │                             │  数据生成
│   └── enhanced_graph_data/    ◄─┘
│       └─► 增强图格式 CSV        │
│           (含风险事件数据)       │
│                                 │
│                                 │  数据导入
│                                 ▼
└── [Nebula Graph]
    └─► contract_graph (Space)
        ├─► Tags (7 types)
        └─► Edges (14 types)
```

## 使用决策树

```
需要导入数据？
    │
    ├─ 是否首次使用？
    │   ├─ 是 → 使用一键脚本
    │   │       └─ import_enhanced_data.sh  (推荐)
    │   │       └─ import_graph_data.sh     (快速测试)
    │   │
    │   └─ 否 → 是否需要自定义？
    │           ├─ 是 → 分步执行
    │           │       └─ 生成 → 导入
    │           │
    │           └─ 否 → 使用一键脚本
    │
    └─ 使用哪个数据源？
        ├─ 开发测试   → graph_data
        ├─ 功能演示   → enhanced_graph_data (推荐)
        ├─ 生产环境   → enhanced_graph_data
        └─ 自定义数据 → 自己的目录
```

## 总结

- **数据源**: 2个（mock_data, enhanced_mock_data）
- **生成脚本**: 2个（生成 graph_data 或 enhanced_graph_data）
- **导入方式**: 3种（一键脚本、分步执行、自定义路径）
- **最终目标**: Nebula Graph 图数据库
- **应用场景**: 5种高级分析 + Web 可视化
  - FraudRank 欺诈风险传导分析
  - 高级循环交易检测（分散-汇聚模式）
  - 空壳公司网络识别
  - 关联方串通网络分析
  - 履约关联风险检测
  - Web 可视化系统

整个数据流程清晰、灵活、可配置，满足不同场景的需求。

## 数据导入说明

### HAS_PARTY 边的自动生成

`HAS_PARTY` 边在数据导入时自动生成，用于支持 FraudRank 风险传导分析。该边从 `PARTY_A/B/C/D` 边反向生成，表示合同到参与方公司的关系。

**生成逻辑**：
- 对于每条 `PARTY_A/B/C/D` 边：`Company -[:PARTY_X]-> Contract`
- 自动生成反向边：`Contract -[:HAS_PARTY]-> Company`
- 边属性继承自原始 PARTY 边

**用途**：
- 在 PageRank 算法中，允许风险从合同节点传导到公司节点
- 支持合同风险向参与方的直接传导路径

### 新增风险事件数据

enhanced_graph_data 新增了以下风险事件数据：

1. **AdminPenalty（行政处罚）**：来自 DaaS 外部数据，记录企业受到的行政处罚
2. **BusinessAbnormal（经营异常）**：来自 DaaS 外部数据，记录企业的经营异常状态
3. **EMPLOYED_BY（雇佣关系）**：记录人员在公司的任职信息，包含职位和任职时间
