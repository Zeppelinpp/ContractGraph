# 知识图谱 Schema 设计文档

## 概述

本文档描述了央企穿透式监督知识图谱的完整Schema设计，包括所有节点类型（Tag）、边类型（Edge）及其属性。

---

## 节点类型 (Tags)

### 1. Person（人员节点）

**用途**: 表示人员实体，包括企业法人代表、案件经办人等

**属性**:
| 属性名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| name | string | 姓名 | 张伟 |
| number | string | 工号 | EMP0001 |
| id_card | string | 证件号码 | 310101199001011234 |
| gender | string | 性别 | 1:男 2:女 |
| birthday | string | 出生日期 | 1990-01-01 |
| status | string | 数据状态 | C:已审核 |
| email | string | 邮箱 | emp0001@cnbuild-mail.com |
| phone | string | 电话 | 13962000001 |

**数据来源**: t_sec_user_虚拟数据.csv

**节点数量**: 80个

**Schema定义**:
```ngql
CREATE TAG IF NOT EXISTS Person (
    name string,
    number string,
    id_card string,
    gender string,
    birthday string,
    status string,
    email string,
    phone string
);
```

---

### 2. Company（公司节点）

**用途**: 表示企业实体，包括央企组织、供应商、客户、相对方等

**属性**:
| 属性名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| name | string | 公司名称 | 中央建设集团有限公司 |
| number | string | 编码 | ORG001 |
| legal_person | string | 法人代表 | 张伟 |
| credit_code | string | 统一社会信用代码 | 91110000MA001234XX |
| establish_date | string | 成立日期 | 2010-05-20 |
| status | string | 数据状态 | C:已审核 |
| description | string | 描述 | 公司详细描述 |

**注意**: 已移除 `company_type` 属性，公司类型通过关系边来表达

**数据来源**: 
- t_org_org_虚拟数据.csv (组织)
- t_bd_supplier_虚拟数据.csv (供应商)
- t_bd_customer_虚拟数据.csv (客户)
- t_mscon_counterpart_虚拟数据.csv (相对方)

**节点数量**: 92个

**Schema定义**:
```ngql
CREATE TAG IF NOT EXISTS Company (
    name string,
    number string,
    legal_person string,
    credit_code string,
    establish_date string,
    status string,
    description string
);
```

---

### 3. Contract（合同节点）

**用途**: 表示合同实体，连接交易双方

**属性**:
| 属性名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| contract_no | string | 合同编号 | HT2024000001 |
| contract_name | string | 合同名称 | 建材采购合同-华信建材有限公司 |
| amount | double | 签约金额 | 5353380.00 |
| sign_date | string | 签订日期 | 2025-09-18 |
| status | string | 合同状态 | EXECUTING:执行中 COMPLETED:已完成 |
| description | string | 描述 | 合同详细描述 |

**数据来源**: t_mscon_contract_虚拟数据.csv

**节点数量**: 102个

**Schema定义**:
```ngql
CREATE TAG IF NOT EXISTS Contract (
    contract_no string,
    contract_name string,
    amount double,
    sign_date string,
    status string,
    description string
);
```

---

### 4. LegalEvent（法律事件节点）

**用途**: 表示法律事件实体，包括案件、纠纷等

**属性**:
| 属性名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| event_type | string | 事件类型 | Case:案件 Dispute:纠纷 |
| event_no | string | 事件编号 | AJ202400001 |
| event_name | string | 事件名称 | 合同纠纷案件-1 |
| amount | double | 涉及金额 | 1500000.00 |
| status | string | 事件状态 | F:已立案 I:一审 N:已结案 J:执行 |
| register_date | string | 登记日期 | 2024-03-15 |
| description | string | 描述 | 事件详细描述 |

**数据来源**: 
- t_conl_case_虚拟数据.csv (案件)
- t_conl_disputeregist_虚拟数据.csv (纠纷)

**节点数量**: 22个

**Schema定义**:
```ngql
CREATE TAG IF NOT EXISTS LegalEvent (
    event_type string,
    event_no string,
    event_name string,
    amount double,
    status string,
    register_date string,
    description string
);
```

---

### 5. Transaction（交易节点）【新增】

**用途**: 表示交易流水实体，记录公司间的实际资金流动

**属性**:
| 属性名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| transaction_type | string | 交易类型 | INFLOW:流入 OUTFLOW:流出 |
| transaction_no | string | 交易单号 | IN2024000001 |
| contract_no | string | 关联合同号 | HT2024000001 |
| amount | double | 交易金额 | 2500000.00 |
| transaction_date | string | 交易日期 | 2025-09-20 |
| status | string | 状态 | C:已完成 |
| description | string | 描述 | 建材采购首期款项流入 |
| fpaidamount | double | 已履约金额 | 1898679.00 |
| ftotalamount | double | 应履约金额 | 2675136.00 |
| fbiztimeend | string | 履约截止日期 | 2026-04-02 |
| fperformstatus | string | 履约状态 | A:待履约 B:履约中 C:已履约 |

**数据来源**: 
- t_mscon_performplanin_虚拟数据.csv (流入)
- t_mscon_performplanout_虚拟数据.csv (流出)

**节点数量**: 60个 (30个INFLOW + 30个OUTFLOW)

**Schema定义**:
```ngql
CREATE TAG IF NOT EXISTS Transaction (
    transaction_type string,
    transaction_no string,
    contract_no string,
    amount double,
    transaction_date string,
    status string,
    description string,
    fpaidamount double,
    ftotalamount double,
    fbiztimeend string,
    fperformstatus string
);
```

---

## 边类型 (Edges)

### 1. LEGAL_PERSON（法人代表关系）

**方向**: Person → Company

**用途**: 表示人员担任公司法人代表的关系

**属性**:
| 属性名 | 类型 | 说明 |
|--------|------|------|
| properties | string | 边属性描述 |

**数据来源**: 各公司表的FARTIFICIALPERSON字段

**边数量**: 90条

**Schema定义**:
```ngql
CREATE EDGE IF NOT EXISTS LEGAL_PERSON (
    properties string
);
```

**示例**:
```ngql
-- 张伟 担任 中央建设集团有限公司 的法人代表
USER_001 -[:LEGAL_PERSON]-> ORG_001
```

---

### 2. CONTROLS（控股关系）

**方向**: Company → Company

**用途**: 表示公司之间的控股关系

**属性**:
| 属性名 | 类型 | 说明 |
|--------|------|------|
| properties | string | 边属性描述 |

**数据来源**: t_org_org的FPARENTORGID字段

**边数量**: 15条

**Schema定义**:
```ngql
CREATE EDGE IF NOT EXISTS CONTROLS (
    properties string
);
```

**示例**:
```ngql
-- 中央建设集团 控股 中建华东分公司
ORG_001 -[:CONTROLS]-> ORG_002
```

---

### 3. PARTY_A / PARTY_B / PARTY_C / PARTY_D（合同签署关系）

**方向**: Company → Contract

**用途**: 表示公司作为合同甲方/乙方/丙方/丁方签署合同

**属性**:
| 属性名 | 类型 | 说明 |
|--------|------|------|
| properties | string | 边属性描述，包含角色信息 |

**数据来源**: t_mscon_contract的FPARTAID/FPARTBID/FPARTCID/FPARTDID字段

**边数量**: 200条 (PARTY_A: 100条, PARTY_B: 100条)

**Schema定义**:
```ngql
CREATE EDGE IF NOT EXISTS PARTY_A (properties string);
CREATE EDGE IF NOT EXISTS PARTY_B (properties string);
CREATE EDGE IF NOT EXISTS PARTY_C (properties string);
CREATE EDGE IF NOT EXISTS PARTY_D (properties string);
```

**示例**:
```ngql
-- 中建华东分公司 作为甲方签署 建材采购合同
ORG_002 -[:PARTY_A]-> CON_001
-- 华信建材有限公司 作为乙方签署 建材采购合同
SUP_001 -[:PARTY_B]-> CON_001
```

---

### 4. TRADES_WITH（交易关系）

**方向**: Company → Company

**用途**: 表示公司间的直接交易关系（从合同推导）

**属性**:
| 属性名 | 类型 | 说明 |
|--------|------|------|
| properties | string | 交易信息，包含金额和合同号 |

**数据来源**: 从合同表推导（甲方 → 乙方）

**边数量**: 100条

**Schema定义**:
```ngql
CREATE EDGE IF NOT EXISTS TRADES_WITH (
    properties string
);
```

**示例**:
```ngql
-- 中建华东分公司 与 华信建材有限公司 有交易
ORG_002 -[:TRADES_WITH]-> SUP_001
properties: "交易金额:5353380.00,合同:HT2024000001"
```

---

### 5. INVOLVED_IN（涉及法律事件）

**方向**: Person → LegalEvent

**用途**: 表示人员涉及法律事件（如案件经办人）

**属性**:
| 属性名 | 类型 | 说明 |
|--------|------|------|
| properties | string | 边属性描述 |

**数据来源**: t_conl_case的FOPERATORID字段

**边数量**: 10条

**Schema定义**:
```ngql
CREATE EDGE IF NOT EXISTS INVOLVED_IN (
    properties string
);
```

**示例**:
```ngql
-- 张伟 涉及 合同纠纷案件-1
USER_001 -[:INVOLVED_IN]-> CASE_001
```

---

### 6. RELATED_TO（关联法律事件）

**方向**: Contract → LegalEvent

**用途**: 表示合同关联法律事件

**属性**:
| 属性名 | 类型 | 说明 |
|--------|------|------|
| properties | string | 边属性描述 |

**数据来源**: 
- t_conl_case的FRELATECONTRACTID字段
- t_conl_disputeregist的FRELATECONTRACTID字段

**边数量**: 20条 (案件10条 + 纠纷10条)

**Schema定义**:
```ngql
CREATE EDGE IF NOT EXISTS RELATED_TO (
    properties string
);
```

**示例**:
```ngql
-- 建材采购合同 关联 合同纠纷案件-1
CON_001 -[:RELATED_TO]-> CASE_001
```

---

### 7. IS_SUPPLIER（供应商关系）【新增】

**方向**: Company → Company

**用途**: 表示一个公司是另一个公司的供应商

**属性**:
| 属性名 | 类型 | 说明 |
|--------|------|------|
| properties | string | 供应商关系描述 |

**生成逻辑**: 从合同表中，若乙方类型为bd_supplier，则乙方是甲方的供应商

**边数量**: 43条

**Schema定义**:
```ngql
CREATE EDGE IF NOT EXISTS IS_SUPPLIER (
    properties string
);
```

**示例**:
```ngql
-- 华信建材有限公司 是 中建华东分公司 的供应商
SUP_001 -[:IS_SUPPLIER]-> ORG_002
properties: "供应商关系-华信建材有限公司为中建华东分公司提供产品/服务"
```

---

### 8. IS_CUSTOMER（客户关系）【新增】

**方向**: Company → Company

**用途**: 表示一个公司是另一个公司的客户

**属性**:
| 属性名 | 类型 | 说明 |
|--------|------|------|
| properties | string | 客户关系描述 |

**生成逻辑**: 从合同表中，若某方类型为bd_customer，则该方是对方的客户

**边数量**: 73条

**Schema定义**:
```ngql
CREATE EDGE IF NOT EXISTS IS_CUSTOMER (
    properties string
);
```

**示例**:
```ngql
-- 华润置地发展 是 中天建筑材料 的客户
CUS_001 -[:IS_CUSTOMER]-> SUP_011
properties: "客户关系-华润置地发展是中天建筑材料的客户"
```

---

### 9. PAYS（支付关系）【新增】

**方向**: Company → Transaction

**用途**: 表示公司发起支付到交易

**属性**:
| 属性名 | 类型 | 说明 |
|--------|------|------|
| properties | string | 支付描述 |

**数据来源**: 
- INFLOW: 乙方付款（from_node = party_b, to_node = transaction）
- OUTFLOW: 甲方付款（from_node = party_a, to_node = transaction）

**边数量**: 60条

**Schema定义**:
```ngql
CREATE EDGE IF NOT EXISTS PAYS (
    properties string
);
```

**示例**:
```ngql
-- 华信建材有限公司 支付款项
SUP_001 -[:PAYS]-> TXN_OUT_0001
properties: "付款-中建华东分公司向华信建材有限公司支付"
```

---

### 10. RECEIVES（收款关系）【新增】

**方向**: Transaction → Company

**用途**: 表示交易的收款方

**属性**:
| 属性名 | 类型 | 说明 |
|--------|------|------|
| properties | string | 收款描述 |

**数据来源**: 
- INFLOW: 甲方收款（from_node = transaction, to_node = party_a）
- OUTFLOW: 乙方收款（from_node = transaction, to_node = party_b）

**边数量**: 60条

**Schema定义**:
```ngql
CREATE EDGE IF NOT EXISTS RECEIVES (
    properties string
);
```

**示例**:
```ngql
-- 中建华东分公司 收到款项
TXN_IN_0001 -[:RECEIVES]-> ORG_002
properties: "收款-中建华东分公司收到华信建材有限公司付款"
```

---

## 完整Schema创建脚本

```ngql
-- 创建图空间
CREATE SPACE IF NOT EXISTS contract_1117 (
    vid_type = FIXED_STRING(64),
    partition_num = 10,
    replica_factor = 1
);

USE contract_1117;

-- 创建节点Tag
CREATE TAG IF NOT EXISTS Person (
    name string,
    number string,
    id_card string,
    gender string,
    birthday string,
    status string,
    email string,
    phone string
);

CREATE TAG IF NOT EXISTS Company (
    name string,
    number string,
    legal_person string,
    credit_code string,
    establish_date string,
    status string,
    description string
);

CREATE TAG IF NOT EXISTS Contract (
    contract_no string,
    contract_name string,
    amount double,
    sign_date string,
    status string,
    description string
);

CREATE TAG IF NOT EXISTS LegalEvent (
    event_type string,
    event_no string,
    event_name string,
    amount double,
    status string,
    register_date string,
    description string
);

CREATE TAG IF NOT EXISTS Transaction (
    transaction_type string,
    transaction_no string,
    contract_no string,
    amount double,
    transaction_date string,
    status string,
    description string,
    fpaidamount double,
    ftotalamount double,
    fbiztimeend string,
    fperformstatus string
);

-- 创建边Edge
CREATE EDGE IF NOT EXISTS LEGAL_PERSON (properties string);
CREATE EDGE IF NOT EXISTS CONTROLS (properties string);
CREATE EDGE IF NOT EXISTS PARTY_A (properties string);
CREATE EDGE IF NOT EXISTS PARTY_B (properties string);
CREATE EDGE IF NOT EXISTS PARTY_C (properties string);
CREATE EDGE IF NOT EXISTS PARTY_D (properties string);
CREATE EDGE IF NOT EXISTS TRADES_WITH (properties string);
CREATE EDGE IF NOT EXISTS INVOLVED_IN (properties string);
CREATE EDGE IF NOT EXISTS RELATED_TO (properties string);
CREATE EDGE IF NOT EXISTS IS_SUPPLIER (properties string);
CREATE EDGE IF NOT EXISTS IS_CUSTOMER (properties string);
CREATE EDGE IF NOT EXISTS PAYS (properties string);
CREATE EDGE IF NOT EXISTS RECEIVES (properties string);
```

---

## 图谱统计信息

### 节点统计
| 节点类型 | 数量 | 说明 |
|---------|------|------|
| Person | 80 | 人员节点 |
| Company | 92 | 公司节点 |
| Contract | 102 | 合同节点 |
| LegalEvent | 22 | 法律事件节点 |
| Transaction | 60 | 交易节点 |
| **总计** | **356** | |

### 边统计
| 边类型 | 数量 | 说明 |
|--------|------|------|
| LEGAL_PERSON | 90 | 法人代表关系 |
| CONTROLS | 15 | 控股关系 |
| PARTY_A | 100 | 合同甲方 |
| PARTY_B | 100 | 合同乙方 |
| PARTY_C | 0 | 合同丙方 |
| PARTY_D | 0 | 合同丁方 |
| TRADES_WITH | 100 | 交易关系 |
| INVOLVED_IN | 10 | 涉及法律事件 |
| RELATED_TO | 20 | 关联法律事件 |
| IS_SUPPLIER | 43 | 供应商关系 |
| IS_CUSTOMER | 73 | 客户关系 |
| PAYS | 60 | 支付关系 |
| RECEIVES | 60 | 收款关系 |
| **总计** | **671** | |

---

## 业务场景支持

### 1. 法律事件风险传导
**路径**: Person → LEGAL_PERSON → Company → CONTROLS → Company → PARTY_A/B → Contract → PARTY_A/B → Company

**用途**: 追踪法人代表涉及法律事件后，风险如何通过控股关系和合同关系传导到交易对手

### 2. 循环交易检测
**路径**: Company → TRADES_WITH → Company → TRADES_WITH → Company → ... → Company (形成闭环)

**用途**: 检测公司间的循环交易模式，计算金额相似度识别可疑交易

### 3. 供应链关系分析
**路径**: Company → IS_SUPPLIER/IS_CUSTOMER → Company

**用途**: 分析企业的供应商和客户网络，识别供应链风险

### 4. 资金流向追踪
**路径**: Company → PAYS → Transaction → RECEIVES → Company

**用途**: 追踪实际资金流向，验证合同履约情况

---

## Schema版本历史

### v2.0 (当前版本)
- 移除Company.company_type属性
- 新增Transaction节点类型
- 新增IS_SUPPLIER、IS_CUSTOMER关系边
- 新增PAYS、RECEIVES关系边
- 通过关系边表达公司类型，而非节点属性

### v1.0
- 初始版本
- 包含Person、Company、Contract、LegalEvent节点
- Company包含company_type属性
- 基础关系边：LEGAL_PERSON、CONTROLS、PARTY_*、TRADES_WITH、INVOLVED_IN、RELATED_TO

---

## 数据文件清单

### 节点文件
- `nodes_person.csv` (80行)
- `nodes_company.csv` (92行)
- `nodes_contract.csv` (102行)
- `nodes_legal_event.csv` (22行)
- `nodes_transaction.csv` (60行)

### 边文件
- `edges_legal_person.csv` (90行)
- `edges_controls.csv` (15行)
- `edges_party.csv` (200行)
- `edges_trades_with.csv` (100行)
- `edges_case_person.csv` (10行)
- `edges_case_contract.csv` (10行)
- `edges_dispute_contract.csv` (10行)
- `edges_is_supplier.csv` (43行)
- `edges_is_customer.csv` (73行)
- `edges_company_transaction.csv` (120行)

---

## 数据导入

使用提供的导入脚本：

```bash
# 设置环境变量
export NEBULA_ADDRESS="172.18.53.63:9669"
export NEBULA_USERNAME="root"
export NEBULA_PASSWORD="nebula"
export NEBULA_SPACE="contract_1117"

# 运行导入脚本
uv run python src/nebula_import.py
```

---

## 相关文档

- [知识图谱设计方案](./03_知识图谱设计.md)
- [字段筛选说明](./02_字段筛选说明.md)
- [Web Demo使用说明](./WebDemo使用说明.md)
- [原型系统说明文档](./原型系统说明文档.md)

