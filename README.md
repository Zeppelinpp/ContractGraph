# 央企穿透式监督知识图谱 - 高级分析系统

## 项目概述

本项目是一个基于知识图谱的央企穿透式监督系统，包含合同、交易、法律事件等多维度数据，支持四个高级分析场景：

1. **FraudRank 欺诈风险传导分析** - 基于 PageRank 算法识别风险传导路径
2. **高级循环交易检测** - 检测分散-汇聚模式的循环交易
3. **空壳公司网络识别** - 识别具有空壳公司特征的企业网络
4. **关联方串通网络分析** - 检测轮流中标、围标等串通行为

## 快速开始

### 1. 安装依赖

```bash
uv sync
```

### 2. 配置环境变量

创建 `.env` 文件：

```bash
# Nebula Graph 配置
NEBULA_HOST=172.18.53.63
NEBULA_PORT=9669
NEBULA_USERNAME=root
NEBULA_PASSWORD=nebula
NEBULA_SPACE=contract_1117
```

### 3. 导入数据

```bash
# 导入数据到 Nebula Graph
uv run python src/nebula_import.py
```

### 4. 运行分析

```bash
# 运行所有分析场景
uv run python src/analysis/run_all_analysis.py

# 或单独运行某个场景
uv run python src/analysis/fraud_rank.py
uv run python src/analysis/circular_trade.py
uv run python src/analysis/shell_company.py
uv run python src/analysis/collusion.py
```

### 5. 查看报告

所有分析报告保存在 `reports/` 目录：
- `fraud_rank_report.csv` - FraudRank 风险分析报告
- `circular_trade_detection_report.csv` - 循环交易检测报告
- `shell_company_detection_report.csv` - 空壳公司识别报告
- `collusion_network_report.csv` - 串通网络分析报告

## 项目结构

```
contract-graph/
├── src/
│   ├── analysis/                      # 高级分析模块
│   │   ├── fraud_rank.py             # FraudRank 分析
│   │   ├── circular_trade.py         # 循环交易检测
│   │   ├── shell_company.py          # 空壳公司识别
│   │   ├── collusion.py              # 串通网络分析
│   │   └── run_all_analysis.py       # 运行所有分析
│   ├── scripts/                       # 数据生成脚本
│   │   ├── analyze_scenario_support.py      # 场景支持分析
│   │   └── generate_advanced_scenario_data.py # 生成增强数据
│   ├── nebula_import.py              # Nebula Graph 导入
│   ├── settings.py                   # 配置管理
│   └── web_demo.py                   # Web 演示
├── data/
│   ├── graph_data/                   # CSV 图数据
│   └── mock_data/                    # 原始 mock 数据
├── reports/                          # 分析报告输出目录
├── docs/                             # 文档
│   ├── 知识图谱高级分析场景实施方案.md
│   ├── 高级分析场景使用说明.md
│   ├── 场景数据支持分析报告.md
│   └── graph_schema.md
└── tests/                            # 测试用例
```

## 核心功能

### 1. FraudRank 欺诈风险传导分析

**原理**：类似 PageRank，传播的是"欺诈嫌疑分数"而非"权威分数"

**应用场景**：
- 合同审批流程风险评估
- 供应商准入评估
- 定期风险扫描

**关键指标**：
- 风险分数（0-1）
- 风险等级（高/中/低/正常）
- 风险传导路径

### 2. 高级循环交易检测

**原理**：检测资金从核心公司分散到多个中转站，经过复杂交易后汇聚回核心公司或关联方的模式

**应用场景**：
- 异常交易监控
- 虚假交易识别
- 资金流向分析

**关键指标**：
- 流入流出金额相似度
- 分散节点数量
- 中间交易密度
- 时间窗口

### 3. 空壳公司网络识别

**原理**：基于资金穿透率、交易速度、交易对手多样性等特征识别空壳公司

**应用场景**：
- 供应商准入审核
- 虚开发票识别
- 资金中转洗钱检测

**关键指标**：
- 资金穿透率（>0.9 高嫌疑）
- 交易速度（<7天 高嫌疑）
- 交易对手多样性（<0.3 高嫌疑）
- 法人公司数量（>=5 高嫌疑）

### 4. 关联方串通网络分析

**原理**：通过社区检测算法识别共享法人或控股关系的公司集群，分析轮流中标模式

**应用场景**：
- 招投标审核
- 围标/陪标检测
- 利益输送识别

**关键指标**：
- 轮换分数（>0.8 高嫌疑）
- 合同金额相似度
- 卡阈值比例（>40% 高嫌疑）
- 网络密度

## 技术栈

- **图数据库**: Nebula Graph 3.x
- **Python**: 3.12+
- **数据处理**: pandas, csv
- **图算法**: 自实现 PageRank、社区检测
- **Web 框架**: Flask 3.0.0+
- **进度显示**: tqdm

## 数据统计

### 节点统计
- Person（人员）: 80 个
- Company（公司）: 97 个
- Contract（合同）: 109 个
- LegalEvent（法律事件）: 25 个
- Transaction（交易）: 79 个
- **总计**: 390 个节点

### 边统计
- LEGAL_PERSON（法人关系）: 97 条
- CONTROLS（控股关系）: 15 条
- PARTY_A/B（合同签署）: 218 条
- TRADES_WITH（交易关系）: 100 条
- IS_SUPPLIER/IS_CUSTOMER: 116 条
- PAYS/RECEIVES（支付/收款）: 158 条
- RELATED_TO（关联法律事件）: 25 条
- **总计**: 729 条边

## 分析结果示例

### FraudRank 分析结果
```
前 10 高风险公司：
SUP_017  远大建筑材料   0.1199  正常
CUS_023  荣盛发展      0.1177  正常
CUS_025  富力地产      0.1168  正常
...
```

### 循环交易检测结果
```
发现可疑模式数: 9

模式 #1
  核心公司: ORG_002
  流出金额: ¥51,085,269.00
  流入金额: ¥65,492,615.50
  相似度: 78.00%
  风险分数: 0.7920
```

### 空壳公司识别结果
```
高嫌疑空壳公司数量: 7 (7.2%)

SUP_025  金鼎建筑器材    0.70  穿透率=1.0  多样性=0.25
SHELL_094 空壳公司4-刘杰 0.65  穿透率=0.95 法人公司数=8
```

### 串通网络分析结果
```
发现可疑串通网络数: 2

NETWORK_1: 9家公司
  风险分数: 0.6639
  轮换分数: 0.9053
  卡阈值比例: 46.15%
  涉及金额: ¥61,910,260.79
```

## 文档

- [知识图谱高级分析场景实施方案](docs/知识图谱高级分析场景实施方案.md) - 详细算法原理和实施步骤
- [高级分析场景使用说明](docs/高级分析场景使用说明.md) - 使用指南和参数调整
- [场景数据支持分析报告](docs/场景数据支持分析报告.md) - 数据支持情况分析
- [图谱 Schema 设计](docs/graph_schema.md) - 完整的图谱结构设计

## 性能指标

- **分析速度**: 约 0.5 秒完成所有四个场景分析（97家公司）
- **内存占用**: < 200MB
- **报告生成**: 实时生成 CSV 报告

## 扩展开发

### 添加新的分析场景

1. 在 `src/analysis/` 创建新的分析脚本
2. 实现 `main()` 函数
3. 在 `run_all_analysis.py` 中注册

### 集成到 Web 系统

```python
from flask import Flask, jsonify
from analysis.fraud_rank import compute_fraud_rank

app = Flask(__name__)

@app.route('/api/fraud-rank/<company_id>')
def get_fraud_rank(company_id):
    score = compute_fraud_rank(company_id)
    return jsonify({'company_id': company_id, 'score': score})
```

### 定时任务

使用 cron 每天自动运行分析：
```bash
0 2 * * * cd /path/to/contract-graph && uv run python src/analysis/run_all_analysis.py
```

## 许可证

本项目仅用于学习和研究目的。

## 联系方式

如有问题或建议，请查看项目文档或提交 Issue。
