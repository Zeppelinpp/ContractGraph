"""
央企穿透式监督知识图谱 API 服务

提供 FraudRank 欺诈风险分析、合同风险子图等功能的 RESTful API
"""

import os
from datetime import datetime
import time
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from src.utils.nebula_utils import get_nebula_session
from src.analysis.fraud_rank import (
    load_weighted_graph,
    initialize_risk_seeds,
    compute_fraud_rank,
    analyze_fraud_rank_results,
    DEFAULT_CONFIG,
)
from src.analysis.contract_risk_subgraph import (
    get_contract_risk_subgraph_with_html,
)
from src.server.models import (
    FraudRankRequest,
    FraudRankParams,
    BaseResponse,
    ContractRiskItem,
    CompanyRiskItem,
    ContractSubGraphRequest,
    ContractSubGraphResponse,
    SubGraphNode,
    SubGraphEdge,
)
from src.config.models import FraudRankConfig

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

app = FastAPI(
    title="央企穿透式监督知识图谱API",
    description="提供 FraudRank 欺诈风险分析、合同风险子图等功能",
    version="1.0.0",
)


# ============================================================================
# FraudRank API
# ============================================================================


@app.post("/api/fraud-rank", response_model=BaseResponse)
async def fraud_rank_analysis(request: FraudRankRequest):
    """
    FraudRank 欺诈风险传导分析

    基于 PageRank 算法，计算企业和合同的欺诈风险传导分数。
    返回公司风险排名和合同风险排名（按风险分数倒序）。
    """
    session = None
    start_time = time.time()
    try:
        session = get_nebula_session()

        # 解析参数：用户配置优先，否则使用默认配置
        params = request.params or FraudRankParams()
        config = FraudRankConfig(
            edge_weights=params.edge_weights,
            event_type_weights=params.event_type_weights,
            event_type_default_weight=params.event_type_default_weight,
            status_weights=params.status_weights,
            status_default_weight=params.status_default_weight,
            amount_threshold=params.amount_threshold,
            init_score_weights=params.init_score_weights,
        )

        # 加载图数据
        graph = load_weighted_graph(
            session,
            force_recompute=params.force_recompute,
            config=config,
            company_ids=request.orgs,
            periods=request.period,
        )

        # 初始化风险种子
        init_scores = initialize_risk_seeds(session, config=config)

        # 计算 FraudRank
        fraud_scores = compute_fraud_rank(graph, init_scores, damping=0.85)

        # 生成分析报告
        report = analyze_fraud_rank_results(
            fraud_scores, session, top_n=params.top_n, company_ids=request.orgs
        )

        company_df = report.get("company_report")
        contract_df = report.get("contract_report")

        # 转换为响应格式
        company_report = []
        if company_df is not None and len(company_df) > 0:
            for _, row in company_df.iterrows():
                company_report.append(
                    CompanyRiskItem(
                        company_id=str(row.get("公司ID", "")),
                        company_name=str(row.get("公司名称", "")),
                        risk_score=float(row.get("风险分数", 0)),
                        risk_level=str(row.get("风险等级", "")),
                        legal_person=str(row.get("法人代表", "")),
                        credit_code=str(row.get("信用代码", "")),
                    )
                )

        contract_report = []
        if contract_df is not None and len(contract_df) > 0:
            for _, row in contract_df.iterrows():
                contract_report.append(
                    ContractRiskItem(
                        contract_id=str(row.get("合同ID", "")),
                        contract_no=str(row.get("合同编号", "")),
                        contract_name=str(row.get("合同名称", "")),
                        risk_score=float(row.get("风险分数", 0)),
                        risk_level=str(row.get("风险等级", "")),
                        amount=float(row.get("签约金额", 0) or 0),
                        sign_date=str(row.get("签订日期", "")),
                        status=str(row.get("合同状态", "")),
                        party_a_id=str(row.get("甲方ID", "")),
                        party_a_name=str(row.get("甲方名称", "")),
                        party_b_id=str(row.get("乙方ID", "")),
                        party_b_name=str(row.get("乙方名称", "")),
                    )
                )

        seed_count = sum(1 for s in init_scores.values() if s > 0)

        return BaseResponse(
            type="fraud_rank",
            count=len(contract_report),
            contract_ids=[contract.contract_id for contract in contract_report],
            details={
                "contract_list": contract_report,
                "metadata": {
                    "node_count": len(graph["nodes"]),
                    "edge_count": sum(len(v) for v in graph["edges"].values()),
                    "seed_count": seed_count,
                    "company_count": len(company_report),
                    "contract_count": len(contract_report),
                    "timestamp": datetime.now().isoformat(),
                    "execution_time": round(time.time() - start_time, 2),
                },
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if session:
            session.release()


# ============================================================================
# 合同风险子图 API
# ============================================================================


@app.post("/api/contract-risk/subgraph", response_model=ContractSubGraphResponse)
async def get_contract_subgraph(request: ContractSubGraphRequest):
    """
    获取合同风险子图

    以合同为起点，递归获取关联的法律事件、相对方公司、
    以及这些公司涉及的其他有法律事件的合同，生成交互式HTML页面。

    返回的 html_url 可直接嵌入前端 iframe 中。
    """
    session = None
    try:
        session = get_nebula_session()

        result = get_contract_risk_subgraph_with_html(
            contract_id=request.contract_id,
            max_depth=request.max_depth,
            session=session,
        )

        subgraph = result["subgraph"]
        html_path = result["html_url"]

        # 生成可访问的URL（相对路径）
        html_filename = os.path.basename(html_path)
        html_url = f"/api/contract-risk/view/{html_filename}"

        return ContractSubGraphResponse(
            success=True,
            contract_id=request.contract_id,
            max_depth=request.max_depth,
            html_url=html_url,
            node_count=len(subgraph["nodes"]),
            edge_count=len(subgraph["edges"]),
            nodes=[SubGraphNode(**n) for n in subgraph["nodes"]],
            edges=[SubGraphEdge(**e) for e in subgraph["edges"]],
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if session:
            session.release()


@app.get("/api/contract-risk/subgraph/{contract_id}")
async def get_contract_subgraph_by_id(
    contract_id: str,
    max_depth: int = Query(default=3, ge=1, le=5, description="递归深度"),
):
    """
    GET 方式获取合同风险子图（便于前端直接调用）
    """
    request = ContractSubGraphRequest(contract_id=contract_id, max_depth=max_depth)
    return await get_contract_subgraph(request)


@app.get("/api/contract-risk/view/{filename}")
async def view_contract_risk_html(filename: str):
    """
    提供合同风险子图 HTML 文件访问

    前端可通过 iframe 嵌入此 URL 展示交互式图谱
    """
    file_path = os.path.join(REPORTS_DIR, filename)

    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="text/html")
    else:
        raise HTTPException(status_code=404, detail="File not found")


# ============================================================================
# 健康检查
# ============================================================================


@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok"}


@app.get("/")
async def root():
    """API 根路径"""
    return {
        "name": "央企穿透式监督知识图谱API",
        "version": "1.0.0",
        "endpoints": {
            "fraud_rank": "POST /api/fraud-rank",
            "contract_subgraph": "POST /api/contract-risk/subgraph",
            "contract_subgraph_get": "GET /api/contract-risk/subgraph/{contract_id}",
            "view_html": "GET /api/contract-risk/view/{filename}",
        },
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
