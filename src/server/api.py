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
from src.analysis.circular_trade import (
    detect_fan_out_fan_in,
    detect_circular_trade_by_contract,
)
from src.analysis.collusion import (
    detect_collusion_network,
    detect_collusion_by_contract,
)
from src.analysis.perform_risk import (
    analyze_perform_risk,
    get_perform_risk_subgraph,
)
from src.analysis.external_risk_rank import (
    load_weighted_graph as load_external_risk_graph,
    initialize_external_risk_seeds,
    compute_external_risk_rank,
    analyze_external_risk_results,
    get_external_risk_subgraph,
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
    CircularTradeRequest,
    CircularTradeParams,
    CircularTradePattern,
    CircularTradeSubGraphRequest,
    CircularTradeSubGraphResponse,
    PerformRiskRequest,
    PerformRiskParams,
    PerformRiskCompanyItem,
    PerformRiskSubGraphRequest,
    PerformRiskSubGraphResponse,
    ExternalRiskRankRequest,
    ExternalRiskRankParams,
    ExternalRiskCompanyItem,
    ExternalRiskRankSubGraphRequest,
    ExternalRiskRankSubGraphResponse,
    CollusionRequest,
    CollusionParams,
    CollusionNetworkItem,
    CollusionSubGraphRequest,
    CollusionSubGraphResponse,
)
from src.config.models import FraudRankConfig, PerformRiskConfig, ExternalRiskRankConfig, CollusionConfig
from src.utils.embedding import load_edge_weights

BASE_DIR = os.path.join(os.path.dirname(__file__), "../..")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
CACHE_DIR = os.path.join(BASE_DIR, "cache")

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


@app.post("/api/contract-risk/subgraph")
async def get_contract_subgraph(request: ContractSubGraphRequest):
    """
    获取合同风险子图

    以合同为起点，递归获取关联的法律事件、相对方公司、
    以及这些公司涉及的其他有法律事件的合同，生成交互式HTML页面。

    直接返回 HTML 文件，可嵌入前端 iframe 中。
    """
    session = None
    try:
        session = get_nebula_session()

        result = get_contract_risk_subgraph_with_html(
            contract_id=request.contract_id,
            max_depth=request.max_depth,
            session=session,
        )

        html_path = result["html_url"]

        if not os.path.exists(html_path):
            raise HTTPException(status_code=404, detail="HTML文件生成失败")

        return FileResponse(html_path, media_type="text/html")

    except HTTPException:
        raise
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
# 循环交易检测 API
# ============================================================================


@app.post("/api/circular-trade", response_model=BaseResponse)
async def circular_trade_detection(request: CircularTradeRequest):
    """
    循环交易检测 - 分散汇聚模式分析

    检测复杂的循环交易模式，包括分散-汇聚模式。
    返回可疑的循环交易模式列表（按风险分数倒序），包含涉及的合同ID。
    """
    session = None
    start_time = time.time()
    try:
        session = get_nebula_session()

        params = request.params or CircularTradeParams()

        suspicious_patterns = detect_fan_out_fan_in(
            session=session,
            time_window_days=params.time_window_days,
            amount_threshold=params.amount_threshold,
            company_ids=request.orgs,
            periods=request.period,
        )

        pattern_list = [
            CircularTradePattern(
                central_company=p["central_company"],
                central_company_name=p.get("central_company_name", ""),
                dispersed_companies=p["dispersed_companies"],
                related_companies=p["related_companies"],
                total_outflow=p["total_outflow"],
                total_inflow=p["total_inflow"],
                similarity=p["similarity"],
                inter_trade_count=p["inter_trade_count"],
                time_span_days=p["time_span_days"],
                risk_score=p["risk_score"],
                transaction_ids=p.get("transaction_ids", []),
                contract_ids=p.get("contract_ids", []),
            )
            for p in suspicious_patterns
        ]

        # Collect all contract IDs from all patterns
        all_contract_ids = list(set(
            cid for p in pattern_list for cid in p.contract_ids
        ))

        return BaseResponse(
            type="circular_trade",
            count=len(pattern_list),
            contract_ids=all_contract_ids,
            details={
                "pattern_list": [p.model_dump() for p in pattern_list],
                "metadata": {
                    "pattern_count": len(pattern_list),
                    "contract_count": len(all_contract_ids),
                    "time_window_days": params.time_window_days,
                    "amount_threshold": params.amount_threshold,
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


@app.post("/api/circular-trade/subgraph")
async def get_circular_trade_subgraph(request: CircularTradeSubGraphRequest):
    """
    获取合同关联的循环交易模式子图

    以合同ID为入口，查找合同的甲/乙方公司，
    以这些公司为核心检测循环交易模式，并生成交互式HTML页面。
    直接返回 HTML 文件，可嵌入前端 iframe 中。
    """
    session = None
    try:
        session = get_nebula_session()

        result = detect_circular_trade_by_contract(
            session=session,
            contract_id=request.contract_id,
            time_window_days=request.time_window_days,
            amount_threshold=request.amount_threshold,
        )

        if not result.get("html_url"):
            raise HTTPException(
                status_code=404,
                detail=result.get("message", "未检测到循环交易模式")
            )

        html_path = result["html_url"]

        if not os.path.exists(html_path):
            raise HTTPException(status_code=404, detail="HTML文件生成失败")

        return FileResponse(html_path, media_type="text/html")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if session:
            session.release()


@app.get("/api/circular-trade/view/{filename}")
async def view_circular_trade_html(filename: str):
    """
    提供循环交易模式 HTML 文件访问

    前端可通过 iframe 嵌入此 URL 展示交互式图谱
    """
    file_path = os.path.join(REPORTS_DIR, filename)

    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="text/html")
    else:
        raise HTTPException(status_code=404, detail="File not found")


# ============================================================================
# 履约关联风险检测 API
# ============================================================================


@app.post("/api/perform-risk", response_model=BaseResponse)
async def perform_risk_analysis(request: PerformRiskRequest):
    """
    履约关联风险检测

    根据相对方获取签署履约状态的合同存在收款逾期或交货逾期的，
    排查并列出同一相对方或存在相关关系的相对方的相同标的名称的其他合同。
    返回公司风险排名和风险合同ID列表（按风险分数倒序）。
    """
    session = None
    start_time = time.time()
    try:
        session = get_nebula_session()

        params = request.params or PerformRiskParams()
        config = PerformRiskConfig(
            overdue_days_max=params.overdue_days_max,
            severity_power=params.severity_power,
            overdue_base_weight=params.overdue_base_weight,
            severity_multiplier_max=params.severity_multiplier_max,
            overdue_score_cap=params.overdue_score_cap,
            risk_contract_weight=params.risk_contract_weight,
            amount_threshold=params.amount_threshold,
            amount_weight=params.amount_weight,
        )

        # Parse current_date
        current_date = datetime.now()
        if params.current_date:
            try:
                current_date = datetime.strptime(params.current_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="日期格式不正确，请使用 YYYY-MM-DD 格式"
                )

        result = analyze_perform_risk(
            session=session,
            current_date=current_date,
            top_n=params.top_n,
            company_ids=request.orgs,
            periods=request.period,
            config=config,
        )

        report_df = result.get("report")
        risk_contract_ids = result.get("risk_contract_ids", [])

        # Build company report
        company_report = []
        if report_df is not None and len(report_df) > 0:
            for _, row in report_df.iterrows():
                company_report.append(
                    PerformRiskCompanyItem(
                        company_id=str(row.get("公司ID", "")),
                        company_name=str(row.get("公司名称", "")),
                        risk_score=float(row.get("风险分数", 0)),
                        overdue_count=int(row.get("逾期交易数", 0)),
                        risk_contract_count=int(row.get("风险合同数", 0)),
                        legal_person=str(row.get("法人代表", "")),
                        credit_code=str(row.get("信用代码", "")),
                        risk_contracts=str(row.get("风险合同列表", "")).split("; ") if row.get("风险合同列表") else [],
                    )
                )

        return BaseResponse(
            type="perform_risk",
            count=len(risk_contract_ids),
            contract_ids=risk_contract_ids,
            details={
                "company_list": [c.model_dump() for c in company_report],
                "metadata": {
                    "company_count": len(company_report),
                    "contract_count": len(risk_contract_ids),
                    "overdue_transaction_count": len(result.get("overdue_transactions", [])),
                    "current_date": current_date.strftime("%Y-%m-%d"),
                    "timestamp": datetime.now().isoformat(),
                    "execution_time": round(time.time() - start_time, 2),
                },
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if session:
            session.release()


@app.post("/api/perform-risk/subgraph")
async def get_perform_risk_subgraph_api(request: PerformRiskSubGraphRequest):
    """
    获取履约风险子图

    以风险合同ID为入口，查找合同的相关方，
    获取这些相关方的逾期交易以及涉及的合同，生成交互式HTML页面。
    直接返回 HTML 文件，可嵌入前端 iframe 中。
    """
    session = None
    try:
        session = get_nebula_session()

        # Parse current_date
        current_date = datetime.now()
        if request.current_date:
            try:
                current_date = datetime.strptime(request.current_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="日期格式不正确，请使用 YYYY-MM-DD 格式"
                )

        result = get_perform_risk_subgraph(
            session=session,
            contract_id=request.contract_id,
            current_date=current_date,
        )

        if not result.get("success"):
            raise HTTPException(
                status_code=404,
                detail=result.get("message", "未找到相关数据")
            )

        html_path = result.get("html_url")
        if not html_path or not os.path.exists(html_path):
            raise HTTPException(status_code=404, detail="HTML文件生成失败")

        return FileResponse(html_path, media_type="text/html")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if session:
            session.release()


@app.get("/api/perform-risk/subgraph/{contract_id}")
async def get_perform_risk_subgraph_by_id(
    contract_id: str,
    current_date: str = Query(default=None, description="当前日期，格式：YYYY-MM-DD"),
):
    """
    GET 方式获取履约风险子图（便于前端直接调用）
    """
    request = PerformRiskSubGraphRequest(
        contract_id=contract_id,
        current_date=current_date,
    )
    return await get_perform_risk_subgraph_api(request)


@app.get("/api/perform-risk/view/{filename}")
async def view_perform_risk_html(filename: str):
    """
    提供履约风险子图 HTML 文件访问

    前端可通过 iframe 嵌入此 URL 展示交互式图谱
    """
    file_path = os.path.join(REPORTS_DIR, filename)

    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="text/html")
    else:
        raise HTTPException(status_code=404, detail="File not found")


# ============================================================================
# 外部风险事件传导分析 API
# ============================================================================


@app.post("/api/external-risk-rank", response_model=BaseResponse)
async def external_risk_rank_analysis(request: ExternalRiskRankRequest):
    """
    外部风险事件传导分析

    基于 PageRank 算法，计算企业因行政处罚、经营异常等外部风险事件的风险传导分数。
    返回公司风险排名和关联合同ID列表（按风险分数倒序）。
    """
    session = None
    start_time = time.time()
    try:
        session = get_nebula_session()

        params = request.params or ExternalRiskRankParams()
        config = ExternalRiskRankConfig(
            edge_weights=params.edge_weights,
            admin_penalty_weights=params.admin_penalty_weights,
            admin_penalty_status_weights=params.admin_penalty_status_weights,
            admin_penalty_amount_max=params.admin_penalty_amount_max,
            business_abnormal_weights=params.business_abnormal_weights,
            business_abnormal_status_weights=params.business_abnormal_status_weights,
            damping=params.damping,
            risk_level_thresholds=params.risk_level_thresholds,
        )

        # Load embedding weights
        embedding_weights = None
        if params.use_cached_embedding:
            cache_file = os.path.join(CACHE_DIR, "edge_weights.json")
            embedding_weights = load_edge_weights(cache_file)

        # Load graph data
        graph = load_external_risk_graph(
            session,
            embedding_weights=embedding_weights,
            company_ids=request.orgs,
            periods=request.period,
            config=config,
        )

        # Initialize risk seeds
        init_scores, risk_details = initialize_external_risk_seeds(
            session,
            risk_type=params.risk_type,
            company_ids=request.orgs,
            periods=request.period,
            config=config,
        )

        # Compute External Risk Rank
        risk_scores = compute_external_risk_rank(
            graph, init_scores, damping=config.damping
        )

        # Generate report
        result = analyze_external_risk_results(
            risk_scores, risk_details, session,
            top_n=params.top_n,
            risk_type=params.risk_type,
            company_ids=request.orgs,
            config=config,
        )

        company_df = result.get("company_report")
        contract_ids = result.get("contract_ids", [])

        # Build company report
        company_report = []
        if company_df is not None and len(company_df) > 0:
            for _, row in company_df.iterrows():
                company_report.append(
                    ExternalRiskCompanyItem(
                        company_id=str(row.get("公司ID", "")),
                        company_name=str(row.get("公司名称", "")),
                        risk_score=float(row.get("风险分数", 0)),
                        risk_level=str(row.get("风险等级", "")),
                        risk_source=str(row.get("风险来源", "")),
                        risk_events=str(row.get("关联事件", "")),
                        legal_person=str(row.get("法人代表", "")),
                        credit_code=str(row.get("信用代码", "")),
                    )
                )

        seed_count = sum(1 for s in init_scores.values() if s > 0)

        return BaseResponse(
            type="external_risk_rank",
            count=len(contract_ids),
            contract_ids=contract_ids,
            details={
                "company_list": [c.model_dump() for c in company_report],
                "metadata": {
                    "node_count": len(graph["nodes"]),
                    "edge_count": sum(len(v) for v in graph["edges"].values()),
                    "seed_count": seed_count,
                    "company_count": len(company_report),
                    "contract_count": len(contract_ids),
                    "risk_type": params.risk_type,
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


@app.post("/api/external-risk-rank/subgraph")
async def get_external_risk_subgraph_api(request: ExternalRiskRankSubGraphRequest):
    """
    获取外部风险子图

    以合同ID为入口，查找合同的相关方中存在经营异常/行政处罚的公司，
    获取这些公司的风险事件以及涉及的其他合同，递归展开生成交互式HTML页面。
    直接返回 HTML 文件，可嵌入前端 iframe 中。
    """
    session = None
    try:
        session = get_nebula_session()

        result = get_external_risk_subgraph(
            session=session,
            contract_id=request.contract_id,
            max_depth=request.max_depth,
            risk_type=request.risk_type,
        )

        if not result.get("success"):
            raise HTTPException(
                status_code=404,
                detail=result.get("message", "未找到相关数据")
            )

        html_path = result.get("html_url")
        if not html_path or not os.path.exists(html_path):
            raise HTTPException(status_code=404, detail="HTML文件生成失败")

        return FileResponse(html_path, media_type="text/html")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if session:
            session.release()


@app.get("/api/external-risk-rank/subgraph/{contract_id}")
async def get_external_risk_subgraph_by_id(
    contract_id: str,
    max_depth: int = Query(default=2, ge=1, le=4, description="递归深度"),
    risk_type: str = Query(default="all", description="风险类型: admin_penalty, business_abnormal, all"),
):
    """
    GET 方式获取外部风险子图（便于前端直接调用）
    """
    request = ExternalRiskRankSubGraphRequest(
        contract_id=contract_id,
        max_depth=max_depth,
        risk_type=risk_type,
    )
    return await get_external_risk_subgraph_api(request)


@app.get("/api/external-risk-rank/view/{filename}")
async def view_external_risk_html(filename: str):
    """
    提供外部风险子图 HTML 文件访问

    前端可通过 iframe 嵌入此 URL 展示交互式图谱
    """
    file_path = os.path.join(REPORTS_DIR, filename)

    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="text/html")
    else:
        raise HTTPException(status_code=404, detail="File not found")


# ============================================================================
# 关联方串通网络分析 API
# ============================================================================


@app.post("/api/collusion", response_model=BaseResponse)
async def collusion_network_analysis(request: CollusionRequest):
    """
    关联方串通网络分析

    检测关联方串通网络，包括轮流中标、围标等模式。
    返回可疑串通网络列表（按风险分数倒序），包含涉及的合同ID。
    """
    session = None
    start_time = time.time()
    try:
        session = get_nebula_session()

        params = request.params or CollusionParams()
        config = CollusionConfig(
            min_cluster_size=params.min_cluster_size,
            risk_score_threshold=params.risk_score_threshold,
            approval_thresholds=params.approval_thresholds,
            threshold_margin=params.threshold_margin,
            feature_weights=params.feature_weights,
        )

        suspicious_networks = detect_collusion_network(
            session=session,
            company_ids=request.orgs,
            periods=request.period,
            config=config,
        )

        # Sort by risk_score descending and limit to top_n
        sorted_networks = sorted(
            suspicious_networks, key=lambda x: x["risk_score"], reverse=True
        )[:params.top_n]

        network_list = [
            CollusionNetworkItem(
                network_id=n["network_id"],
                companies=n["companies"],
                size=n["size"],
                risk_score=n["risk_score"],
                rotation_score=n.get("rotation_score", 0),
                amount_similarity=n.get("amount_similarity", 0),
                threshold_ratio=n.get("threshold_ratio", 0),
                network_density=n.get("network_density", 0),
                contract_count=n.get("contract_count", 0),
                total_amount=n.get("total_amount", 0),
                avg_amount=n.get("avg_amount", 0),
                contract_ids=n.get("contract_ids", []),
            )
            for n in sorted_networks
        ]

        # Collect all contract IDs from all networks
        all_contract_ids = list(set(
            cid for n in network_list for cid in n.contract_ids
        ))

        return BaseResponse(
            type="collusion",
            count=len(all_contract_ids),
            contract_ids=all_contract_ids,
            details={
                "network_list": [n.model_dump() for n in network_list],
                "metadata": {
                    "network_count": len(network_list),
                    "contract_count": len(all_contract_ids),
                    "min_cluster_size": config.min_cluster_size,
                    "risk_score_threshold": config.risk_score_threshold,
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


@app.post("/api/collusion/subgraph")
async def get_collusion_subgraph(request: CollusionSubGraphRequest):
    """
    获取合同关联的串通网络子图

    以合同ID为入口，查找合同的甲/乙方公司，
    检测这些公司所在的串通网络，并生成交互式HTML页面。
    直接返回 HTML 文件，可嵌入前端 iframe 中。
    """
    session = None
    try:
        session = get_nebula_session()

        config = CollusionConfig(
            min_cluster_size=request.min_cluster_size,
            risk_score_threshold=request.risk_score_threshold,
        )

        result = detect_collusion_by_contract(
            session=session,
            contract_id=request.contract_id,
            config=config,
        )

        if not result.get("html_url"):
            raise HTTPException(
                status_code=404,
                detail=result.get("message", "未检测到串通网络")
            )

        html_path = result["html_url"]

        if not os.path.exists(html_path):
            raise HTTPException(status_code=404, detail="HTML文件生成失败")

        return FileResponse(html_path, media_type="text/html")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if session:
            session.release()


@app.get("/api/collusion/subgraph/{contract_id}")
async def get_collusion_subgraph_by_id(
    contract_id: str,
    min_cluster_size: int = Query(default=3, ge=2, le=10, description="最小集群大小"),
    risk_score_threshold: float = Query(default=0.5, ge=0.0, le=1.0, description="风险分数阈值"),
):
    """
    GET 方式获取串通网络子图（便于前端直接调用）
    """
    request = CollusionSubGraphRequest(
        contract_id=contract_id,
        min_cluster_size=min_cluster_size,
        risk_score_threshold=risk_score_threshold,
    )
    return await get_collusion_subgraph(request)


@app.get("/api/collusion/view/{filename}")
async def view_collusion_html(filename: str):
    """
    提供串通网络 HTML 文件访问

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
            "circular_trade": "POST /api/circular-trade",
            "circular_trade_subgraph": "POST /api/circular-trade/subgraph",
            "circular_trade_view": "GET /api/circular-trade/view/{filename}",
            "contract_subgraph": "POST /api/contract-risk/subgraph",
            "contract_subgraph_get": "GET /api/contract-risk/subgraph/{contract_id}",
            "contract_view": "GET /api/contract-risk/view/{filename}",
            "perform_risk": "POST /api/perform-risk",
            "perform_risk_subgraph": "POST /api/perform-risk/subgraph",
            "perform_risk_subgraph_get": "GET /api/perform-risk/subgraph/{contract_id}",
            "perform_risk_view": "GET /api/perform-risk/view/{filename}",
            "external_risk_rank": "POST /api/external-risk-rank",
            "external_risk_rank_subgraph": "POST /api/external-risk-rank/subgraph",
            "external_risk_rank_subgraph_get": "GET /api/external-risk-rank/subgraph/{contract_id}",
            "external_risk_rank_view": "GET /api/external-risk-rank/view/{filename}",
            "collusion": "POST /api/collusion",
            "collusion_subgraph": "POST /api/collusion/subgraph",
            "collusion_subgraph_get": "GET /api/collusion/subgraph/{contract_id}",
            "collusion_view": "GET /api/collusion/view/{filename}",
        },
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
