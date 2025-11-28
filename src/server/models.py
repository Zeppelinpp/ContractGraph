from typing import List, Union, Dict, Any, Optional
from pydantic import BaseModel, Field

from src.config.models import FraudRankConfig, PerformRiskConfig, ExternalRiskRankConfig, CollusionConfig


# ============================================================================
# 通用请求/响应模型
# ============================================================================


class BaseRequest(BaseModel):
    """通用请求基类"""

    orgs: Optional[List[str]] = Field(default=None, description="组织ID列表")
    period: Optional[List[str]] = Field(default=None, description="时间范围")
    params: Optional[Dict[str, Any]] = Field(default=None, description="算法参数")


class BaseResponse(BaseModel):
    """统一响应结构"""

    type: str = Field(..., description="分析类型")
    count: int = Field(..., description="合同数量")
    contract_ids: List[str] = Field(..., description="合同ID列表")
    details: dict = Field(..., description="分析详情")


# ============================================================================
# FraudRank 相关模型
# ============================================================================


class FraudRankParams(FraudRankConfig):
    """FraudRank 算法参数（继承自 FraudRankConfig，新增API专用字段）"""

    type: str = Field(default="fraud_rank", description="场景类型")
    top_n: int = Field(default=50, description="返回 top N 结果")
    force_recompute: bool = Field(
        default=False, description="是否强制重新计算 embedding 权重"
    )


class FraudRankRequest(BaseRequest):
    """FraudRank 分析请求"""

    params: Optional[FraudRankParams] = Field(
        default=None, description="FraudRank 算法参数"
    )


class ContractRiskItem(BaseModel):
    """合同风险项"""

    contract_id: str
    contract_no: str
    contract_name: str
    risk_score: float
    risk_level: str
    amount: float
    sign_date: str
    status: str
    party_a_id: str
    party_a_name: str
    party_b_id: str
    party_b_name: str


class CompanyRiskItem(BaseModel):
    """公司风险项"""

    company_id: str
    company_name: str
    risk_score: float
    risk_level: str
    legal_person: str
    credit_code: str


class FraudRankResponse(BaseModel):
    """FraudRank 分析响应"""

    success: bool
    message: str
    company_report: List[CompanyRiskItem]
    contract_report: List[ContractRiskItem]
    stats: dict


# ============================================================================
# 合同风险子图相关模型
# ============================================================================


class ContractSubGraphRequest(BaseModel):
    """合同风险子图请求"""

    contract_id: str = Field(..., description="合同ID（Nebula Graph 节点ID）")
    max_depth: int = Field(default=3, ge=1, le=5, description="递归深度，1-5")


class SubGraphNode(BaseModel):
    """子图节点"""

    id: str
    type: str
    label: str
    properties: dict


class SubGraphEdge(BaseModel):
    """子图边"""

    source: str
    target: str
    type: str
    properties: dict


class ContractSubGraphResponse(BaseModel):
    """合同风险子图响应"""

    success: bool
    contract_id: str
    max_depth: int
    html_url: str
    node_count: int
    edge_count: int
    nodes: List[SubGraphNode]
    edges: List[SubGraphEdge]


# ============================================================================
# 循环交易检测相关模型
# ============================================================================


class CircularTradeParams(BaseModel):
    """循环交易检测算法参数"""

    type: str = Field(default="circular_trade", description="场景类型")
    time_window_days: int = Field(default=180, ge=1, le=365, description="时间窗口（天）")
    amount_threshold: float = Field(default=500000.0, ge=0, description="金额阈值")


class CircularTradeRequest(BaseRequest):
    """循环交易检测请求"""

    params: Optional[CircularTradeParams] = Field(
        default=None, description="循环交易检测算法参数"
    )


class CircularTradePattern(BaseModel):
    """循环交易可疑模式"""

    central_company: str = Field(..., description="核心公司ID")
    central_company_name: str = Field(default="", description="核心公司名称")
    dispersed_companies: List[str] = Field(..., description="分散节点公司ID列表")
    related_companies: List[str] = Field(..., description="关联公司ID列表")
    total_outflow: float = Field(..., description="流出金额")
    total_inflow: float = Field(..., description="流入金额")
    similarity: float = Field(..., description="流入流出相似度")
    inter_trade_count: int = Field(..., description="中间交易数量")
    time_span_days: int = Field(..., description="时间跨度（天）")
    risk_score: float = Field(..., description="风险分数")
    transaction_ids: List[str] = Field(default=[], description="涉及的交易ID列表")
    contract_ids: List[str] = Field(default=[], description="涉及的合同ID列表")


class CircularTradeSubGraphRequest(BaseModel):
    """循环交易子图请求"""

    contract_id: str = Field(..., description="合同ID（Nebula Graph 节点ID）")
    time_window_days: int = Field(default=180, ge=1, le=365, description="时间窗口（天）")
    amount_threshold: float = Field(default=500000.0, ge=0, description="金额阈值")


class CircularTradeSubGraphResponse(BaseModel):
    """循环交易子图响应"""

    success: bool
    central_company: str
    html_url: str
    node_count: int
    edge_count: int
    contract_ids: List[str]


# ============================================================================
# 履约关联风险检测相关模型
# ============================================================================


class PerformRiskParams(PerformRiskConfig):
    """履约风险检测算法参数（继承自 PerformRiskConfig，新增API专用字段）"""

    type: str = Field(default="perform_risk", description="场景类型")
    top_n: int = Field(default=50, description="返回 top N 结果")
    current_date: Optional[str] = Field(
        default=None, description="当前日期，格式：YYYY-MM-DD，默认为今天"
    )


class PerformRiskRequest(BaseRequest):
    """履约风险检测请求"""

    params: Optional[PerformRiskParams] = Field(
        default=None, description="履约风险检测算法参数"
    )


class PerformRiskCompanyItem(BaseModel):
    """履约风险公司项"""

    company_id: str
    company_name: str
    risk_score: float
    overdue_count: int
    risk_contract_count: int
    legal_person: str
    credit_code: str
    risk_contracts: List[str] = Field(default=[], description="风险合同列表")


class PerformRiskSubGraphRequest(BaseModel):
    """履约风险子图请求"""

    contract_id: str = Field(..., description="风险合同ID（Nebula Graph 节点ID）")
    current_date: Optional[str] = Field(
        default=None, description="当前日期，格式：YYYY-MM-DD，默认为今天"
    )


class PerformRiskSubGraphResponse(BaseModel):
    """履约风险子图响应"""

    success: bool
    contract_id: str
    html_url: Optional[str]
    node_count: int
    edge_count: int
    overdue_transaction_count: int
    related_contract_count: int
    company_count: int
    contract_ids: List[str] = Field(default=[], description="关联的逾期合同ID列表")
    nodes: List[SubGraphNode] = Field(default=[], description="子图节点")
    edges: List[SubGraphEdge] = Field(default=[], description="子图边")


# ============================================================================
# 外部风险事件传导分析相关模型
# ============================================================================


class ExternalRiskRankParams(ExternalRiskRankConfig):
    """外部风险事件传导分析算法参数（继承自 ExternalRiskRankConfig，新增API专用字段）"""

    type: str = Field(default="external_risk_rank", description="场景类型")
    top_n: int = Field(default=50, description="返回 top N 结果")
    risk_type: str = Field(
        default="all",
        description="风险类型: admin_penalty(行政处罚), business_abnormal(经营异常), all(全部)"
    )
    use_cached_embedding: bool = Field(
        default=True, description="是否使用缓存的 embedding 权重"
    )


class ExternalRiskRankRequest(BaseRequest):
    """外部风险事件传导分析请求"""

    params: Optional[ExternalRiskRankParams] = Field(
        default=None, description="外部风险事件传导分析算法参数"
    )


class ExternalRiskCompanyItem(BaseModel):
    """外部风险公司项"""

    company_id: str
    company_name: str
    risk_score: float
    risk_level: str
    risk_source: str = Field(description="风险来源: 直接关联 or 传导")
    risk_events: str = Field(description="关联的风险事件")
    legal_person: str
    credit_code: str


class ExternalRiskRankSubGraphRequest(BaseModel):
    """外部风险子图请求"""

    contract_id: str = Field(..., description="合同ID（Nebula Graph 节点ID）")
    max_depth: int = Field(default=2, ge=1, le=4, description="递归深度，1-4")
    risk_type: str = Field(
        default="all",
        description="风险类型: admin_penalty(行政处罚), business_abnormal(经营异常), all(全部)"
    )


class ExternalRiskRankSubGraphResponse(BaseModel):
    """外部风险子图响应"""

    success: bool
    contract_id: str
    html_url: Optional[str]
    max_depth: int
    node_count: int
    edge_count: int
    company_count: int
    risk_event_count: int
    contract_ids: List[str] = Field(default=[], description="关联的风险合同ID列表")
    nodes: List[SubGraphNode] = Field(default=[], description="子图节点")
    edges: List[SubGraphEdge] = Field(default=[], description="子图边")


# ============================================================================
# 关联方串通网络分析相关模型
# ============================================================================


class CollusionParams(CollusionConfig):
    """关联方串通网络分析算法参数（继承自 CollusionConfig，新增API专用字段）"""

    type: str = Field(default="collusion", description="场景类型")
    top_n: int = Field(default=50, description="返回 top N 结果")


class CollusionRequest(BaseRequest):
    """关联方串通网络分析请求"""

    params: Optional[CollusionParams] = Field(
        default=None, description="关联方串通网络分析算法参数"
    )


class CollusionNetworkItem(BaseModel):
    """串通网络项"""

    network_id: str = Field(..., description="网络ID")
    companies: List[str] = Field(..., description="网络中的公司ID列表")
    size: int = Field(..., description="网络中公司数量")
    risk_score: float = Field(..., description="风险分数")
    rotation_score: float = Field(default=0, description="轮换分数")
    amount_similarity: float = Field(default=0, description="金额相似度")
    threshold_ratio: float = Field(default=0, description="卡阈值比例")
    network_density: float = Field(default=0, description="网络密度")
    contract_count: int = Field(default=0, description="涉及合同数量")
    total_amount: float = Field(default=0, description="涉及金额总计")
    avg_amount: float = Field(default=0, description="平均合同金额")
    contract_ids: List[str] = Field(default=[], description="涉及的合同ID列表")


class CollusionSubGraphRequest(BaseModel):
    """串通网络子图请求"""

    contract_id: str = Field(..., description="合同ID（Nebula Graph 节点ID）")
    min_cluster_size: int = Field(default=3, ge=2, le=10, description="最小集群大小")
    risk_score_threshold: float = Field(default=0.5, ge=0.0, le=1.0, description="风险分数阈值")


class CollusionSubGraphResponse(BaseModel):
    """串通网络子图响应"""

    success: bool
    contract_id: str
    html_url: Optional[str]
    network_id: str = Field(default="", description="最高风险网络ID")
    node_count: int
    edge_count: int
    company_count: int
    contract_ids: List[str] = Field(default=[], description="关联的合同ID列表")
    nodes: List[SubGraphNode] = Field(default=[], description="子图节点")
    edges: List[SubGraphEdge] = Field(default=[], description="子图边")
