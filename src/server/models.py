from typing import List, Union, Dict, Any, Optional
from pydantic import BaseModel, Field

from src.config.models import FraudRankConfig, PerformRiskConfig


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
