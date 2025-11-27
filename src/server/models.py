from typing import List, Union, Dict, Any, Optional
from pydantic import BaseModel, Field

from src.config.models import FraudRankConfig


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
