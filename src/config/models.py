"""
配置模型定义

用于定义各个分析场景的参数配置模型
"""

from typing import Dict, Optional
from pydantic import BaseModel, Field


class FraudRankConfig(BaseModel):
    """
    FraudRank 欺诈风险传导分析的参数配置模型
    """
    
    # 边权重配置
    edge_weights: Dict[str, float] = Field(
        default={
            "CONTROLS": 0.8,
            "LEGAL_PERSON": 0.75,
            "PAYS": 0.65,
            "RECEIVES": 0.60,
            "TRADES_WITH": 0.50,
            "IS_SUPPLIER": 0.45,
            "IS_CUSTOMER": 0.40,
            "PARTY_A": 0.50,
            "PARTY_B": 0.50,
        },
        description="边权重配置，定义不同类型边的风险传导权重"
    )
    
    # 事件类型权重配置
    event_type_weights: Dict[str, float] = Field(
        default={
            "Case": 0.8,
            "Dispute": 0.5,
        },
        description="事件类型权重配置，定义不同类型法律事件的风险权重"
    )
    
    # 事件类型默认权重
    event_type_default_weight: float = Field(
        default=0.3,
        description="未知事件类型的默认权重"
    )
    
    # 状态权重配置
    status_weights: Dict[str, float] = Field(
        default={
            "F": 0.9,  # 已立案
            "I": 0.8,  # 一审
            "J": 0.7,  # 执行
            "N": 0.4,  # 已结案
        },
        description="状态权重配置，定义法律事件不同状态的风险权重"
    )
    
    # 状态默认权重
    status_default_weight: float = Field(
        default=0.5,
        description="未知状态的默认权重"
    )
    
    # 金额上限（用于归一化）
    amount_threshold: float = Field(
        default=10000000.0,  # 1000万
        description="金额权重归一化的上限金额，超过此金额的事件金额权重将被限制为 1.0"
    )
    
    # 初始分数计算的加权平均系数
    init_score_weights: Dict[str, float] = Field(
        default={
            "event_type": 0.4,   # 事件类型权重占比
            "amount": 0.35,      # 金额权重占比
            "status": 0.25,      # 状态权重占比
        },
        description="初始风险分数计算时各因子的加权平均系数，总和应为1.0"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "edge_weights": {
                    "CONTROLS": 0.8,
                    "LEGAL_PERSON": 0.75,
                    "PAYS": 0.65,
                    "RECEIVES": 0.60,
                    "TRADES_WITH": 0.50,
                    "IS_SUPPLIER": 0.45,
                    "IS_CUSTOMER": 0.40,
                    "PARTY_A": 0.50,
                    "PARTY_B": 0.50,
                },
                "event_type_weights": {
                    "Case": 0.8,
                    "Dispute": 0.5,
                },
                "event_type_default_weight": 0.3,
                "status_weights": {
                    "F": 0.9,
                    "I": 0.8,
                    "J": 0.7,
                    "N": 0.4,
                },
                "status_default_weight": 0.5,
                "amount_threshold": 10000000.0,
            }
        }

