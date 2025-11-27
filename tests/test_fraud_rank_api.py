"""
FraudRank API 测试

测试 /api/fraud-rank 接口的默认参数和自定义参数调用
"""

import pytest
from fastapi.testclient import TestClient

from src.server.api import app

client = TestClient(app)


class TestFraudRankAPI:
    """FraudRank API 测试类"""

    def test_fraud_rank_default_params(self):
        """测试使用默认参数调用 FraudRank"""
        request_data = {"orgs": None, "period": None, "params": {"type": "fraud_rank"}}

        response = client.post("/api/fraud-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()

        # 验证响应结构
        assert data["type"] == "fraud_rank"
        assert "count" in data
        assert "contract_ids" in data
        assert "details" in data
        assert "contract_list" in data["details"]
        assert "metadata" in data["details"]

        # 验证 metadata 字段
        metadata = data["details"]["metadata"]
        assert "node_count" in metadata
        assert "edge_count" in metadata
        assert "seed_count" in metadata
        assert "timestamp" in metadata
        assert "execution_time" in metadata

    def test_fraud_rank_with_orgs_filter(self):
        """测试使用组织过滤参数"""
        request_data = {
            "orgs": ["ORG_001", "ORG_002"],
            "period": None,
            "params": {"type": "fraud_rank"},
        }

        response = client.post("/api/fraud-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "fraud_rank"

    def test_fraud_rank_with_period_filter(self):
        """测试使用时间范围过滤"""
        request_data = {
            "orgs": None,
            "period": ["2024-01-01", "2024-12-31"],
            "params": {"type": "fraud_rank"},
        }

        response = client.post("/api/fraud-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "fraud_rank"

    def test_fraud_rank_custom_top_n(self):
        """测试自定义 top_n 参数"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {"type": "fraud_rank", "top_n": 10},
        }

        response = client.post("/api/fraud-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        # 验证返回的合同数量不超过 top_n
        assert len(data["details"]["contract_list"]) <= 10

    def test_fraud_rank_custom_edge_weights(self):
        """测试自定义边权重参数"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "fraud_rank",
                "edge_weights": {
                    "CONTROLS": 0.9,
                    "LEGAL_PERSON": 0.85,
                    "TRADES_WITH": 0.6,
                },
            },
        }

        response = client.post("/api/fraud-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "fraud_rank"

    def test_fraud_rank_custom_event_weights(self):
        """测试自定义事件类型权重"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "fraud_rank",
                "event_type_weights": {"Case": 0.9, "Dispute": 0.6},
                "event_type_default_weight": 0.4,
            },
        }

        response = client.post("/api/fraud-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "fraud_rank"

    def test_fraud_rank_custom_status_weights(self):
        """测试自定义状态权重"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "fraud_rank",
                "status_weights": {"F": 1.0, "I": 0.9, "J": 0.8, "N": 0.3},
                "status_default_weight": 0.6,
            },
        }

        response = client.post("/api/fraud-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "fraud_rank"

    def test_fraud_rank_custom_amount_threshold(self):
        """测试自定义金额阈值"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {"type": "fraud_rank", "amount_threshold": 5000000.0},
        }

        response = client.post("/api/fraud-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "fraud_rank"

    def test_fraud_rank_full_custom_params(self):
        """测试完整自定义参数"""
        request_data = {
            "orgs": ["ORG_001"],
            "period": ["2024-01-01", "2024-12-31"],
            "params": {
                "type": "fraud_rank",
                "top_n": 100,
                "force_recompute": False,
                "edge_weights": {
                    "CONTROLS": 0.9,
                    "LEGAL_PERSON": 0.8,
                    "PAYS": 0.7,
                    "RECEIVES": 0.65,
                    "TRADES_WITH": 0.55,
                    "IS_SUPPLIER": 0.5,
                    "IS_CUSTOMER": 0.45,
                    "PARTY_A": 0.55,
                    "PARTY_B": 0.55,
                },
                "event_type_weights": {"Case": 0.85, "Dispute": 0.55},
                "event_type_default_weight": 0.35,
                "status_weights": {"F": 0.95, "I": 0.85, "J": 0.75, "N": 0.35},
                "status_default_weight": 0.55,
                "amount_threshold": 8000000.0,
            },
        }

        response = client.post("/api/fraud-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "fraud_rank"
        assert "details" in data

    def test_fraud_rank_empty_request(self):
        """测试空请求（全部使用默认值）"""
        request_data = {}

        response = client.post("/api/fraud-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "fraud_rank"

    def test_fraud_rank_null_params(self):
        """测试 params 为 null"""
        request_data = {"orgs": None, "period": None, "params": None}

        response = client.post("/api/fraud-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "fraud_rank"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
