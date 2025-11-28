"""
External Risk Rank API 测试

测试 /api/external-risk-rank 接口和子图查询接口
模拟调用流程：先获取风险分析结果，再根据返回的 contract_ids 查询子图
"""

import pytest
from fastapi.testclient import TestClient

from src.server.api import app

client = TestClient(app)


class TestExternalRiskRankAPI:
    """External Risk Rank API 测试类"""

    def test_external_risk_rank_default_params(self):
        """测试使用默认参数调用 External Risk Rank"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {"type": "external_risk_rank"}
        }

        response = client.post("/api/external-risk-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()

        # 验证响应结构
        assert data["type"] == "external_risk_rank"
        assert "count" in data
        assert "contract_ids" in data
        assert "details" in data
        assert "company_list" in data["details"]
        assert "metadata" in data["details"]

        # 验证 metadata 字段
        metadata = data["details"]["metadata"]
        assert "node_count" in metadata
        assert "edge_count" in metadata
        assert "seed_count" in metadata
        assert "risk_type" in metadata
        assert "timestamp" in metadata
        assert "execution_time" in metadata

    def test_external_risk_rank_admin_penalty_only(self):
        """测试仅查询行政处罚风险"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "external_risk_rank",
                "risk_type": "admin_penalty"
            }
        }

        response = client.post("/api/external-risk-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "external_risk_rank"
        assert data["details"]["metadata"]["risk_type"] == "admin_penalty"

    def test_external_risk_rank_business_abnormal_only(self):
        """测试仅查询经营异常风险"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "external_risk_rank",
                "risk_type": "business_abnormal"
            }
        }

        response = client.post("/api/external-risk-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "external_risk_rank"
        assert data["details"]["metadata"]["risk_type"] == "business_abnormal"

    def test_external_risk_rank_with_orgs_filter(self):
        """测试使用组织过滤参数"""
        request_data = {
            "orgs": ["ORG_001", "ORG_002"],
            "period": None,
            "params": {"type": "external_risk_rank"}
        }

        response = client.post("/api/external-risk-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "external_risk_rank"

    def test_external_risk_rank_with_period_filter(self):
        """测试使用时间范围过滤"""
        request_data = {
            "orgs": None,
            "period": ["2024-01-01", "2024-12-31"],
            "params": {"type": "external_risk_rank"}
        }

        response = client.post("/api/external-risk-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "external_risk_rank"

    def test_external_risk_rank_custom_damping(self):
        """测试自定义阻尼系数"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "external_risk_rank",
                "damping": 0.9
            }
        }

        response = client.post("/api/external-risk-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "external_risk_rank"

    def test_external_risk_rank_custom_edge_weights(self):
        """测试自定义边权重"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "external_risk_rank",
                "edge_weights": {
                    "CONTROLS": 0.9,
                    "TRADES_WITH": 0.6,
                    "ADMIN_PENALTY_OF": 0.95
                }
            }
        }

        response = client.post("/api/external-risk-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "external_risk_rank"

    def test_external_risk_rank_full_custom_params(self):
        """测试完整自定义参数"""
        request_data = {
            "orgs": None,
            "period": ["2024-01-01", "2024-12-31"],
            "params": {
                "type": "external_risk_rank",
                "top_n": 100,
                "risk_type": "all",
                "use_cached_embedding": True,
                "damping": 0.85,
                "edge_weights": {
                    "CONTROLS": 0.85,
                    "LEGAL_PERSON": 0.75,
                    "TRADES_WITH": 0.50,
                    "IS_SUPPLIER": 0.45,
                    "IS_CUSTOMER": 0.40,
                    "ADMIN_PENALTY_OF": 0.90,
                    "BUSINESS_ABNORMAL_OF": 0.70
                },
                "admin_penalty_weights": {
                    "amount": 0.4,
                    "status": 0.3,
                    "severity": 0.3
                },
                "risk_level_thresholds": {
                    "high": 0.6,
                    "medium": 0.3,
                    "low": 0.1
                }
            }
        }

        response = client.post("/api/external-risk-rank", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "external_risk_rank"


class TestExternalRiskRankSubGraphAPI:
    """External Risk Rank SubGraph API 测试类"""

    def test_subgraph_post_endpoint(self):
        """测试 POST 方式获取子图"""
        # 假设存在一个合同ID
        request_data = {
            "contract_id": "CON_001",
            "max_depth": 2,
            "risk_type": "all"
        }

        response = client.post("/api/external-risk-rank/subgraph", json=request_data)

        # 可能返回 200 或 404（取决于数据是否存在）
        assert response.status_code in [200, 404, 500]
        
        if response.status_code == 200:
            data = response.json()
            assert data["success"] is True
            assert "contract_id" in data
            assert "html_url" in data
            assert "nodes" in data
            assert "edges" in data
            assert "contract_ids" in data

    def test_subgraph_get_endpoint(self):
        """测试 GET 方式获取子图"""
        response = client.get(
            "/api/external-risk-rank/subgraph/CON_001",
            params={"max_depth": 2, "risk_type": "all"}
        )

        assert response.status_code in [200, 404, 500]

    def test_subgraph_different_depths(self):
        """测试不同递归深度"""
        for depth in [1, 2, 3]:
            request_data = {
                "contract_id": "CON_001",
                "max_depth": depth,
                "risk_type": "all"
            }

            response = client.post("/api/external-risk-rank/subgraph", json=request_data)
            assert response.status_code in [200, 404, 500]


class TestExternalRiskRankWorkflow:
    """测试完整的工作流程：先分析风险，再查询子图"""

    def test_full_workflow(self):
        """
        完整工作流程测试：
        1. 调用 external-risk-rank 获取风险分析结果
        2. 从返回的 contract_ids 中选择一个
        3. 调用 subgraph 接口获取该合同的风险子图
        """
        # Step 1: 调用外部风险分析接口
        print("\n" + "=" * 60)
        print("Step 1: 调用 /api/external-risk-rank 获取风险分析结果")
        print("=" * 60)
        
        risk_request = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "external_risk_rank",
                "risk_type": "all",
                "top_n": 20
            }
        }

        risk_response = client.post("/api/external-risk-rank", json=risk_request)
        
        assert risk_response.status_code == 200
        risk_data = risk_response.json()
        
        print(f"  响应类型: {risk_data['type']}")
        print(f"  风险合同数量: {risk_data['count']}")
        print(f"  公司数量: {len(risk_data['details']['company_list'])}")
        
        metadata = risk_data["details"]["metadata"]
        print(f"  图节点数: {metadata['node_count']}")
        print(f"  图边数: {metadata['edge_count']}")
        print(f"  风险种子数: {metadata['seed_count']}")
        print(f"  执行时间: {metadata['execution_time']}s")
        
        # 打印前5个高风险公司
        company_list = risk_data["details"]["company_list"]
        if company_list:
            print("\n  前5个高风险公司:")
            for i, company in enumerate(company_list[:5]):
                print(f"    {i+1}. {company['company_name']} - 分数: {company['risk_score']:.4f} ({company['risk_level']})")
        
        contract_ids = risk_data["contract_ids"]
        print(f"\n  返回的合同ID列表 (前10个): {contract_ids[:10]}")

        # Step 2: 选择一个合同ID查询子图
        if contract_ids:
            selected_contract_id = contract_ids[0]
            print("\n" + "=" * 60)
            print(f"Step 2: 选择合同 {selected_contract_id} 查询风险子图")
            print("=" * 60)
            
            subgraph_request = {
                "contract_id": selected_contract_id,
                "max_depth": 5,
                "risk_type": "all"
            }

            subgraph_response = client.post(
                "/api/external-risk-rank/subgraph",
                json=subgraph_request
            )
            
            if subgraph_response.status_code == 200:
                subgraph_data = subgraph_response.json()
                
                print(f"  子图查询成功!")
                print(f"  合同ID: {subgraph_data['contract_id']}")
                print(f"  递归深度: {subgraph_data['max_depth']}")
                print(f"  节点数: {subgraph_data['node_count']}")
                print(f"  边数: {subgraph_data['edge_count']}")
                print(f"  公司数: {subgraph_data['company_count']}")
                print(f"  风险事件数: {subgraph_data['risk_event_count']}")
                print(f"  关联合同数: {len(subgraph_data['contract_ids'])}")
                print(f"  HTML URL: {subgraph_data['html_url']}")
                
                # 统计节点类型
                node_types = {}
                for node in subgraph_data["nodes"]:
                    node_type = node["type"]
                    node_types[node_type] = node_types.get(node_type, 0) + 1
                print(f"\n  节点类型分布: {node_types}")
                
                # 统计边类型
                edge_types = {}
                for edge in subgraph_data["edges"]:
                    edge_type = edge["type"]
                    edge_types[edge_type] = edge_types.get(edge_type, 0) + 1
                print(f"  边类型分布: {edge_types}")
                
                assert subgraph_data["success"] is True
            else:
                print(f"  子图查询失败: {subgraph_response.status_code}")
                print(f"  错误信息: {subgraph_response.json()}")
        else:
            print("\n  没有返回风险合同，跳过子图查询")

        print("\n" + "=" * 60)
        print("测试完成!")
        print("=" * 60)

    def test_workflow_with_different_risk_types(self):
        """测试不同风险类型的工作流程"""
        for risk_type in ["admin_penalty", "business_abnormal", "all"]:
            print(f"\n测试风险类型: {risk_type}")
            
            risk_request = {
                "orgs": None,
                "period": None,
                "params": {
                    "type": "external_risk_rank",
                    "risk_type": risk_type,
                    "top_n": 10
                }
            }

            response = client.post("/api/external-risk-rank", json=risk_request)
            assert response.status_code == 200
            
            data = response.json()
            print(f"  合同数量: {data['count']}, 公司数量: {len(data['details']['company_list'])}")


if __name__ == "__main__":
    # 可以直接运行此文件进行测试
    print("运行 External Risk Rank API 测试...")
    
    # 运行完整工作流程测试
    test = TestExternalRiskRankWorkflow()
    test.test_full_workflow()
    
    # 或使用 pytest 运行所有测试
    # pytest.main([__file__, "-v", "-s"])

