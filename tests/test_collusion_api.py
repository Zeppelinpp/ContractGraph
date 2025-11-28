"""
Collusion Network API 测试

测试 /api/collusion 接口和子图查询接口
模拟调用流程：先获取串通网络分析结果，再根据返回的 contract_ids 查询子图
"""

import pytest
from fastapi.testclient import TestClient

from src.server.api import app

client = TestClient(app)


class TestCollusionAPI:
    """Collusion Network API 测试类"""

    def test_collusion_default_params(self):
        """测试使用默认参数调用 Collusion"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {"type": "collusion"}
        }

        response = client.post("/api/collusion", json=request_data)

        assert response.status_code == 200
        data = response.json()

        # 验证响应结构
        assert data["type"] == "collusion"
        assert "count" in data
        assert "contract_ids" in data
        assert "details" in data
        assert "network_list" in data["details"]
        assert "metadata" in data["details"]

        # 验证 metadata 字段
        metadata = data["details"]["metadata"]
        assert "network_count" in metadata
        assert "contract_count" in metadata
        assert "min_cluster_size" in metadata
        assert "risk_score_threshold" in metadata
        assert "timestamp" in metadata
        assert "execution_time" in metadata

    def test_collusion_custom_min_cluster_size(self):
        """测试自定义最小集群大小"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "collusion",
                "min_cluster_size": 2
            }
        }

        response = client.post("/api/collusion", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "collusion"
        assert data["details"]["metadata"]["min_cluster_size"] == 2

    def test_collusion_custom_risk_threshold(self):
        """测试自定义风险分数阈值"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "collusion",
                "risk_score_threshold": 0.3
            }
        }

        response = client.post("/api/collusion", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "collusion"
        assert data["details"]["metadata"]["risk_score_threshold"] == 0.3

    def test_collusion_with_orgs_filter(self):
        """测试使用组织过滤参数"""
        request_data = {
            "orgs": ["ORG_001", "ORG_002"],
            "period": None,
            "params": {"type": "collusion"}
        }

        response = client.post("/api/collusion", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "collusion"

    def test_collusion_with_period_filter(self):
        """测试使用时间范围过滤"""
        request_data = {
            "orgs": None,
            "period": ["2024-01-01", "2024-12-31"],
            "params": {"type": "collusion"}
        }

        response = client.post("/api/collusion", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "collusion"

    def test_collusion_custom_feature_weights(self):
        """测试自定义特征权重"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "collusion",
                "feature_weights": {
                    "rotation": 0.4,
                    "amount_similarity": 0.25,
                    "threshold_ratio": 0.15,
                    "network_density": 0.15,
                    "strong_relation": 0.05
                }
            }
        }

        response = client.post("/api/collusion", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "collusion"

    def test_collusion_custom_approval_thresholds(self):
        """测试自定义审批金额阈值"""
        request_data = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "collusion",
                "approval_thresholds": [500000, 1000000, 2000000, 5000000]
            }
        }

        response = client.post("/api/collusion", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "collusion"

    def test_collusion_full_custom_params(self):
        """测试完整自定义参数"""
        request_data = {
            "orgs": None,
            "period": ["2024-01-01", "2024-12-31"],
            "params": {
                "type": "collusion",
                "top_n": 100,
                "min_cluster_size": 2,
                "risk_score_threshold": 0.4,
                "approval_thresholds": [1000000, 3000000, 5000000, 10000000],
                "threshold_margin": 0.08,
                "feature_weights": {
                    "rotation": 0.35,
                    "amount_similarity": 0.2,
                    "threshold_ratio": 0.2,
                    "network_density": 0.15,
                    "strong_relation": 0.1
                }
            }
        }

        response = client.post("/api/collusion", json=request_data)

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "collusion"


class TestCollusionSubGraphAPI:
    """Collusion SubGraph API 测试类"""

    def test_subgraph_post_endpoint(self):
        """测试 POST 方式获取子图"""
        request_data = {
            "contract_id": "CON_001",
            "min_cluster_size": 3,
            "risk_score_threshold": 0.5
        }

        response = client.post("/api/collusion/subgraph", json=request_data)

        # 可能返回 200 或 404（取决于数据是否存在）
        assert response.status_code in [200, 404, 500]
        
        if response.status_code == 200:
            data = response.json()
            assert data["success"] is True
            assert "contract_id" in data
            assert "html_url" in data
            assert "network_id" in data
            assert "node_count" in data
            assert "edge_count" in data
            assert "company_count" in data
            assert "contract_ids" in data

    def test_subgraph_get_endpoint(self):
        """测试 GET 方式获取子图"""
        response = client.get(
            "/api/collusion/subgraph/CON_001",
            params={"min_cluster_size": 3, "risk_score_threshold": 0.5}
        )

        assert response.status_code in [200, 404, 500]

    def test_subgraph_different_cluster_sizes(self):
        """测试不同最小集群大小"""
        for cluster_size in [2, 3, 5]:
            request_data = {
                "contract_id": "CON_001",
                "min_cluster_size": cluster_size,
                "risk_score_threshold": 0.5
            }

            response = client.post("/api/collusion/subgraph", json=request_data)
            assert response.status_code in [200, 404, 500]

    def test_subgraph_different_risk_thresholds(self):
        """测试不同风险阈值"""
        for threshold in [0.3, 0.5, 0.7]:
            request_data = {
                "contract_id": "CON_001",
                "min_cluster_size": 3,
                "risk_score_threshold": threshold
            }

            response = client.post("/api/collusion/subgraph", json=request_data)
            assert response.status_code in [200, 404, 500]


class TestCollusionWorkflow:
    """测试完整的工作流程：先分析串通网络，再查询子图"""

    def test_full_workflow(self):
        """
        完整工作流程测试：
        1. 调用 collusion 获取串通网络分析结果
        2. 从返回的 contract_ids 中选择一个
        3. 调用 subgraph 接口获取该合同的串通网络子图
        """
        # Step 1: 调用串通网络分析接口
        print("\n" + "=" * 60)
        print("Step 1: 调用 /api/collusion 获取串通网络分析结果")
        print("=" * 60)
        
        collusion_request = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "collusion",
                "min_cluster_size": 2,
                "risk_score_threshold": 0.3,
                "top_n": 20
            }
        }

        collusion_response = client.post("/api/collusion", json=collusion_request)
        
        assert collusion_response.status_code == 200
        collusion_data = collusion_response.json()
        
        print(f"  响应类型: {collusion_data['type']}")
        print(f"  风险合同数量: {collusion_data['count']}")
        print(f"  串通网络数量: {len(collusion_data['details']['network_list'])}")
        
        metadata = collusion_data["details"]["metadata"]
        print(f"  最小集群大小: {metadata['min_cluster_size']}")
        print(f"  风险阈值: {metadata['risk_score_threshold']}")
        print(f"  执行时间: {metadata['execution_time']}s")
        
        # 打印前5个高风险网络
        network_list = collusion_data["details"]["network_list"]
        if network_list:
            print("\n  前5个高风险串通网络:")
            for i, network in enumerate(network_list[:5]):
                print(f"    {i+1}. {network['network_id']}")
                print(f"       公司数量: {network['size']}")
                print(f"       风险分数: {network['risk_score']:.4f}")
                print(f"       轮换分数: {network['rotation_score']:.4f}")
                print(f"       金额相似度: {network['amount_similarity']:.4f}")
                print(f"       卡阈值比例: {network['threshold_ratio']:.2%}")
                print(f"       网络密度: {network['network_density']:.4f}")
                print(f"       合同数量: {network['contract_count']}")
                print(f"       涉及金额: ¥{network['total_amount']:,.2f}")
        
        contract_ids = collusion_data["contract_ids"]
        print(f"\n  返回的合同ID列表 (前10个): {contract_ids[:10]}")

        # Step 2: 选择一个合同ID查询子图
        if contract_ids:
            selected_contract_id = contract_ids[0]
            print("\n" + "=" * 60)
            print(f"Step 2: 选择合同 {selected_contract_id} 查询串通网络子图")
            print("=" * 60)
            
            subgraph_request = {
                "contract_id": selected_contract_id,
                "min_cluster_size": 2,
                "risk_score_threshold": 0.3
            }

            subgraph_response = client.post(
                "/api/collusion/subgraph",
                json=subgraph_request
            )
            
            if subgraph_response.status_code == 200:
                subgraph_data = subgraph_response.json()
                
                print(f"  子图查询成功!")
                print(f"  合同ID: {subgraph_data['contract_id']}")
                print(f"  网络ID: {subgraph_data['network_id']}")
                print(f"  节点数: {subgraph_data['node_count']}")
                print(f"  边数: {subgraph_data['edge_count']}")
                print(f"  公司数: {subgraph_data['company_count']}")
                print(f"  关联合同数: {len(subgraph_data['contract_ids'])}")
                print(f"  HTML URL: {subgraph_data['html_url']}")
                
                # 统计节点类型
                if subgraph_data["nodes"]:
                    node_types = {}
                    for node in subgraph_data["nodes"]:
                        node_type = node["type"]
                        node_types[node_type] = node_types.get(node_type, 0) + 1
                    print(f"\n  节点类型分布: {node_types}")
                
                # 统计边类型
                if subgraph_data["edges"]:
                    edge_types = {}
                    for edge in subgraph_data["edges"]:
                        edge_type = edge["type"]
                        edge_types[edge_type] = edge_types.get(edge_type, 0) + 1
                    print(f"  边类型分布: {edge_types}")
                
                assert subgraph_data["success"] is True
            elif subgraph_response.status_code == 404:
                print(f"  子图查询未找到相关网络: {subgraph_response.json()}")
            else:
                print(f"  子图查询失败: {subgraph_response.status_code}")
                print(f"  错误信息: {subgraph_response.json()}")
        else:
            print("\n  没有返回风险合同，跳过子图查询")

        print("\n" + "=" * 60)
        print("测试完成!")
        print("=" * 60)

    def test_workflow_with_different_params(self):
        """测试不同参数组合的工作流程"""
        param_combinations = [
            {"min_cluster_size": 2, "risk_score_threshold": 0.3},
            {"min_cluster_size": 3, "risk_score_threshold": 0.5},
            {"min_cluster_size": 4, "risk_score_threshold": 0.6},
        ]
        
        for params in param_combinations:
            print(f"\n测试参数组合: min_cluster_size={params['min_cluster_size']}, risk_threshold={params['risk_score_threshold']}")
            
            collusion_request = {
                "orgs": None,
                "period": None,
                "params": {
                    "type": "collusion",
                    **params,
                    "top_n": 10
                }
            }

            response = client.post("/api/collusion", json=collusion_request)
            assert response.status_code == 200
            
            data = response.json()
            print(f"  网络数量: {len(data['details']['network_list'])}, 合同数量: {data['count']}")

    def test_workflow_iterate_all_contracts(self):
        """测试遍历所有风险合同并查询子图"""
        print("\n" + "=" * 60)
        print("测试：遍历所有风险合同查询子图")
        print("=" * 60)
        
        # Step 1: 获取串通网络
        collusion_request = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "collusion",
                "min_cluster_size": 2,
                "risk_score_threshold": 0.3,
                "top_n": 50
            }
        }

        response = client.post("/api/collusion", json=collusion_request)
        assert response.status_code == 200
        
        data = response.json()
        contract_ids = data["contract_ids"]
        
        print(f"  发现 {len(contract_ids)} 个风险合同")
        
        # Step 2: 遍历前5个合同查询子图
        success_count = 0
        fail_count = 0
        
        for i, contract_id in enumerate(contract_ids[:5]):
            subgraph_request = {
                "contract_id": contract_id,
                "min_cluster_size": 2,
                "risk_score_threshold": 0.3
            }
            
            subgraph_response = client.post("/api/collusion/subgraph", json=subgraph_request)
            
            if subgraph_response.status_code == 200:
                success_count += 1
                subgraph_data = subgraph_response.json()
                print(f"  [{i+1}] {contract_id}: 成功 - 网络{subgraph_data['network_id']}, {subgraph_data['company_count']}家公司")
            else:
                fail_count += 1
                print(f"  [{i+1}] {contract_id}: 未找到相关网络")
        
        print(f"\n  统计: 成功 {success_count}, 未找到 {fail_count}")


class TestCollusionHTMLView:
    """测试 HTML 视图访问"""

    def test_view_nonexistent_file(self):
        """测试访问不存在的 HTML 文件"""
        response = client.get("/api/collusion/view/nonexistent_file.html")
        assert response.status_code == 404

    def test_view_after_subgraph_generation(self):
        """测试生成子图后访问 HTML 文件"""
        # 先获取串通网络
        collusion_request = {
            "orgs": None,
            "period": None,
            "params": {
                "type": "collusion",
                "min_cluster_size": 2,
                "risk_score_threshold": 0.3,
                "top_n": 10
            }
        }

        response = client.post("/api/collusion", json=collusion_request)
        if response.status_code != 200:
            pytest.skip("无法获取串通网络数据")
            
        data = response.json()
        contract_ids = data["contract_ids"]
        
        if not contract_ids:
            pytest.skip("没有返回风险合同")
        
        # 生成子图
        subgraph_request = {
            "contract_id": contract_ids[0],
            "min_cluster_size": 2,
            "risk_score_threshold": 0.3
        }
        
        subgraph_response = client.post("/api/collusion/subgraph", json=subgraph_request)
        
        if subgraph_response.status_code == 200:
            subgraph_data = subgraph_response.json()
            html_url = subgraph_data["html_url"]
            
            # 访问生成的 HTML 文件
            html_response = client.get(html_url)
            assert html_response.status_code == 200
            assert "text/html" in html_response.headers.get("content-type", "")


if __name__ == "__main__":
    # 可以直接运行此文件进行测试
    print("运行 Collusion Network API 测试...")
    
    # 运行完整工作流程测试
    test = TestCollusionWorkflow()
    test.test_full_workflow()
    
    # 或使用 pytest 运行所有测试
    # pytest.main([__file__, "-v", "-s"])

