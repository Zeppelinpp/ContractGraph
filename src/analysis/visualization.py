"""
高级可视化分析模块

实现两个高级可视化视角：
1. 视角二：风险染色视角 (Risk Gradient View) - 验证 FraudRank 算法效果
2. 视角三：隐式关联视角 (Implicit Link View) - 揭示隐式关联关系
"""

import os
import numpy as np
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from sklearn.manifold import TSNE
from sklearn.metrics.pairwise import cosine_similarity
from typing import Dict, List, Optional, Tuple, Set
from collections import defaultdict

from src.utils.nebula_utils import get_nebula_session, execute_query
from src.utils.embedding_viz import get_node_embeddings, extract_embeddings
from src.analysis.fraud_rank import (
    load_weighted_graph,
    initialize_risk_seeds,
    compute_fraud_rank
)


def compute_pagerank(graph: Dict) -> Dict[str, float]:
    """
    计算节点的 PageRank 值
    
    Args:
        graph: 图数据结构，包含 'nodes', 'edges', 'out_degree'
    
    Returns:
        dict: {node_id: pagerank_score}
    """
    G = nx.DiGraph()
    
    for node in graph["nodes"]:
        G.add_node(node)
    
    for from_node, neighbors_list in graph["edges"].items():
        for to_node, weight in neighbors_list:
            G.add_edge(from_node, to_node, weight=weight)
    
    pagerank = nx.pagerank(G, alpha=0.85, max_iter=100)
    return pagerank


def get_original_graph_edges(session) -> Set[Tuple[str, str]]:
    """
    获取原始图中所有边的集合（用于判断两个节点是否直接相连）
    
    Args:
        session: Nebula Graph session
    
    Returns:
        set: {(src_id, dst_id)} 边的集合
    """
    edges_set = set()
    
    ngql = """
    MATCH (v)-[e]->(w) 
    RETURN id(v) as src, id(w) as dst
    LIMIT 50000
    """
    rows = execute_query(session, ngql)
    
    for row in rows:
        src = row.get('src', '')
        dst = row.get('dst', '')
        if src and dst:
            edges_set.add((src, dst))
            edges_set.add((dst, src))
    
    return edges_set


def risk_gradient_view(
    session=None,
    output_path: Optional[str] = None,
    size_by: str = 'out_degree',
    limit: int = 10000,
    dimensions: int = 32,
    figsize: Tuple[int, int] = (14, 10)
):
    """
    视角二：风险染色视角 (Risk Gradient View)
    
    验证 FraudRank 算法效果，看高风险节点是否在向量空间上占据了特定位置
    
    Args:
        session: Nebula Graph session，如果为 None 则创建新 session
        output_path: 输出图片路径
        size_by: 点的大小依据，'out_degree' 或 'pagerank'
        limit: 查询边的数量限制
        dimensions: embedding 维度
        figsize: 图片大小
    """
    should_release = False
    if session is None:
        session = get_nebula_session()
        should_release = True
    
    try:
        print("=" * 60)
        print("视角二：风险染色视角 (Risk Gradient View)")
        print("=" * 60)
        
        # Step 1: 获取节点 embeddings
        print("\n[1/5] 获取节点 embeddings...")
        model, node_to_type, node_ids_list = get_node_embeddings(
            session=session, limit=limit, dimensions=dimensions
        )
        print(f"  节点数: {len(node_ids_list)}")
        
        # Step 2: 提取 embedding 向量
        print("\n[2/5] 提取 embedding 向量...")
        embeddings = extract_embeddings(model, node_ids_list)
        print(f"  向量维度: {embeddings.shape}")
        
        # Step 3: 使用 t-SNE 降维到 2D
        print("\n[3/5] 使用 t-SNE 降维到 2D...")
        tsne = TSNE(n_components=2, random_state=42, perplexity=30, max_iter=1000)
        reduced_embeddings = tsne.fit_transform(embeddings)
        print("  t-SNE 降维完成")
        
        # Step 4: 计算 FraudRank 分数
        print("\n[4/5] 计算 FraudRank 分数...")
        graph = load_weighted_graph(session, use_embedding_weights=False)
        init_scores = initialize_risk_seeds(session)
        fraud_scores = compute_fraud_rank(graph, init_scores, damping=0.85)
        
        # 获取节点大小依据
        if size_by == 'pagerank':
            print("  计算 PageRank 值...")
            pagerank_scores = compute_pagerank(graph)
            node_sizes = np.array([pagerank_scores.get(node_id, 0.0) for node_id in node_ids_list])
        else:
            node_sizes = np.array([graph["out_degree"].get(node_id, 0) for node_id in node_ids_list])
        
        # 归一化节点大小（用于可视化）
        if node_sizes.max() > 0:
            node_sizes = 50 + (node_sizes / node_sizes.max()) * 200
        else:
            node_sizes = np.full(len(node_ids_list), 50)
        
        # 获取 FraudScore（用于颜色）
        fraud_values = np.array([fraud_scores.get(node_id, 0.0) for node_id in node_ids_list])
        
        # Step 5: 可视化
        print("\n[5/5] 生成风险染色可视化...")
        fig, ax = plt.subplots(figsize=figsize)
        
        scatter = ax.scatter(
            reduced_embeddings[:, 0],
            reduced_embeddings[:, 1],
            c=fraud_values,
            s=node_sizes,
            cmap='RdYlGn_r',
            alpha=0.6,
            edgecolors='black',
            linewidths=0.3
        )
        
        ax.set_xlabel('t-SNE Dimension 1', fontsize=12)
        ax.set_ylabel('t-SNE Dimension 2', fontsize=12)
        ax.set_title('Risk Gradient View: FraudScore Distribution in Vector Space', fontsize=14, fontweight='bold')
        
        cbar = plt.colorbar(scatter, ax=ax)
        cbar.set_label('FraudScore (Red=High Risk, Green=Low Risk)', fontsize=10)
        
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if output_path:
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            print(f"  风险染色可视化已保存至: {output_path}")
        else:
            plt.show()
        
        plt.close()
        
        # 分析结果
        print("\n分析结果：")
        high_risk_indices = np.where(fraud_values >= 0.7)[0]
        low_risk_indices = np.where(fraud_values < 0.2)[0]
        
        if len(high_risk_indices) > 0:
            high_risk_coords = reduced_embeddings[high_risk_indices]
            high_risk_center = high_risk_coords.mean(axis=0)
            high_risk_std = high_risk_coords.std(axis=0)
            print(f"  高风险节点数: {len(high_risk_indices)}")
            print(f"  高风险节点中心位置: ({high_risk_center[0]:.2f}, {high_risk_center[1]:.2f})")
            print(f"  高风险节点分布标准差: ({high_risk_std[0]:.2f}, {high_risk_std[1]:.2f})")
            
            if high_risk_std.mean() < 5.0:
                print("  ✓ 发现：高风险节点形成聚集（红色斑块），可能存在团伙作案风险")
        
        if len(low_risk_indices) > 0 and len(high_risk_indices) > 0:
            low_risk_coords = reduced_embeddings[low_risk_indices]
            high_risk_coords = reduced_embeddings[high_risk_indices]
            
            distances = []
            for low_coord in low_risk_coords[:100]:
                dists = np.sqrt(((high_risk_coords - low_coord) ** 2).sum(axis=1))
                distances.append(dists.min())
            
            if distances:
                min_dist = min(distances)
                if min_dist < 2.0:
                    print(f"  ⚠ 警告：发现低风险节点与高风险节点距离很近（最小距离: {min_dist:.2f}）")
                    print("    这些节点需要重点人工复核（可能是漏网之鱼）")
        
        print("\n" + "=" * 60)
        print("风险染色视角分析完成！")
        print("=" * 60)
    
    finally:
        if should_release and session:
            session.release()


def implicit_link_view(
    session=None,
    output_path: Optional[str] = None,
    similarity_threshold: float = 0.9,
    limit: int = 10000,
    dimensions: int = 32,
    figsize: Tuple[int, int] = (14, 10),
    max_links: int = 500
):
    """
    视角三：隐式关联视角 (Implicit Link View)
    
    揭示"虽然没有直接连边，但在向量空间里却在一起"的关系
    
    Args:
        session: Nebula Graph session，如果为 None 则创建新 session
        output_path: 输出图片路径
        similarity_threshold: Embedding 相似度阈值（默认 0.9）
        limit: 查询边的数量限制
        dimensions: embedding 维度
        figsize: 图片大小
        max_links: 最多显示的隐式关联数量（避免图像过于拥挤）
    """
    should_release = False
    if session is None:
        session = get_nebula_session()
        should_release = True
    
    try:
        print("=" * 60)
        print("视角三：隐式关联视角 (Implicit Link View)")
        print("=" * 60)
        
        # Step 1: 获取节点 embeddings
        print("\n[1/5] 获取节点 embeddings...")
        model, node_to_type, node_ids_list = get_node_embeddings(
            session=session, limit=limit, dimensions=dimensions
        )
        print(f"  节点数: {len(node_ids_list)}")
        
        # Step 2: 提取 embedding 向量
        print("\n[2/5] 提取 embedding 向量...")
        embeddings = extract_embeddings(model, node_ids_list)
        print(f"  向量维度: {embeddings.shape}")
        
        # Step 3: 使用 t-SNE 降维到 2D
        print("\n[3/5] 使用 t-SNE 降维到 2D...")
        tsne = TSNE(n_components=2, random_state=42, perplexity=30, max_iter=1000)
        reduced_embeddings = tsne.fit_transform(embeddings)
        print("  t-SNE 降维完成")
        
        # Step 4: 计算 embedding 相似度并找出隐式关联
        print("\n[4/5] 计算隐式关联...")
        
        # 计算余弦相似度矩阵
        similarity_matrix = cosine_similarity(embeddings)
        
        # 获取原始图中的边
        original_edges = get_original_graph_edges(session)
        print(f"  原始图边数: {len(original_edges) // 2}")
        
        # 找出隐式关联（相似度高但不在原始图中相连）
        implicit_links = []
        node_id_to_idx = {node_id: idx for idx, node_id in enumerate(node_ids_list)}
        
        for i in range(len(node_ids_list)):
            for j in range(i + 1, len(node_ids_list)):
                node_i = node_ids_list[i]
                node_j = node_ids_list[j]
                
                similarity = similarity_matrix[i, j]
                
                if similarity >= similarity_threshold:
                    if (node_i, node_j) not in original_edges:
                        implicit_links.append((i, j, similarity))
        
        implicit_links.sort(key=lambda x: x[2], reverse=True)
        implicit_links = implicit_links[:max_links]
        
        print(f"  发现隐式关联数: {len(implicit_links)} (相似度 >= {similarity_threshold})")
        
        # Step 5: 可视化
        print("\n[5/5] 生成隐式关联可视化...")
        fig, ax = plt.subplots(figsize=figsize)
        
        # 绘制所有节点
        ax.scatter(
            reduced_embeddings[:, 0],
            reduced_embeddings[:, 1],
            c='lightgray',
            s=30,
            alpha=0.5,
            edgecolors='black',
            linewidths=0.2,
            label='Nodes'
        )
        
        # 绘制隐式关联（虚线）
        for i, j, similarity in implicit_links:
            x_coords = [reduced_embeddings[i, 0], reduced_embeddings[j, 0]]
            y_coords = [reduced_embeddings[i, 1], reduced_embeddings[j, 1]]
            ax.plot(
                x_coords,
                y_coords,
                '--',
                color='red',
                alpha=0.3,
                linewidth=0.5
            )
        
        # 高亮显示有隐式关联的节点
        linked_nodes = set()
        for i, j, _ in implicit_links:
            linked_nodes.add(i)
            linked_nodes.add(j)
        
        if linked_nodes:
            linked_indices = list(linked_nodes)
            ax.scatter(
                reduced_embeddings[linked_indices, 0],
                reduced_embeddings[linked_indices, 1],
                c='red',
                s=100,
                alpha=0.7,
                edgecolors='darkred',
                linewidths=1.5,
                label=f'Nodes with Implicit Links ({len(linked_nodes)})',
                zorder=10
            )
        
        ax.set_xlabel('t-SNE Dimension 1', fontsize=12)
        ax.set_ylabel('t-SNE Dimension 2', fontsize=12)
        ax.set_title(
            f'Implicit Link View: Implicit Links with Similarity >= {similarity_threshold}',
            fontsize=14,
            fontweight='bold'
        )
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        if output_path:
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            print(f"  隐式关联可视化已保存至: {output_path}")
        else:
            plt.show()
        
        plt.close()
        
        # 分析结果
        print("\n分析结果：")
        if implicit_links:
            print(f"  发现 {len(implicit_links)} 条隐式关联")
            print(f"  涉及 {len(linked_nodes)} 个节点")
            
            # 按节点类型分组统计
            type_groups = defaultdict(int)
            for idx in linked_nodes:
                node_id = node_ids_list[idx]
                node_type = node_to_type.get(node_id, 'Unknown')
                type_groups[node_type] += 1
            
            print("\n  隐式关联节点类型分布：")
            for node_type, count in sorted(type_groups.items(), key=lambda x: x[1], reverse=True):
                print(f"    {node_type}: {count}")
            
            print("\n  业务洞察：")
            print("    - 红色虚线表示隐式关联（结构相似但无直接连接）")
            print("    - 这些节点可能通过共同的关系人、交易对象等产生间接关联")
            print("    - 建议对这些隐式关联进行人工核查，可能存在潜在的团伙关系")
        else:
            print("  未发现满足条件的隐式关联")
        
        print("\n" + "=" * 60)
        print("隐式关联视角分析完成！")
        print("=" * 60)
    
    finally:
        if should_release and session:
            session.release()


def interactive_risk_exploration(
    session=None,
    output_path: Optional[str] = None,
    size_by: str = 'out_degree',
    similarity_threshold: float = 0.9,
    limit: int = 10000,
    dimensions: int = 32,
    max_links: int = 500
):
    """
    生成交互式 HTML 报告，结合风险染色和隐式关联两个视角
    
    Args:
        session: Nebula Graph session，如果为 None 则创建新 session
        output_path: 输出 HTML 文件路径
        size_by: 点的大小依据，'out_degree' 或 'pagerank'
        similarity_threshold: Embedding 相似度阈值（默认 0.9）
        limit: 查询边的数量限制
        dimensions: embedding 维度
        max_links: 最多显示的隐式关联数量
    """
    should_release = False
    if session is None:
        session = get_nebula_session()
        should_release = True
    
    try:
        print("=" * 60)
        print("Interactive Risk Exploration (Plotly)")
        print("=" * 60)
        
        # Step 1: 获取节点 embeddings
        print("\n[1/6] Getting node embeddings...")
        model, node_to_type, node_ids_list = get_node_embeddings(
            session=session, limit=limit, dimensions=dimensions
        )
        print(f"  Nodes: {len(node_ids_list)}")
        
        # Step 2: 提取 embedding 向量
        print("\n[2/6] Extracting embedding vectors...")
        embeddings = extract_embeddings(model, node_ids_list)
        print(f"  Embedding shape: {embeddings.shape}")
        
        # Step 3: 使用 t-SNE 降维到 2D
        print("\n[3/6] Reducing dimensions with t-SNE...")
        tsne = TSNE(n_components=2, random_state=42, perplexity=30, max_iter=1000)
        reduced_embeddings = tsne.fit_transform(embeddings)
        print("  t-SNE completed")
        
        # Step 4: 计算 FraudRank 分数
        print("\n[4/6] Computing FraudRank scores...")
        graph = load_weighted_graph(session, use_embedding_weights=False)
        init_scores = initialize_risk_seeds(session)
        fraud_scores = compute_fraud_rank(graph, init_scores, damping=0.85)
        
        # 获取节点大小依据
        if size_by == 'pagerank':
            print("  Computing PageRank values...")
            pagerank_scores = compute_pagerank(graph)
            node_sizes = np.array([pagerank_scores.get(node_id, 0.0) for node_id in node_ids_list])
        else:
            node_sizes = np.array([graph["out_degree"].get(node_id, 0) for node_id in node_ids_list])
        
        # 归一化节点大小到合理范围（5-15像素）
        if node_sizes.max() > 0:
            node_sizes_normalized = 5 + (node_sizes / node_sizes.max()) * 10
        else:
            node_sizes_normalized = np.full(len(node_ids_list), 8)
        
        # 获取 FraudScore
        fraud_values = np.array([fraud_scores.get(node_id, 0.0) for node_id in node_ids_list])
        
        # Step 5: 计算隐式关联
        print("\n[5/6] Computing implicit links...")
        similarity_matrix = cosine_similarity(embeddings)
        original_edges = get_original_graph_edges(session)
        
        implicit_links = []
        for i in range(len(node_ids_list)):
            for j in range(i + 1, len(node_ids_list)):
                node_i = node_ids_list[i]
                node_j = node_ids_list[j]
                similarity = similarity_matrix[i, j]
                
                if similarity >= similarity_threshold:
                    if (node_i, node_j) not in original_edges:
                        implicit_links.append((i, j, similarity))
        
        implicit_links.sort(key=lambda x: x[2], reverse=True)
        implicit_links = implicit_links[:max_links]
        print(f"  Found {len(implicit_links)} implicit links (similarity >= {similarity_threshold})")
        
        # Step 6: 创建交互式图表
        print("\n[6/6] Creating interactive visualization...")
        
        # 构造 DataFrame
        df = pd.DataFrame({
            'node_id': node_ids_list,
            'x': reduced_embeddings[:, 0],
            'y': reduced_embeddings[:, 1],
            'type': [node_to_type.get(nid, 'Unknown') for nid in node_ids_list],
            'score': fraud_values,
            'size': node_sizes_normalized,
            'out_degree': [graph["out_degree"].get(nid, 0) for nid in node_ids_list]
        })
        
        # 创建图表
        fig = go.Figure()
        
        # 添加隐式关联线（作为背景层）
        if implicit_links:
            link_x = []
            link_y = []
            link_info = []
            
            for i, j, sim in implicit_links:
                link_x.extend([reduced_embeddings[i, 0], reduced_embeddings[j, 0], None])
                link_y.extend([reduced_embeddings[i, 1], reduced_embeddings[j, 1], None])
                link_info.append(f"Similarity: {sim:.3f}")
            
            fig.add_trace(go.Scatter(
                x=link_x,
                y=link_y,
                mode='lines',
                name='Implicit Links',
                line=dict(color='rgba(255,0,0,0.2)', width=1, dash='dash'),
                hoverinfo='skip',
                showlegend=True
            ))
        
        # 按类型分组添加节点
        node_types = sorted(df['type'].unique())
        for idx, n_type in enumerate(node_types):
            sub_df = df[df['type'] == n_type]
            
            hover_template = (
                "<b>Node ID:</b> %{customdata[0]}<br>" +
                "<b>Type:</b> %{customdata[1]}<br>" +
                "<b>Fraud Score:</b> %{marker.color:.4f}<br>" +
                "<b>Out Degree:</b> %{customdata[2]}<br>" +
                "<extra></extra>"
            )
            
            # 只在最后一个 trace 显示 colorbar
            show_colorbar = (idx == len(node_types) - 1)
            
            fig.add_trace(go.Scatter(
                x=sub_df['x'],
                y=sub_df['y'],
                mode='markers',
                name=n_type,
                marker=dict(
                    size=sub_df['size'],
                    sizemode='diameter',
                    color=sub_df['score'],
                    colorscale='RdYlGn_r',
                    showscale=show_colorbar,
                    colorbar=dict(
                        title=dict(
                            text="Fraud Score",
                            font=dict(size=12)
                        )
                    ) if show_colorbar else None,
                    line=dict(width=0.5, color='DarkSlateGrey'),
                    opacity=0.7
                ),
                customdata=sub_df[['node_id', 'type', 'out_degree']].values,
                hovertemplate=hover_template
            ))
        
        # 布局设置
        fig.update_layout(
            title=dict(
                text="<b>Contract Risk Graph Embedding Space</b><br>" +
                     "<sup>Shape=Type, Color=Risk Score, Red Dashed Lines=Implicit Links</sup>",
                x=0.5,
                font=dict(size=16)
            ),
            xaxis=dict(
                title="t-SNE Dimension 1",
                showgrid=True,
                zeroline=False,
                gridcolor='rgba(200,200,200,0.3)'
            ),
            yaxis=dict(
                title="t-SNE Dimension 2",
                showgrid=True,
                zeroline=False,
                gridcolor='rgba(200,200,200,0.3)'
            ),
            plot_bgcolor='rgba(245,245,245,1)',
            width=1400,
            height=900,
            hovermode='closest',
            dragmode='pan',
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor='rgba(255,255,255,0.8)',
                bordercolor='rgba(0,0,0,0.2)',
                borderwidth=1
            )
        )
        
        # 添加交互工具
        fig.update_layout(
            modebar_add=['pan2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d', 'resetScale2d']
        )
        
        # 保存 HTML 文件
        if output_path is None:
            output_path = os.path.join(
                os.path.dirname(__file__), "../..", "reports",
                "interactive_risk_exploration.html"
            )
        
        fig.write_html(output_path)
        print(f"\nInteractive visualization saved to: {output_path}")
        print("Please open the HTML file in your browser to explore.")
        
        print("\n" + "=" * 60)
        print("Interactive visualization completed!")
        print("=" * 60)
    
    finally:
        if should_release and session:
            session.release()


def main():
    """主函数：生成两个可视化视角"""
    import argparse
    
    parser = argparse.ArgumentParser(description='高级可视化分析')
    parser.add_argument('--view', type=str, 
                       choices=['risk', 'implicit', 'all', 'interactive'], 
                       default='all', help='选择可视化视角')
    parser.add_argument('--size-by', type=str, choices=['out_degree', 'pagerank'],
                       default='out_degree', help='风险染色视角中点的大小依据')
    parser.add_argument('--similarity-threshold', type=float, default=0.9,
                       help='隐式关联视角的相似度阈值')
    parser.add_argument('--limit', type=int, default=10000, help='查询边的数量限制')
    parser.add_argument('--dimensions', type=int, default=32, help='embedding 维度')
    args = parser.parse_args()
    
    output_dir = os.path.join(os.path.dirname(__file__), "../..", "reports")
    os.makedirs(output_dir, exist_ok=True)
    
    session = None
    try:
        session = get_nebula_session()
        
        if args.view in ['risk', 'all']:
            risk_output = os.path.join(
                output_dir,
                f"risk_gradient_view_{args.size_by}.png"
            )
            risk_gradient_view(
                session=session,
                output_path=risk_output,
                size_by=args.size_by,
                limit=args.limit,
                dimensions=args.dimensions
            )
        
        if args.view in ['implicit', 'all']:
            implicit_output = os.path.join(
                output_dir,
                f"implicit_link_view_threshold_{args.similarity_threshold}.png"
            )
            implicit_link_view(
                session=session,
                output_path=implicit_output,
                similarity_threshold=args.similarity_threshold,
                limit=args.limit,
                dimensions=args.dimensions
            )
        
        if args.view == 'interactive':
            interactive_output = os.path.join(
                output_dir,
                "interactive_risk_exploration.html"
            )
            interactive_risk_exploration(
                session=session,
                output_path=interactive_output,
                size_by=args.size_by,
                similarity_threshold=args.similarity_threshold,
                limit=args.limit,
                dimensions=args.dimensions
            )
    
    finally:
        if session:
            session.release()


if __name__ == "__main__":
    main()

