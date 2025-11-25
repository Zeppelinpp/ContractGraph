"""
Graph Embedding 可视化模块

使用 PCA 或其他降维方法将 graph embedding 降维到 2D/3D 并进行可视化
"""

import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from node2vec import Node2Vec
from typing import Dict, List, Optional, Tuple
from src.utils.nebula_utils import execute_query, get_nebula_session


def get_node_embeddings(session=None, limit=10000, dimensions=32):
    """
    从 Nebula Graph 构建图并训练 Node2Vec 模型，返回节点 embedding 向量
    
    Args:
        session: Nebula Graph session，如果为 None 则创建新 session
        limit: 查询边的数量限制
        dimensions: embedding 维度
    
    Returns:
        tuple: (model, node_to_type_dict, node_ids_list)
            - model: 训练好的 Node2Vec 模型
            - node_to_type_dict: {node_id: node_type} 映射
            - node_ids_list: 节点ID列表（与向量顺序对应）
    """
    should_release = False
    if session is None:
        session = get_nebula_session()
        should_release = True
    
    try:
        # 查询所有边
        ngql = f"""
        MATCH (v)-[e]->(w) 
        RETURN id(v) as src, id(w) as dst, type(e) as edge_type,
               tags(v)[0] as src_type, tags(w)[0] as dst_type
        LIMIT {limit}
        """
        rows = execute_query(session, ngql)
        
        if not rows:
            raise ValueError("未查询到图数据")
        
        # 构建 NetworkX 图
        G = nx.Graph()
        node_to_type = {}
        
        for row in rows:
            src = row.get('src', '')
            dst = row.get('dst', '')
            src_type = row.get('src_type', 'Unknown')
            dst_type = row.get('dst_type', 'Unknown')
            
            if src and dst:
                G.add_edge(src, dst)
                node_to_type[src] = src_type
                node_to_type[dst] = dst_type
        
        # 训练 Node2Vec 模型
        print(f"训练 Node2Vec 模型（节点数: {len(G.nodes())}, 边数: {len(G.edges())}）...")
        n2v = Node2Vec(G, dimensions=dimensions, walk_length=10, num_walks=20, workers=4, p=1, q=0.5)
        model = n2v.fit(window=5, min_count=1, batch_words=4)
        
        # 获取所有节点的ID列表（按模型中的顺序）
        node_ids_list = list(model.wv.index_to_key)
        
        return model, node_to_type, node_ids_list
    
    finally:
        if should_release and session:
            session.release()


def extract_embeddings(model, node_ids_list: List[str]) -> np.ndarray:
    """
    从模型中提取所有节点的 embedding 向量
    
    Args:
        model: Node2Vec 模型
        node_ids_list: 节点ID列表
    
    Returns:
        np.ndarray: shape (n_nodes, dimensions) 的向量矩阵
    """
    embeddings = []
    for node_id in node_ids_list:
        if node_id in model.wv:
            embeddings.append(model.wv[node_id])
        else:
            # 如果节点不在模型中，使用零向量
            embeddings.append(np.zeros(model.wv.vector_size))
    
    return np.array(embeddings)


def reduce_dimensions(embeddings: np.ndarray, n_components: int = 2, method: str = 'pca') -> np.ndarray:
    """
    使用降维方法将高维向量降维到 2D 或 3D
    
    Args:
        embeddings: shape (n_nodes, dimensions) 的向量矩阵
        n_components: 降维后的维度（2 或 3）
        method: 降维方法，'pca' 或 'tsne'
    
    Returns:
        np.ndarray: shape (n_nodes, n_components) 的降维后向量
    """
    if method.lower() == 'pca':
        reducer = PCA(n_components=n_components, random_state=42)
        reduced = reducer.fit_transform(embeddings)
        explained_variance = sum(reducer.explained_variance_ratio_)
        print(f"PCA 降维完成，解释方差比例: {explained_variance:.2%}")
        return reduced
    
    elif method.lower() == 'tsne':
        print("使用 t-SNE 降维（可能需要较长时间）...")
        reducer = TSNE(n_components=n_components, random_state=42, perplexity=30, max_iter=1000)
        reduced = reducer.fit_transform(embeddings)
        print("t-SNE 降维完成")
        return reduced
    
    else:
        raise ValueError(f"不支持的降维方法: {method}，请使用 'pca' 或 'tsne'")


def visualize_embeddings_2d(
    reduced_embeddings: np.ndarray,
    node_ids_list: List[str],
    node_to_type: Dict[str, str],
    output_path: Optional[str] = None,
    figsize: Tuple[int, int] = (12, 8),
    alpha: float = 0.6,
    point_size: float = 20.0
):
    """
    可视化 2D embedding
    
    Args:
        reduced_embeddings: shape (n_nodes, 2) 的降维后向量
        node_ids_list: 节点ID列表
        node_to_type: {node_id: node_type} 映射
        output_path: 输出图片路径，如果为 None 则显示而不保存
        figsize: 图片大小
        alpha: 点的透明度
        point_size: 点的大小
    """
    # 节点类型颜色映射
    node_types = ['Company', 'Person', 'Contract', 'LegalEvent', 'Transaction', 'Unknown']
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8', '#D3D3D3']
    type_to_color = dict(zip(node_types, colors))
    
    # 创建图形
    fig, ax = plt.subplots(figsize=figsize)
    
    # 按类型分组绘制
    for node_type in node_types:
        indices = [i for i, node_id in enumerate(node_ids_list) 
                  if node_to_type.get(node_id, 'Unknown') == node_type]
        
        if indices:
            x_coords = reduced_embeddings[indices, 0]
            y_coords = reduced_embeddings[indices, 1]
            color = type_to_color.get(node_type, '#D3D3D3')
            ax.scatter(x_coords, y_coords, c=color, label=node_type, 
                      alpha=alpha, s=point_size, edgecolors='black', linewidths=0.5)
    
    ax.set_xlabel('第一主成分', fontsize=12)
    ax.set_ylabel('第二主成分', fontsize=12)
    ax.set_title('Graph Embedding 2D 可视化', fontsize=14, fontweight='bold')
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"2D 可视化已保存至: {output_path}")
    else:
        plt.show()
    
    plt.close()


def visualize_embeddings_3d(
    reduced_embeddings: np.ndarray,
    node_ids_list: List[str],
    node_to_type: Dict[str, str],
    output_path: Optional[str] = None,
    figsize: Tuple[int, int] = (14, 10),
    alpha: float = 0.6,
    point_size: float = 20.0
):
    """
    可视化 3D embedding
    
    Args:
        reduced_embeddings: shape (n_nodes, 3) 的降维后向量
        node_ids_list: 节点ID列表
        node_to_type: {node_id: node_type} 映射
        output_path: 输出图片路径，如果为 None 则显示而不保存
        figsize: 图片大小
        alpha: 点的透明度
        point_size: 点的大小
    """
    from mpl_toolkits.mplot3d import Axes3D
    
    # 节点类型颜色映射
    node_types = ['Company', 'Person', 'Contract', 'LegalEvent', 'Transaction', 'Unknown']
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8', '#D3D3D3']
    type_to_color = dict(zip(node_types, colors))
    
    # 创建 3D 图形
    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111, projection='3d')
    
    # 按类型分组绘制
    for node_type in node_types:
        indices = [i for i, node_id in enumerate(node_ids_list) 
                  if node_to_type.get(node_id, 'Unknown') == node_type]
        
        if indices:
            x_coords = reduced_embeddings[indices, 0]
            y_coords = reduced_embeddings[indices, 1]
            z_coords = reduced_embeddings[indices, 2]
            color = type_to_color.get(node_type, '#D3D3D3')
            ax.scatter(x_coords, y_coords, z_coords, c=color, label=node_type,
                      alpha=alpha, s=point_size, edgecolors='black', linewidths=0.5)
    
    ax.set_xlabel('第一主成分', fontsize=12)
    ax.set_ylabel('第二主成分', fontsize=12)
    ax.set_zlabel('第三主成分', fontsize=12)
    ax.set_title('Graph Embedding 3D 可视化', fontsize=14, fontweight='bold')
    ax.legend(loc='best', fontsize=10)
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"3D 可视化已保存至: {output_path}")
    else:
        plt.show()
    
    plt.close()


def visualize_graph_embedding(
    session=None,
    n_components: int = 2,
    method: str = 'pca',
    output_path: Optional[str] = None,
    limit: int = 10000,
    dimensions: int = 32
):
    """
    完整的 graph embedding 可视化流程
    
    Args:
        session: Nebula Graph session，如果为 None 则创建新 session
        n_components: 降维后的维度（2 或 3）
        method: 降维方法，'pca' 或 'tsne'
        output_path: 输出图片路径，如果为 None 则显示而不保存
        limit: 查询边的数量限制
        dimensions: embedding 维度
    """
    print("=" * 60)
    print("Graph Embedding 可视化")
    print("=" * 60)
    
    # Step 1: 获取节点 embeddings
    print("\n[1/4] 获取节点 embeddings...")
    model, node_to_type, node_ids_list = get_node_embeddings(
        session=session, limit=limit, dimensions=dimensions
    )
    print(f"  节点数: {len(node_ids_list)}")
    
    # Step 2: 提取 embedding 向量
    print("\n[2/4] 提取 embedding 向量...")
    embeddings = extract_embeddings(model, node_ids_list)
    print(f"  向量维度: {embeddings.shape}")
    
    # Step 3: 降维
    print(f"\n[3/4] 使用 {method.upper()} 降维到 {n_components}D...")
    reduced_embeddings = reduce_dimensions(embeddings, n_components=n_components, method=method)
    
    # Step 4: 可视化
    print(f"\n[4/4] 生成 {n_components}D 可视化...")
    if n_components == 2:
        visualize_embeddings_2d(reduced_embeddings, node_ids_list, node_to_type, output_path)
    elif n_components == 3:
        visualize_embeddings_3d(reduced_embeddings, node_ids_list, node_to_type, output_path)
    else:
        raise ValueError("n_components 必须是 2 或 3")
    
    print("\n" + "=" * 60)
    print("可视化完成！")
    print("=" * 60)


if __name__ == "__main__":
    import os
    import argparse

    parser = argparse.ArgumentParser(description='Graph Embedding Visualization')
    parser.add_argument('--method', type=str, default='pca', help='降维方法，pca 或 tsne')
    parser.add_argument('--n_components', type=int, default=2, help='降维后的维度，2 或 3')
    parser.add_argument('--limit', type=int, default=10000, help='查询边的数量限制')
    parser.add_argument('--dimensions', type=int, default=32, help='embedding 维度')
    args = parser.parse_args()
    
    # 创建输出目录
    output_dir = os.path.join(os.path.dirname(__file__), "../..", "reports")
    os.makedirs(output_dir, exist_ok=True)

    # 生成 可视化
    print(f"\n生成 {args.n_components}D {args.method} 可视化...")
    visualize_graph_embedding(
        session=None,
        n_components=args.n_components,
        method=args.method,
        output_path=os.path.join(output_dir, f"embedding_{args.n_components}d_{args.method}_{args.dimensions}.png"),
        limit=args.limit,
        dimensions=args.dimensions
    )
    
