import json
import os
import hashlib
from typing import Optional, Dict, Tuple

import networkx as nx
import pandas as pd
from node2vec import Node2Vec
from sklearn.metrics.pairwise import cosine_similarity
from src.utils.nebula_utils import execute_query, get_nebula_session

EDGE_WEIGHTS = {
    "CONTROLS": 0.8,
    "LEGAL_PERSON": 0.75,
    "PAYS": 0.65,
    "RECEIVES": 0.60,
    "TRADES_WITH": 0.50,
    "IS_SUPPLIER": 0.45,
    "IS_CUSTOMER": 0.40,
    "PARTY_A": 0.50,
    "PARTY_B": 0.50,
    "ADMIN_PENALTY_OF": 0.90,
    "BUSINESS_ABNORMAL_OF": 0.70,
}


def compute_edge_weights(session=None, limit=10000, business_weight=0.7, ai_weight=0.3):
    """
    使用 graph embedding 算法计算图中每条边的动态权重
    
    Args:
        session: Nebula Graph session，如果为 None 则创建新 session
        limit: 查询边的数量限制
        business_weight: 业务权重占比（默认 0.7）
        ai_weight: AI 相似度权重占比（默认 0.3）
    
    Returns:
        dict: {(src, dst): weight} 格式的边权重字典
    """
    should_release = False
    if session is None:
        session = get_nebula_session()
        should_release = True
    
    try:
        ngql = f"""
        MATCH (v)-[e]->(w) 
        RETURN id(v) as src, id(w) as dst, type(e) as edge_type 
        LIMIT {limit}
        """
        rows = execute_query(session, ngql)
        df = pd.DataFrame(rows)
        
        if df.empty:
            return {}
        
        G = nx.Graph()
        for _, row in df.iterrows():
            G.add_edge(row['src'], row['dst'], label=row['edge_type'])
        
        n2v = Node2Vec(G, dimensions=32, walk_length=10, num_walks=20, workers=4, p=1, q=0.5)
        model = n2v.fit(window=5, min_count=1, batch_words=4)
        
        def get_sim_scores(u, v):
            if u not in model.wv or v not in model.wv:
                return 0.0
            vec_u = model.wv[u].reshape(1, -1)
            vec_v = model.wv[v].reshape(1, -1)
            sim = cosine_similarity(vec_u, vec_v)[0][0]
            return (sim + 1) / 2
        
        final_edge_weights = {}
        
        for _, row in df.iterrows():
            u, v, e_type = row['src'], row['dst'], row['edge_type']
            
            base_weight = EDGE_WEIGHTS.get(e_type, 0.5)
            ai_sim = get_sim_scores(u, v)
            
            final_w = business_weight * base_weight + ai_weight * ai_sim
            final_edge_weights[(u, v)] = round(final_w, 4)
        
        return final_edge_weights
    
    finally:
        if should_release and session:
            session.release()


def save_edge_weights(weights: Dict[Tuple[str, str], float], filepath: str) -> bool:
    """
    Save edge weights to JSON file for persistence
    
    Args:
        weights: dict {(src, dst): weight}
        filepath: Path to save the JSON file
    
    Returns:
        bool: True if saved successfully
    """
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        # Convert tuple keys to string for JSON serialization
        serializable = {f"{k[0]}|||{k[1]}": v for k, v in weights.items()}
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(serializable, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"  ! 保存边权重失败: {e}")
        return False


def load_edge_weights(filepath: str) -> Optional[Dict[Tuple[str, str], float]]:
    """
    Load edge weights from JSON file
    
    Args:
        filepath: Path to the JSON file
    
    Returns:
        dict {(src, dst): weight} or None if file doesn't exist
    """
    if not os.path.exists(filepath):
        return None
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            serializable = json.load(f)
        # Convert string keys back to tuples
        weights = {}
        for k, v in serializable.items():
            parts = k.split("|||")
            if len(parts) == 2:
                weights[(parts[0], parts[1])] = v
        return weights
    except Exception as e:
        print(f"  ! 加载边权重失败: {e}")
        return None


def compute_graph_hash(session, limit: int = 10000) -> str:
    """
    Compute a hash of the graph structure to detect changes
    Used to invalidate cache when graph structure changes
    
    Args:
        session: Nebula Graph session
        limit: Query limit
    
    Returns:
        str: MD5 hash of graph structure
    """
    ngql = f"""
    MATCH (v)-[e]->(w) 
    RETURN id(v) as src, id(w) as dst, type(e) as edge_type 
    ORDER BY src, dst, edge_type
    LIMIT {limit}
    """
    rows = execute_query(session, ngql)
    
    # Create a deterministic string representation
    edges_str = "|".join([f"{r['src']}-{r['edge_type']}-{r['dst']}" for r in rows])
    return hashlib.md5(edges_str.encode()).hexdigest()


def get_or_compute_edge_weights(
    session=None,
    cache_dir: str = None,
    limit: int = 10000,
    force_recompute: bool = False,
    business_weight: float = 0.7,
    ai_weight: float = 0.3
) -> Dict[Tuple[str, str], float]:
    """
    Get edge weights from cache or compute if not available/outdated
    
    Args:
        session: Nebula Graph session
        cache_dir: Directory to store cache files
        limit: Query limit for edges
        force_recompute: Force recomputation even if cache exists
        business_weight: Business weight factor
        ai_weight: AI similarity weight factor
    
    Returns:
        dict: {(src, dst): weight}
    """
    if cache_dir is None:
        cache_dir = os.path.join(os.path.dirname(__file__), "../../cache")
    
    os.makedirs(cache_dir, exist_ok=True)
    
    weights_file = os.path.join(cache_dir, "edge_weights.json")
    hash_file = os.path.join(cache_dir, "graph_hash.txt")
    
    should_release = False
    if session is None:
        session = get_nebula_session()
        should_release = True
    
    try:
        # Compute current graph hash
        current_hash = compute_graph_hash(session, limit)
        
        # Check if cache is valid
        if not force_recompute and os.path.exists(weights_file) and os.path.exists(hash_file):
            with open(hash_file, 'r') as f:
                cached_hash = f.read().strip()
            
            if cached_hash == current_hash:
                weights = load_edge_weights(weights_file)
                if weights:
                    print(f"  使用缓存的边权重 (hash: {current_hash[:8]}...)")
                    return weights
        
        # Compute new weights
        print(f"  计算新的边权重 (hash: {current_hash[:8]}...)")
        weights = compute_edge_weights(
            session=session,
            limit=limit,
            business_weight=business_weight,
            ai_weight=ai_weight
        )
        
        # Save to cache
        save_edge_weights(weights, weights_file)
        with open(hash_file, 'w') as f:
            f.write(current_hash)
        
        return weights
    
    finally:
        if should_release and session:
            session.release()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="计算并缓存图边权重")
    parser.add_argument("--force", action="store_true", help="强制重新计算")
    parser.add_argument("--limit", type=int, default=10000, help="边数量限制")
    args = parser.parse_args()
    
    weights = get_or_compute_edge_weights(
        force_recompute=args.force,
        limit=args.limit
    )
    print(f"计算了 {len(weights)} 条边的权重")
    print("示例权重（前10条）：")
    for i, (edge, weight) in enumerate(list(weights.items())[:10]):
        print(f"  {edge}: {weight}")