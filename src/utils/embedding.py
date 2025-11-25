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


if __name__ == "__main__":
    weights = compute_edge_weights()
    print(f"计算了 {len(weights)} 条边的权重")
    print("示例权重（前10条）：")
    for i, (edge, weight) in enumerate(list(weights.items())[:10]):
        print(f"  {edge}: {weight}")