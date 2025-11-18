"""
Nebula Graph 查询工具模块

提供统一的 Nebula Graph 连接和查询接口
"""

from nebula3.gclient.net import ConnectionPool, Session
from nebula3.Config import Config
from src.settings import settings


def get_nebula_session() -> Session:
    """获取 Nebula Graph session"""
    config = Config()
    config.max_connection_pool_size = 10
    
    connection_pool = ConnectionPool()
    ok = connection_pool.init(
        [(settings.nebula_config["host"], settings.nebula_config["port"])], 
        config
    )
    if not ok:
        raise Exception("Failed to initialize Nebula connection pool")
    
    session = connection_pool.get_session(
        settings.nebula_config["user"], 
        settings.nebula_config["password"]
    )
    
    # 切换到指定 space
    result = session.execute(f"USE {settings.nebula_config['space']}")
    if not result.is_succeeded():
        raise Exception(f"Failed to use space: {result.error_msg()}")
    
    return session


def execute_query(session: Session, query: str):
    """
    执行查询并返回结果列表
    
    Args:
        session: Nebula session
        query: nGQL 查询语句
    
    Returns:
        list: 包含字典的列表，每个字典代表一行结果
    """
    result = session.execute(query)
    if not result.is_succeeded():
        raise RuntimeError(f"查询失败: {result.error_msg()}\nQuery: {query}")
    
    rows = result.as_primitive()
    return rows if rows else []

