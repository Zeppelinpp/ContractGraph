from fastapi import FastAPI, Request
from src.analysis import compute_fraud_rank, find_circular_trades_for_company, identify_shell_networks, detect_collusion_networks

app = FastAPI(
    title="央企穿透式监督知识图谱API",
    version="0.1.0",
)

@app.post("/api/fraud-rank")
async def fraud_rank(request: Request):
    data = await request.json()
    # 获取


@app.post("/api/circular-trades")
async def circular_trades(request: Request):
    data = await request.json()
    # 获取循环交易

@app.post("/api/shell-company")
async def shell_company(request: Request):
    data = await request.json()
    # 获取空壳公司

@app.post("/api/collusion")
async def collusion(request: Request):
    data = await request.json()
    # 获取关联方串通网络