import time
import json
import base64
import hmac
from datetime import datetime
import random
import string
import requests
from hashlib import sha256
from src.settings import settings

CLIENT_ID = settings.daas_config["client_id"]
CLIENT_SECRET = settings.daas_config["client_secret"]
BASE_URL = settings.daas_config["base_url"]
API_PATH = {
    "getCompanyPartner": "/icBaseServer/company/getCompanyPartner",
    "年报股东": "/kddi/company/getAnnualReportPartner",
    "企业基本信息清洗后字段": "/kddi/company/getCompanyBaseClean",
    "企业主要人员信息查询": "/kddi/company/getCompanyEmployee",
}

def query(type: str, payload: dict):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nonce = "".join(random.choices(string.digits, k=10))
    s = timestamp + nonce
    h = hmac.new(CLIENT_SECRET.encode("utf-8"), s.encode("utf-8"), sha256)
    sign = base64.b64encode(h.digest()).decode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "x-acgw-appid": CLIENT_ID,
        "x-acgw-timestamp": timestamp,
        "x-acgw-nonce": nonce,
        "x-acgw-sign": sign,
    }
    url = BASE_URL + API_PATH[type]
    response = requests.post(url, headers=headers, json=payload)
    return response.json()

if __name__ == "__main__":
    payload = {
        "employee_name": "朱光",
        "person_id": "f443259b7c6a385b5b49d26e67b0524b"
    }
    result = query("企业主要人员信息查询", payload)
    print(result)