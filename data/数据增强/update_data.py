#!/usr/bin/env python
"""
Use real-world company profiles to replace the placeholder entities inside the
央企穿透式监督 demo data so that contracts、争议和履约过程都能引用真实可查的主体。

数据来源：
1. 东方财富-巨潮资讯的 company profile 接口（通过 akshare.stock_profile_cninfo）
2. 针对少量央企集团（如中国建筑集团、国家电网等）依赖国务院国资委及公开年报信息整理的基础资料
"""
from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional

import akshare as ak
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
ENCODING = "utf-8-sig"

NAME_KEY = "\u516c\u53f8\u540d\u79f0"
LEGAL_KEY = "\u6cd5\u4eba\u4ee3\u8868"
FOUND_KEY = "\u6210\u7acb\u65e5\u671f"
SCOPE_KEY = "\u7ecf\u8425\u8303\u56f4"
INTRO_KEY = "\u516c\u53f8\u7b80\u4ecb"


def log(msg: str) -> None:
    print(msg, file=sys.stderr)


def clean_text(value: Optional[object]) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def fetch_profile(code: str, need_intro: bool = True, retries: int = 4) -> Mapping[str, str]:
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            df = ak.stock_profile_cninfo(code)
            if df.empty:
                raise RuntimeError(f"{code} profile is empty")
            row = df.iloc[0]
            info = {
                "name": clean_text(row.get(NAME_KEY)),
                "legal_rep": clean_text(row.get(LEGAL_KEY)),
                "found_date": clean_text(row.get(FOUND_KEY)),
                "biz_scope": clean_text(row.get(SCOPE_KEY)),
            }
            if need_intro:
                info["intro"] = clean_text(row.get(INTRO_KEY))
            return info
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            wait = 1 + attempt
            log(f"[retry {attempt+1}/{retries}] fetch {code} failed: {exc}, sleep {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Unable to fetch profile for {code}") from last_err


def resolve_entity(source: Mapping[str, object], *, need_intro: bool = False) -> Mapping[str, str]:
    if "code" in source:
        return fetch_profile(str(source["code"]), need_intro=need_intro)
    manual = source["manual"]
    return {
        "name": manual["name"],
        "legal_rep": manual["legal_rep"],
        "found_date": manual["found_date"],
        "biz_scope": manual.get("biz_scope", ""),
        "intro": manual.get("intro", ""),
    }


def update_customer_like(
    df: pd.DataFrame,
    sources: List[Mapping[str, object]],
    *,
    need_intro: bool = False,
) -> pd.DataFrame:
    df = df.sort_values("FID").copy()
    assert len(sources) == len(df), "source count mismatch"
    for (fid, source) in zip(df["FID"], sources):
        info = resolve_entity(source, need_intro=need_intro)
        df.loc[df["FID"] == fid, "FNAME"] = info["name"]
        df.loc[df["FID"] == fid, "FARTIFICIALPERSON"] = info["legal_rep"]
        df.loc[df["FID"] == fid, "FCREATETIME"] = info["found_date"]
        if "FBUSINESSSCOPE" in df.columns:
            df.loc[df["FID"] == fid, "FBUSINESSSCOPE"] = info["biz_scope"]
        if need_intro and "FDESCRIPTION" in df.columns:
            df.loc[df["FID"] == fid, "FDESCRIPTION"] = info["intro"]
    return df


CUSTOMER_SOURCES: List[Mapping[str, object]] = [
    {"code": "601668"},
    {"code": "601186"},
    {"code": "601390"},
    {"code": "601800"},
    {"code": "601618"},
    {"code": "601669"},
    {"code": "601868"},
    {"code": "601117"},
    {"code": "601789"},
    {"code": "601766"},
    {"code": "000002"},
    {"code": "600048"},
    {"code": "001979"},
    {"code": "000069"},
    {"code": "600606"},
    {"code": "600383"},
    {"code": "600325"},
    {"code": "600266"},
    {"code": "600208"},
    {"code": "600185"},
    {"code": "600340"},
    {"code": "002244"},
    {"code": "000656"},
    {"code": "000402"},
    {"code": "600376"},
    {"code": "600533"},
    {"code": "600663"},
    {"code": "000718"},
    {"code": "600246"},
    {
        "manual": {
            "name": "恒大地产集团有限公司",
            "legal_rep": "赵长龙",
            "found_date": "1996-06-24",
            "biz_scope": "房地产开发经营、商品房销售、物业管理、商业综合体运营以及建筑施工总承包等",
        }
    },
]

SUPPLIER_SOURCES: List[Mapping[str, object]] = [
    {"code": "600019"},
    {"code": "601600"},
    {"code": "601899"},
    {"code": "600362"},
    {"code": "600111"},
    {"code": "600585"},
    {"code": "000877"},
    {"code": "600970"},
    {"code": "000758"},
    {"code": "600176"},
    {"code": "600089"},
    {"code": "600320"},
    {"code": "000425"},
    {"code": "000157"},
    {"code": "600031"},
    {"code": "601727"},
    {"code": "600875"},
    {"code": "600406"},
    {"code": "600528"},
    {"code": "603993"},
    {"code": "002271"},
    {"code": "002410"},
    {"code": "002081"},
    {"code": "002318"},
    {"code": "002756"},
    {"code": "000786"},
    {"code": "002110"},
    {"code": "600660"},
    {"code": "000733"},
    {"code": "600309"},
]

COUNTERPART_SOURCES: List[Mapping[str, object]] = [
    {"code": "600629"},
    {"code": "603183"},
    {"code": "002051"},
    {"code": "300284"},
    {"code": "300675"},
    {"code": "300732"},
    {"code": "300746"},
    {"code": "300517"},
    {"code": "300500"},
    {"code": "003031"},
]

ORG_SOURCES: Dict[int, Mapping[str, object]] = {
    1: {
        "manual": {
            "name": "中国建筑集团有限公司",
            "legal_rep": "郑学选",
            "found_date": "1982-09-15",
            "intro": "中国建筑集团有限公司是国务院国资委监管的特大型建筑企业，业务覆盖房屋建筑、基础设施投资建设和工程总承包，2023年《财富》世界500强排名第13位。",
        }
    },
    2: {"code": "601668"},
    3: {"code": "002302"},
    4: {"code": "300425"},
    5: {
        "manual": {
            "name": "国家电网有限公司",
            "legal_rep": "张智刚",
            "found_date": "2002-12-29",
            "intro": "国家电网有限公司是关系国家能源安全和国民经济命脉的特大型国有骨干企业，承担着26个省份电网投资建设与运营，特高压输电技术居世界领先水平。",
        }
    },
    6: {"code": "600517"},
    7: {"code": "600131"},
    8: {"code": "000400"},
    9: {
        "manual": {
            "name": "中国石油天然气集团有限公司",
            "legal_rep": "戴厚良",
            "found_date": "1988-07-27",
            "intro": "中国石油天然气集团有限公司（CNPC）是我国最大的油气生产和供应企业，业务涵盖油气勘探开发、炼化销售、工程技术和新能源布局。",
        }
    },
    10: {"code": "601857"},
    11: {"code": "600339"},
    12: {"code": "000617"},
    13: {
        "manual": {
            "name": "中国中车集团有限公司",
            "legal_rep": "孙永才",
            "found_date": "2015-06-01",
            "intro": "中国中车集团有限公司由原中国北车与中国南车重组合并，专注轨道交通装备制造与系统解决方案，产品服务覆盖全球150多个国家和地区。",
        }
    },
    14: {"code": "601766"},
    15: {"code": "688187"},
    16: {"code": "600458"},
    17: {
        "manual": {
            "name": "国家能源投资集团有限责任公司",
            "legal_rep": "刘国跃",
            "found_date": "2017-11-28",
            "intro": "国家能源集团由神华集团与国电集团重组而成，是全球最大的煤炭生产商和火电运营商，在煤电路港航化一体化基础上积极布局新能源与储能。",
        }
    },
    18: {"code": "601088"},
    19: {"code": "600795"},
    20: {"code": "001289"},
}


def update_org(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for fid, source in ORG_SOURCES.items():
        info = resolve_entity(source, need_intro=True)
        mask = df["FID"] == fid
        if not mask.any():
            raise KeyError(f"ORG fid {fid} missing")
        df.loc[mask, "FNAME"] = info["name"]
        df.loc[mask, "FARTIFICIALPERSON"] = info["legal_rep"]
        df.loc[mask, "FESTABLISHMENTDATE"] = info["found_date"]
        df.loc[mask, "FDESCRIPTION"] = info["intro"]
    return df


def build_name_map(df: pd.DataFrame) -> Dict[int, str]:
    return {int(row.FID): str(row.FNAME) for row in df.itertuples()}


def update_party_names(
    df: pd.DataFrame,
    part_name_cols: Iterable[Mapping[str, str]],
    entity_maps: Mapping[str, Mapping[int, str]],
) -> pd.DataFrame:
    out = df.copy()
    for cfg in part_name_cols:
        id_col, name_col, type_col = cfg["id"], cfg["name"], cfg["type"]
        for row in out.itertuples():
            dtype = getattr(row, type_col)
            if pd.isna(dtype) or dtype not in entity_maps:
                continue
            entity_id = getattr(row, id_col)
            if pd.isna(entity_id):
                continue
            lookup = entity_maps[dtype]
            entity_name = lookup.get(int(entity_id))
            if entity_name:
                out.at[row.Index, name_col] = entity_name
    return out


def build_contract_text(row: pd.Series) -> str:
    party_a = row["FPARTANAME"]
    party_b = row["FPARTBNAME"]
    partner_type = row["FPBTYPE"]
    if partner_type == "bd_supplier":
        return f"建材采购合同-{party_b}"
    if partner_type == "bd_customer":
        return f"循环交易合同-{party_a}至{party_b}"
    if partner_type == "bos_org":
        return f"集团内部协同协议-{party_a}与{party_b}"
    if partner_type == "mscon_counterpart":
        return f"服务合作合同-{party_b}"
    return f"{party_a}-{party_b}合作协议"


def build_contract_desc(row: pd.Series) -> str:
    party_a = row["FPARTANAME"]
    party_b = row["FPARTBNAME"]
    contract_type = row["FPBTYPE"]
    if contract_type == "bd_supplier":
        return f"风险传导场景：{party_a}向{party_b}采购核心建材和设备。"
    if contract_type == "bd_customer":
        return f"风险传导场景：{party_a}与{party_b}开展循环交易，涉及资金闭环监控。"
    if contract_type == "bos_org":
        return f"{party_a}与{party_b}之间的内部协同或总分包合作协议。"
    if contract_type == "mscon_counterpart":
        return f"{party_a}委托{party_b}提供专业咨询/监理等第三方服务。"
    return "关键业务合同，需持续监测履约与资金流。"


CASE_TYPE_LABELS = {
    1: "合同纠纷",
    2: "劳动争议",
    3: "侵权责任",
    4: "债权债务",
    5: "工程款",
}

DISPUTE_TYPE_LABELS = {
    1: "合同履约争议",
    2: "质量争议",
    3: "付款争议",
    4: "工期争议",
}

CASE_STATUS_LABELS = {
    "N": "一审受理",
    "I": "立案审查",
    "J": "已判决",
}

DISPUTE_STATUS_LABELS = {
    "B": "待协商",
    "C": "处理中",
    "F": "已闭环",
}


def enrich_case_text(row: pd.Series, contract_lookup: Mapping[int, Mapping[str, str]]) -> Mapping[str, str]:
    relate_raw = row["FRELATECONTRACTID"]
    relate_id = None if pd.isna(relate_raw) else int(relate_raw)
    contract = contract_lookup.get(relate_id, {}) if relate_id is not None else {}
    party_a = contract.get("FPARTANAME", "甲方")
    party_b = contract.get("FPARTBNAME", "乙方")
    label = CASE_TYPE_LABELS.get(int(row["FCASETYPEID"]), "案件")
    amount = row["FLAWSUITAMOUNT"]
    status = CASE_STATUS_LABELS.get(str(row["FCASESTATUS"]), str(row["FCASESTATUS"]))
    name = f"{party_a}与{party_b}{label}案件"
    intro = f"案件简介：{party_a}与{party_b}围绕{label}产生纠纷，争议金额约{amount:,.2f}元。"
    detail = f"{party_a}指称{party_b}未按合同履约，引发{label}诉讼，涉案金额{amount:,.2f}元，当前状态为{status}。"
    return {"FNAME": name, "FINTRODUCTION": intro, "FDESCRIPTION": detail}


def enrich_dispute_text(row: pd.Series, contract_lookup: Mapping[int, Mapping[str, str]]) -> Mapping[str, str]:
    relate_raw = row["FRELATECONTRACTID"]
    relate_id = None if pd.isna(relate_raw) else int(relate_raw)
    contract = contract_lookup.get(relate_id, {}) if relate_id is not None else {}
    party_a = contract.get("FPARTANAME", "甲方")
    party_b = contract.get("FPARTBNAME", "乙方")
    label = DISPUTE_TYPE_LABELS.get(int(row["FCASETYPEID"]), "争议")
    amount = row["FDISPUTEAMOUNT"]
    status = DISPUTE_STATUS_LABELS.get(str(row["FDISPUTESTATUS"]), str(row["FDISPUTESTATUS"]))
    name = f"{label}-{party_a}与{party_b}"
    intro = f"纠纷简述：{party_a}与{party_b}就{label}产生分歧，争议金额约{amount:,.2f}元。"
    detail = f"涉及合同履约节点及资金结算，争议金额{amount:,.2f}元，当前状态为{status}。"
    return {"FNAME": name, "FDISPUTEINTRODUCTION": intro, "FDESCRIPTION": detail}


def main() -> None:
    customer_path = next(BASE_DIR.glob("t_bd_customer_*.csv"))
    supplier_path = next(BASE_DIR.glob("t_bd_supplier_*.csv"))
    counterpart_path = next(BASE_DIR.glob("t_mscon_counterpart_*.csv"))
    org_path = next(BASE_DIR.glob("t_org_org_*.csv"))

    customer_df = update_customer_like(pd.read_csv(customer_path), CUSTOMER_SOURCES)
    supplier_df = update_customer_like(pd.read_csv(supplier_path), SUPPLIER_SOURCES)
    counterpart_df = update_customer_like(pd.read_csv(counterpart_path), COUNTERPART_SOURCES)
    org_df = update_org(pd.read_csv(org_path))

    customer_df.to_csv(customer_path, index=False, encoding=ENCODING)
    supplier_df.to_csv(supplier_path, index=False, encoding=ENCODING)
    counterpart_df.to_csv(counterpart_path, index=False, encoding=ENCODING)
    org_df.to_csv(org_path, index=False, encoding=ENCODING)

    name_maps = {
        "bd_customer": build_name_map(customer_df),
        "bd_supplier": build_name_map(supplier_df),
        "bos_org": build_name_map(org_df),
        "mscon_counterpart": build_name_map(counterpart_df),
    }

    part_cfgs = [
        {"id": "FPARTAID", "name": "FPARTANAME", "type": "FPATYPE"},
        {"id": "FPARTBID", "name": "FPARTBNAME", "type": "FPBTYPE"},
        {"id": "FPARTCID", "name": "FPARTCNAME", "type": "FPCTYPE"},
        {"id": "FPARTDID", "name": "FPARTDNAME", "type": "FPDTYPE"},
    ]

    contract_path = next(BASE_DIR.glob("t_mscon_contract_*.csv"))
    contract_df = pd.read_csv(contract_path)
    contract_df = update_party_names(contract_df, part_cfgs, name_maps)
    contract_df["FBILLNAME"] = contract_df.apply(build_contract_text, axis=1)
    contract_df["FDESCRIPTION"] = contract_df.apply(build_contract_desc, axis=1)
    contract_df.to_csv(contract_path, index=False, encoding=ENCODING)

    contract_lookup = {
        int(row.FID): {
            "FBILLNAME": row.FBILLNAME,
            "FPARTANAME": row.FPARTANAME,
            "FPARTBNAME": row.FPARTBNAME,
        }
        for row in contract_df.itertuples()
    }

    def map_by(series: pd.Series, lookup: Mapping[int, str], fallback_series: pd.Series) -> pd.Series:
        def _mapper(val, fallback):
            if pd.isna(val):
                return fallback
            return lookup.get(int(val), fallback)

        return pd.Series([_mapper(val, fb) for val, fb in zip(series, fallback_series)], index=series.index)

    in_path = next(BASE_DIR.glob("t_mscon_performplanin_*.csv"))
    plan_in_df = pd.read_csv(in_path)
    plan_in_df["FPARTANAME"] = map_by(plan_in_df["FPARTAID"], name_maps["bos_org"], plan_in_df["FPARTANAME"])
    plan_in_df["FPARTBNAME"] = map_by(plan_in_df["FPARTBID"], name_maps["bd_supplier"], plan_in_df["FPARTBNAME"])
    plan_in_df.to_csv(in_path, index=False, encoding=ENCODING)

    out_path = next(BASE_DIR.glob("t_mscon_performplanout_*.csv"))
    plan_out_df = pd.read_csv(out_path)
    plan_out_df["FPARTANAME"] = map_by(plan_out_df["FPARTAID"], name_maps["bos_org"], plan_out_df["FPARTANAME"])
    plan_out_df["FPARTBNAME"] = map_by(plan_out_df["FPARTBID"], name_maps["bd_supplier"], plan_out_df["FPARTBNAME"])
    plan_out_df.to_csv(out_path, index=False, encoding=ENCODING)

    case_path = next(BASE_DIR.glob("t_conl_case_*.csv"))
    case_df = pd.read_csv(case_path)
    evergrande_case_links = {9: 90, 10: 99}
    for fid, contract_id in evergrande_case_links.items():
        mask = case_df["FID"] == fid
        case_df.loc[mask, "FRELATECONTRACTID"] = contract_id
    case_df["FRELATECONTRACTNAME"] = case_df["FRELATECONTRACTID"].map(
        lambda x: contract_lookup.get(int(x), {}).get("FBILLNAME", "")
    )
    enriched_cases = case_df.apply(lambda r: enrich_case_text(r, contract_lookup), axis=1, result_type="expand")
    for col in ["FNAME", "FINTRODUCTION", "FDESCRIPTION"]:
        case_df[col] = enriched_cases[col]
    case_df.to_csv(case_path, index=False, encoding=ENCODING)

    dispute_path = next(BASE_DIR.glob("t_conl_disputeregist_*.csv"))
    dispute_df = pd.read_csv(dispute_path)
    evergrande_dispute_links = {9: 90, 10: 99}
    for fid, contract_id in evergrande_dispute_links.items():
        mask = dispute_df["FID"] == fid
        dispute_df.loc[mask, "FRELATECONTRACTID"] = contract_id
        dispute_df.loc[mask, "FCASETYPEID"] = 3
        dispute_df.loc[mask, "FDISPUTESTATUS"] = "F"
    dispute_df["FRELATECONTRACTNAME"] = dispute_df["FRELATECONTRACTID"].map(
        lambda x: contract_lookup.get(int(x), {}).get("FBILLNAME", "")
    )
    enriched_disputes = dispute_df.apply(lambda r: enrich_dispute_text(r, contract_lookup), axis=1, result_type="expand")
    for col in ["FNAME", "FDISPUTEINTRODUCTION", "FDESCRIPTION"]:
        dispute_df[col] = enriched_disputes[col]
    dispute_df.to_csv(dispute_path, index=False, encoding=ENCODING)


if __name__ == "__main__":
    main()
