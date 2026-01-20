import os
import re
import time
import json
import requests
from typing import Iterable, Union, List, Dict, Any

SEC_HEADERS = {
    "User-Agent": "TaeyeongJung research bot (tyjung1024@gmail.com)",  
    "Accept-Encoding": "gzip, deflate",
}

def _safe_filename(s: str) -> str:
    s = re.sub(r'[\\/*?:"<>|]', "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _unique_path(path: str) -> str:
    if not os.path.exists(path):
        return path
    base, ext = os.path.splitext(path)
    i = 1
    while True:
        cand = f"{base}_{i}{ext}"
        if not os.path.exists(cand):
            return cand
        i += 1

def _normalize_tickers(tickers: Union[str, Iterable[str]]) -> List[str]:
    if isinstance(tickers, str):
        # "NVDA, MSFT TSLA" 형태도 허용
        raw = re.split(r"[,\s]+", tickers.strip())
        tickers_list = [t for t in raw if t]
    else:
        tickers_list = [str(t).strip() for t in tickers if str(t).strip()]
    # 중복 제거(순서 유지)
    seen = set()
    out = []
    for t in tickers_list:
        u = t.upper()
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def get_cik_from_ticker(ticker: str) -> str:
    url = "https://www.sec.gov/files/company_tickers.json"
    r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()

    t = ticker.strip().upper()
    for _, row in data.items():
        if row["ticker"].upper() == t:
            return str(row["cik_str"]).zfill(10)
    raise ValueError(f"Ticker not found in SEC mapping: {ticker}")

def get_latest_annual_info(cik10: str, preferred_forms=("10-K", "20-F", "40-F", "10-K/A", "20-F/A")) -> dict:
    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    r = requests.get(url, headers=SEC_HEADERS, timeout=30)
    r.raise_for_status()
    sub = r.json()

    recent = sub.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accession_numbers = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])
    filing_dates = recent.get("filingDate", [])

    # preferred_forms 순서대로 “가장 최신”을 하나 찾음
    for want in preferred_forms:
        for i, f in enumerate(forms):
            if f == want:
                return {
                    "companyName": sub.get("name", ""),
                    "cik10": cik10,
                    "form": want,
                    "accession": accession_numbers[i],
                    "primaryDocument": primary_docs[i],
                    "filingDate": filing_dates[i],
                }

    raise ValueError(f"No annual filing found (tried {preferred_forms}) for CIK {cik10}")

def download_latest_10k(
    ticker: str,
    save_folder: str,
    kind: str = "primary_html",  # "primary_html" or "full_submission_zip"
    make_subfolder_per_ticker: bool = True,
    sleep_sec: float = 0.25
) -> str:
    ticker = ticker.strip().upper()

    save_folder = os.path.expanduser(save_folder)
    if make_subfolder_per_ticker:
        save_folder = os.path.join(save_folder, ticker)
    os.makedirs(save_folder, exist_ok=True)

    cik10 = get_cik_from_ticker(ticker)
    info = get_latest_annual_info(cik10)

    accession_no_dashes = info["accession"].replace("-", "")
    cik_no_zeros = str(int(cik10))
    base_dir = f"https://www.sec.gov/Archives/edgar/data/{cik_no_zeros}/{accession_no_dashes}/"

    if kind == "primary_html":
        url = base_dir + info["primaryDocument"]
        ext = os.path.splitext(info["primaryDocument"])[1] or ".html"
        filename = _safe_filename(f"{ticker}_10-K_{info['filingDate']}{ext}")
    elif kind == "full_submission_zip":
        url = base_dir + f"{accession_no_dashes}.zip"
        filename = _safe_filename(f"{ticker}_10-K_{info['filingDate']}.zip")
    else:
        raise ValueError("kind must be 'primary_html' or 'full_submission_zip'")

    out_path = _unique_path(os.path.join(save_folder, filename))

    time.sleep(max(0.0, sleep_sec))
    r = requests.get(url, headers=SEC_HEADERS, timeout=60)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)

    meta_path = _unique_path(os.path.join(save_folder, _safe_filename(f"{ticker}_10-K_{info['filingDate']}_meta.json")))
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(info, f, ensure_ascii=False, indent=2)

    return out_path

def batch_download_10k(
    tickers: Union[str, Iterable[str]],
    save_folder: str,
    kind: str = "primary_html",
    make_subfolder_per_ticker: bool = True,
    sleep_sec: float = 0.25
) -> Dict[str, Any]:
    """
    반환:
      {
        "saved": { "NVDA": "/path/...", ... },
        "failed": { "BAD": "error msg", ... }
      }
    """
    tickers_list = _normalize_tickers(tickers)

    results = {"saved": {}, "failed": {}}
    for t in tickers_list:
        try:
            path = download_latest_10k(
                t, save_folder,
                kind=kind,
                make_subfolder_per_ticker=make_subfolder_per_ticker,
                sleep_sec=sleep_sec
            )
            results["saved"][t] = path
            print(f"[OK] {t} -> {path}")
        except Exception as e:
            results["failed"][t] = str(e)
            print(f"[FAIL] {t} -> {e}")
    return results

if __name__ == "__main__":
    # res = batch_download_10k(["NVDA", "MSFT", "TSLA", "ONON", "AMZN", "TSM", "GOOG", "NKE"], "./SEC_10K")
    res = batch_download_10k(["LRCX", "SOXX", "IAUM", "TEM", "PLTR", "BRK.B"], "./SEC_10K")
    

    # 예시 2) 문자열로도 가능
    # res = batch_download_10k("NVDA, MSFT TSLA", "./SEC_10K", make_subfolder_per_ticker=False)

    print("\n=== Summary ===")
    print("Saved:", len(res["saved"]))
    print("Failed:", len(res["failed"]))
    if res["failed"]:
        print("Failed tickers:", res["failed"])