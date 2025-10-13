import os
import time
from bs4 import BeautifulSoup
import requests
from supabase import create_client, Client
import random
import argparse
import re
from google.cloud import storage

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm
import json

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "./first-project-438808-dc1804307b11.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko,en;q=0.9",
}

# ===== Selenium 드라이버 =====
def build_driver(headless=True, user_agent=None):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.page_load_strategy = "eager"
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1200,2000")
    opts.add_argument("--lang=ko-KR")
    # 리소스 최소화(이미지/CSS 차단)
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.cookies": 1,
        "profile.managed_default_content_settings.javascript": 1,
    }
    opts.add_experimental_option("prefs", prefs)
    if user_agent:
        opts.add_argument(f"--user-agent={user_agent}")
    driver = webdriver.Chrome(options=opts)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        })
    except Exception:
        pass
    driver.set_page_load_timeout(8)
    return driver

def make_requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=64, max_retries=2)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def get_html(driver, url: str, wait_sec: int = 6) -> str:
    driver.get(url)
    try:
        WebDriverWait(driver, wait_sec).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "script#\\__NEXT_DATA__"))
        )
    except Exception:
        print("except")
        WebDriverWait(driver, wait_sec).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(0.06)
    return driver.page_source


def gcs_bucket(name: str = "vton-mss"):
    return storage.Client().bucket(name)

def upload_json_item(bucket, data, folder1: str, filename: str):
    path = f"{folder1}/{filename}"
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(data, ensure_ascii=False, indent=2),
                            content_type="application/json")

def extract_from_url(url: str, gender: str, types: str, index: int, totals, bucket):
    try:
        # opts = Options()
        # opts.add_argument("--headless=new")
        # driver = webdriver.Chrome(options=opts)
        ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36")
        driver = build_driver(headless=True, user_agent=ua)
        driver.get(url)
        # 첫 로딩 기다리기
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/snap/']"))
        )
    except Exception as e:
        print(f"[URL : {url}]\nError while loading first driver - {e}")
        return totals, 0

    # 스크롤을 여러 번 내려서 추가 로딩
    total_num = 0
    try:
        count = 0
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            text = soup.find("div", attrs={"class": "sc-77827aa3-0"}).find("span", attrs={"class": "text-body_13px_reg"}).text
            total_num = int(re.sub("개|,", '', text))
            asa = soup.find_all("a")
            hrefs = [a.get("href") for a in asa if a.get("href")]
            datas = [{
                "url": h,
                "gender": gender,
                "types": types,
            } for h in hrefs if "/snap/" in h]
            totals.extend(datas)
            print(f"{total_num}개, datas: {len(datas)}, {datas[0]}")
            
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)  # 로딩 대기
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
            count += 1

            if count % 20 == 19:
                with open(f"second_data_{index}.json", "w") as f:
                    json.dump(totals, f)
                upload_json_item(bucket, totals, "snap_temporary", f"second_data_{index}.json")
        # driver.quit()
        return totals, total_num
    except Exception as e:
        print(f"[URL : {url}]\nError while crawling - {e}")
        # driver.quit()
        return totals, total_num

def main(index: int, supabase_url: str, supabase_key: str):
    # 일단 DB에서 아직 처리되지 않은 URL 하나를 가져온다.
    supabase = create_client(supabase_url, supabase_key)

    is_finished = False
    totals = []
    bucket = gcs_bucket()
    total_count = 0
    while not is_finished:
        print(f"\n\nTotal_count: {total_count}, {len(totals)}\n\n")
        total_count += 1
        data = supabase.table("logs").select("*").eq("status", "wait").execute()
        if len(data.data) == 0:
            with open(f"second_data_{index}_done.json", "w") as f:
                json.dump(totals, f)
            upload_json_item(bucket, totals, "snap_temporary", f"second_data_{index}_done.json")
            is_finished = True
            break
        current_url = random.choice([d['url'] for d in data.data])
        supabase.table("logs").update({"status": "doing"}).eq("url", current_url).execute()
        print(f"Current URL: {current_url}")

        gender = current_url.split("gf=")[1].split("&")[0]
        height = current_url.split("height-range=")[1].split("&")[0].split("..")[0]
        weight = current_url.split("weight-range=")[1].split("&")[0].split("..")[0]
        types = current_url.split("types=")[1].split("&")[0]

        origin_num = len(totals)
        totals, total_num = extract_from_url(current_url, gender, types, index, totals, bucket)
        if total_num != 0:
            with open(f"second_data_{index}.json", "w") as f:
                json.dump(totals, f)
            upload_json_item(bucket, totals, "snap_temporary", f"second_data_{index}.json")
        supabase.table("logs").update({"status": "done", "total_num": total_num, "get_num": len(totals) - origin_num}).eq("url", current_url).execute()
        time.sleep(1)

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Selenium scrape + upload to GCS (pairs range).")
    p.add_argument("--index", type=int, default=100, help="유효 데이터 묶음 저장 단위")
    p.add_argument("--supabase_url", type=str, default='', help="Supabase URL")
    p.add_argument("--supabase_key", type=str, default='', help="Supabase Key")
    args = p.parse_args()

    print("Supabase ", args.supabase_url, args.supabase_key)

    main(args.index, args.supabase_url, args.supabase_key)
