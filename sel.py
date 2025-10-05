# scrape_musinsa_selenium.py
import os
import re
import json
import time
import argparse
from urllib.parse import urljoin

from tqdm import tqdm
from bs4 import BeautifulSoup

# GCS
from google.cloud import storage

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# === 환경변수: GCP 인증 ===
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./first-project-438808-dc1804307b11.json"


def upload_to_gcs(local_file_path: str, bucket_name: str = "vton-mss", folder: str = "", destination_name: str = None):
    if destination_name is None:
        destination_name = os.path.basename(local_file_path)

    storage_client = storage.Client()
    blob_name = f"{folder.rstrip('/')}/{destination_name}" if folder else destination_name
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_file_path)
    print(f"✅ Uploaded {local_file_path} to gs://{bucket_name}/{blob_name}")


# ====== 파싱 유틸 ======
def parse_from_next_data(soup: BeautifulSoup):
    node = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if not node:
        return None
    data = json.loads(node.text)

    d = data["props"]["pageProps"]["meta"]["data"]
    style_no = d.get("styleNo", "")
    product_id = d.get("goodsNo", "")
    product_name_korean = d.get("goodsNm", "")
    product_name = d.get("goodsNmEng", "")
    genders = d.get("sex", [])
    brand_name_korean = d.get("brandInfo", {}).get("brandName", "")
    brand_name = d.get("brandInfo", {}).get("brand", "")
    cat1 = d.get("category", {}).get("categoryDepth1Name", "")
    cat2 = d.get("category", {}).get("categoryDepth2Name", "")
    price = d.get("goodsPrice", {}).get("salePrice", 0)
    review_count = d.get("goodsReview", {}).get("totalCount", -1)
    review_score = d.get("goodsReview", {}).get("satisfactionScore", -1)

    mats = d.get("goodsMaterial", {}).get("materials", {})
    extra_infos = []
    if mats != {}:
        for m in mats:
            ifo = [m["name"]]
            for item in m.get("items", []):
                if item.get("isSelected", False):
                    ifo.append(item["name"])
            extra_infos.append(ifo)

    return {
        "style_no": style_no,
        "product_id": product_id,
        "product_name": product_name,
        "product_name_korean": product_name_korean,
        "genders": genders,
        "brand_name": brand_name,
        "brand_name_korean": brand_name_korean,
        "category_depth1": cat1,
        "category_depth2": cat2,
        "price_krw": price,
        "extra_infos": extra_infos,
        "review_count": review_count,
        "review_score": review_score,
    }


def parse_from_window_state(html: str):
    m = re.search(r"window\.__MSS__\.product\.state\s*=\s*({.*?});\s*window\.__MSS__", html, re.S)
    if not m:
        m = re.search(r"window\.__MSS__\.product\.state\s*=\s*({.*?});", html, re.S)
        if not m:
            return None
    state = json.loads(m.group(1))

    style_no = state.get("styleNo", "")
    product_id = state.get("goodsNo", "")
    product_name_korean = state.get("goodsNm", "")
    product_name = state.get("goodsNmEng", "")
    genders = state.get("sex", [])
    brand_name_korean = state.get("brandInfo", {}).get("brandName", "")
    brand_name = state.get("brandInfo", {}).get("brand", "")
    cat1 = state.get("category", {}).get("categoryDepth1Name", "")
    cat2 = state.get("category", {}).get("categoryDepth2Name", "")
    price = state.get("goodsPrice", {}).get("salePrice", 0)
    review_count = state.get("goodsReview", {}).get("totalCount", -1)
    review_score = state.get("goodsReview", {}).get("satisfactionScore", -1)

    mats = state.get("goodsMaterial", {}).get("materials", {})
    extra_infos = []
    if mats != {}:
        for m in mats:
            ifo = [m["name"]]
            for item in m.get("items", []):
                if item.get("isSelected", False):
                    ifo.append(item["name"])
            extra_infos.append(ifo)

    return {
        "style_no": style_no,
        "product_id": product_id,
        "product_name": product_name,
        "product_name_korean": product_name_korean,
        "genders": genders,
        "brand_name": brand_name,
        "brand_name_korean": brand_name_korean,
        "category_depth1": cat1,
        "category_depth2": cat2,
        "price_krw": price,
        "extra_infos": extra_infos,
        "review_count": review_count,
        "review_score": review_score,
    }


def scrape_product_fields(soup: BeautifulSoup, html: str):
    try:
        info = parse_from_next_data(soup)
        if info:
            return info
        info = parse_from_window_state(html)
        if info:
            return info
    except Exception as e:
        print(e)
        return None

def normalize(url: str) -> str:
    if not url:
        return url
    if url.startswith("//"):
        return "https:" + url
    return urljoin("https://image.msscdn.net/", url)


def extract_from_next_data_images(soup: BeautifulSoup):
    node = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if not node:
        return []
    data = json.loads(node.text)
    d = data["props"]["pageProps"]["meta"]["data"]
    gallery = [normalize(x.get("imageUrl")) for x in d.get("goodsImages", []) if x.get("imageUrl")]
    return gallery


def extract_from_window_state_images(html: str):
    m = re.search(r"window\.__MSS__\.product\.state\s*=\s*({.*?});", html, re.S)
    if not m:
        return []
    state = json.loads(m.group(1))
    gallery = [normalize(x.get("imageUrl")) for x in state.get("goodsImages", []) if x.get("imageUrl")]
    return gallery


def extract_meta_thumbnail(soup: BeautifulSoup):
    meta = soup.find("meta", attrs={"property": "og:image"})
    og_img = normalize(meta["content"]) if meta and meta.get("content") else None
    return [og_img] if og_img else []

def build_driver2(headless=True, user_agent=None):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1200,2000")
    opts.add_argument("--lang=ko-KR")
    if user_agent:
        opts.add_argument(f"--user-agent={user_agent}")

    # google-chrome-stable은 /usr/bin/google-chrome 에 설치됨
    # Selenium Manager가 해당 버전에 맞는 chromedriver를 자동 다운로드/실행함.
    driver = webdriver.Chrome(options=opts)

    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"},
        )
    except Exception:
        pass
    return driver

from selenium.webdriver.chrome.options import Options

def build_driver(headless=True, user_agent=None):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.page_load_strategy = "eager"   # DOMContentLoaded까지 대기(이미지/서브리소스는 안기다림)
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1200,2000")
    opts.add_argument("--lang=ko-KR")
    # 리소스 최소화
    prefs = {
        "profile.managed_default_content_settings.images": 2,  # 이미지 안받음
        "profile.managed_default_content_settings.stylesheets": 2,
        "profile.managed_default_content_settings.cookies": 1, # 쿠키는 허용
        "profile.managed_default_content_settings.javascript": 1, # JS는 켜둠(필요시)
    }
    opts.add_experimental_option("prefs", prefs)
    if user_agent:
        opts.add_argument(f"--user-agent={user_agent}")
    driver = webdriver.Chrome(options=opts)
    # webdriver 특성 숨김(약간의 도움)
    try:
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        })
    except: pass
    driver.set_page_load_timeout(6)
    return driver



def get_html(driver, url: str, wait_sec: int = 6):
    driver.get(url)
    # __NEXT_DATA__ 가 로드될 때까지 대기(없으면 body)
    try:
        WebDriverWait(driver, wait_sec).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "script#\\__NEXT_DATA__"))
        )
    except Exception:
        # fallback: 본문이라도 로드될 때까지
        WebDriverWait(driver, wait_sec).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

    # 동적 이미지/상세가 늦게 들어오면 약간 더 대기
    time.sleep(0.06)
    return driver.page_source


def get_from_url_selenium(driver, url: str):
    html = get_html(driver, url)
    soup = BeautifulSoup(html, "html.parser")

    info = scrape_product_fields(soup, html)
    gal_images = extract_from_next_data_images(soup)
    if not gal_images:
        gal_images = extract_from_window_state_images(html)

    thumbnail = extract_meta_thumbnail(soup)
    image_urls = ([thumbnail[0]] if thumbnail else []) + gal_images

    return info, image_urls


# ====== 메인 루프 ======
def main(fromn: int, nums: int):
    total_datas = []
    st = time.time()
    ori = fromn

    # UA를 지정하면 403 회피에 도움됨
    ua = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    driver = build_driver(headless=True, user_agent=ua)

    try:
        for i in tqdm(range(0, nums)):
            url = f"https://www.musinsa.com/products/{ori + i}"
            # print(url)

            # 주기 저장 및 업로드
            try:
                if i % 1000 == 999:
                    json.dump(total_datas, open(f"musinsa_datas_{ori}.json", "w"), ensure_ascii=False, indent=2)
                    print(f"데이터 저장: {ori} ~ {ori + i}")
                    print(f"총 {len(total_datas)}개의 상품 정보를 추출했습니다.")
                    print(f"총 {time.time() - st:.1f}초 소요.\n")
                    upload_to_gcs(f"musinsa_datas_{ori}.json", folder="temporarysaves")

                if i % 10000 == 9999:
                    json.dump(
                        total_datas, open(f"musinsa_datas_{ori}_{i}.json", "w"), ensure_ascii=False, indent=2
                    )
                    print(f"데이터 저장: {ori} ~ {ori + i}")
                    print(f"총 {len(total_datas)}개의 상품 정보를 추출했습니다.")
                    print(f"총 {time.time() - st:.1f}초 소요.\n")
                    upload_to_gcs(f"musinsa_datas_{ori}_{i}.json", folder="temporarysaves")
            except Exception as e:
                print("[WARN: save/upload]", e)

            # 개별 페이지 수집 (간단한 재시도 포함)
            try:
                retries = 1
                backoff = 1.2
                for attempt in range(retries):
                    try:
                        info, image_urls = get_from_url_selenium(driver, url)
                        total_datas.append({**info, "product_url": url, "image_urls": image_urls})
                        break
                    except Exception as e:
                        if attempt == retries - 1:
                            continue
                            # print(f"[SKIP] {url} -> {e}")
                        else:
                            time.sleep(backoff)
                            backoff *= 1.7
                # 너무 빠른 접근은 방어막에 걸릴 수 있으니 살짝 쉬어주기
                time.sleep(0.1)
            except Exception as e:
                print(e)
                continue

        json.dump(
            total_datas, open(f"musinsa_datas_{ori}_done.json", "w"), ensure_ascii=False, indent=2
        )
        upload_to_gcs(f"musinsa_datas_{ori}_done.json", folder="temporarysaves")
        print("Finished!")
    finally:
        driver.quit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Musinsa with Selenium")
    parser.add_argument("--fromn", type=int, required=True, help="시작 인덱스(배수 스킴은 기존 코드와 동일)")
    parser.add_argument("--nums", type=int, required=True, help="수집 개수")

    args = parser.parse_args()
    # 기존 코드의 시작 오프셋 계산을 그대로 유지
    start_num = args.fromn * args.nums + 1000000
    main(start_num, args.nums)
