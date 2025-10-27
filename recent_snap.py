import os
import time
import io
import json
import re
import uuid
import mimetypes
from typing import List, Dict, Any, Optional, Set

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from google.cloud import storage
from supabase import create_client, Client

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException, JavascriptException

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "./first-project-438808-dc1804307b11.json")

def build_driver(headless=True, user_agent=None):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.page_load_strategy = "eager"
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1200,2400")
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
    driver.set_page_load_timeout(12)
    return driver

# Selenium 드라이버
ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36")
driver = build_driver(headless=True, user_agent=ua)

ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36")
driver2 = build_driver(headless=True, user_agent=ua)


def select_one_or(soup, selector, default=None):
    el = soup.select_one(selector)
    return el if el else default

def musinsa_product_url_from_img(img_url: str) -> Optional[str]:
    m = re.search(r'/goods_img/\d{8}/(\d{6,8})/', img_url or "")
    return f"https://www.musinsa.com/products/{m.group(1)}" if m else None

NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")  # stable uuid namespace
# ---------- Utils ----------
def stable_uuid(text: str) -> uuid.UUID:
    return uuid.uuid5(NAMESPACE, text)

# ---------- Parsing per item ----------
def parse_item_div(div) -> Optional[Dict[str, Any]]:
    snap_id = div.get("data-key")
    if not snap_id:
        print("SKIP\n")
        return None

    # 타입 추론 (없으면 USER_SNAP 가정)
    if div.select_one("div[class*='sc-552dd808-0']"):
        snap_type = "brand"
    else:
        snap_type = "member"

    # 계정명/모델정보/설명/좋아요/이미지/상품
    try:
        # 루트 블록
        one_item = div

        # 계정명
        user_el = one_item.select_one("div[class*='sc-faa3da62-0']") or select_one_or(one_item, "a[href*='profile'],div[class*='nickname']")
        try:
            user_name = user_el.text
        except:
            user_name = ""
        if user_name == "무신사 코디":
            snap_type = 'mss'

        # 모델 텍스트 (ex: "170/65 · 웜톤")
        meta_el = one_item.select_one("span[class*='sc-7659943b-1']") or select_one_or(one_item, "div[class*='model'] span, span[class*='model']")
        try:
            mtext = meta_el.text
        except:
            mtext = ""
        tone = ""
        height = ""
        weight = ""
        if mtext:
            parts = [p.strip() for p in mtext.split("·")]
            hw = parts[0] if parts else ""
            if "/" in hw:
                hw_parts = hw.split("/")
                height = hw_parts[0].strip()
                weight = hw_parts[1].strip() if len(hw_parts) > 1 else ""
            else:
                if "cm" in hw:
                    height = hw.strip()
                elif "kg" in hw:
                    weight = hw.strip()
            if len(parts) > 1:
                tone = parts[-1].strip()

        # 좋아요 수
        like_el = one_item.select_one("span[class*='sc-7659943b-3']") or select_one_or(one_item, "span[class*='like']")
        like_text = like_el.text
        like_num = int(re.sub(r'[^0-9]', '', like_text) or "0")

        # 설명 텍스트
        desc_el = one_item.select_one("div[class*='sc-7659943b-5']") or select_one_or(one_item, "div[class*='desc'], p[class*='desc']")
        text = desc_el.text


        # 이미지 (슬라이드)
        slide_divs = one_item.select("div[class*='sc-8c7680f3-1']") or one_item.select("div[class*='slide']")
        images = []
        if slide_divs:
            for s in slide_divs:
                img = s.select_one("img")
                if img and (img.get("src") or img.get("data-src")):
                    src = img.get("src") or img.get("data-src")
                    src = re.sub(r'\?w=\d+', '', src)
                    images.append(src)
        # 일부 카드에서는 위 슬라이드 래퍼 없이 img만 있을 수 있으니 fallback
        if not images:
            for img in one_item.select("img"):
                src = img.get("src") or img.get("data-src")
                if src and "snap" in src:
                    src = re.sub(r'\?w=\d+', '', src)
                    images.append(src)

        # 상품들
        products = []
        product_items = div.find("div", class_=re.compile("sc-316ed15c-1"))
        if product_items:
            product_items = product_items.find_all("div", attrs={"data-item-brand": True})
            seen = set()
            for divp in product_items:
                item_id = divp.get('data-item-id')
                if item_id and item_id in seen:
                    continue
                if item_id:
                    seen.add(item_id)

                img = divp.select_one('img[data-src], img[src]')
                img_url = (img.get('data-src') or img.get('src')) if img else ''
                product_url = musinsa_product_url_from_img(img_url)

                brand_el = divp.select_one('span.text-etc_11px_semibold') or divp.select_one("span[class*='brand']")
                brand = brand_el.text if brand_el else (divp.get('data-item-brand') or '').strip()

                divs = divp.find_all("div")

                spans = divs[4].find_all("span")
                try:
                    product_name = spans[0].text
                except:
                    product_name = ''
                product_name_extra = ""
                if len(spans) > 1:
                    try:
                        product_name_extra = spans[1].text
                    except:
                        product_name_extra = ""

                products.append({
                    "product_url": product_url,
                    "brand_name": brand,
                    "product_name": product_name,
                    "desc": product_name_extra,
                })

        account_uuid = str(stable_uuid(user_name or "unknown"))
        data = {
            "snap_url": f"https://www.musinsa.com/snap/{snap_id}",
            "snap_id": str(snap_id),
            "account_name": user_name,
            "account_uuid": account_uuid,
            "model_info": f"{height}/{weight}" + (f", {tone}" if tone else ""),
            "snap_like": like_num,
            "snap_desc": text,
            "img_urls": images,
            "products": products,
            "folder1": snap_type,
            "folder2": account_uuid,
        }
        return data
    except Exception as e:
        print(f"[PARSE-FAIL] data-key={div.get('data-key')} : {e}")
        return None

client = storage.Client()
bucket = storage.Client().bucket("vton-mss-snap")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko,en;q=0.9",
}
def _guess_ext(url: str, content_type: Optional[str]) -> str:
    ext = os.path.splitext(url.split("?")[0])[1].lower()
    if ext in {".jpg", ".jpeg", ".png", ".webp"}:
        return ext
    if content_type:
        ext2 = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if ext2 in {".jpg", ".jpeg", ".png", ".webp"}:
            return ext2
    return ".jpg"

def upload_json_item(bucket, data, folder1: str, folder2: str, filename: str):
    if folder2 == "":
        path = f"json/{folder1}/{filename}"
    else:
        path = f"json/{folder1}/{folder2}/{filename}"
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(data, ensure_ascii=False, indent=2),
                            content_type="application/json")

def upload_image_from_url(bucket, url: str, gcs_path: str, timeout: int = 20):
    r = requests.get(url, headers=HEADERS, timeout=timeout, stream=True)
    r.raise_for_status()
    ext = _guess_ext(url, r.headers.get("Content-Type"))
    blob = bucket.blob(gcs_path + ext)
    bio = io.BytesIO(r.content)
    blob.cache_control = "public, max-age=31536000"
    blob.upload_from_file(bio, content_type=r.headers.get("Content-Type") or "image/jpeg")

def upload_images_for_snap(bucket, folder1: str, folder2: str, snap_id: str, img_urls: List[str]):
    base = f"images/{folder1}/{folder2}/{snap_id}"
    for idx, url in enumerate(img_urls, 1):
        gcs_path = f"{base}/{idx}"   # 확장자는 upload_image_from_url 내부에서 결정
        try:
            upload_image_from_url(bucket, url, gcs_path)
        except Exception as e:
            print(f"[IMG-FAIL] {url} -> gs://{bucket.name}/{gcs_path}*  ({e})")

def download_from_gcs(bucket_name: str, source_blob_name: str, destination_file: str):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file)
    print(f"✅ Downloaded gs://{bucket_name}/{source_blob_name} → {destination_file}")

local_snapid_path = "./done_snap_ids.json"
try:
    download_from_gcs(
        bucket_name=bucket.name,
        source_blob_name=f"files/done_snap_ids.json",
        destination_file=local_snapid_path
    )
except Exception as e:
    print(f"[WARN] cannot download done_ids from GCS: {e}")


def crawl_snaps(crawl_snaps_list):
    page_urls = []
    for snap_id in crawl_snaps_list:
        page_urls.append(f"https://www.musinsa.com/snap/{snap_id}")

    all_items = []
    total_datas = []

    i = 0
    for page_url in tqdm(page_urls):
        driver2.get(page_url)
        time.sleep(2.0)

        html = driver2.page_source
        soup = BeautifulSoup(html, "html.parser")

        card_divs = soup.select("div[class*='sc-7659943b-0']")
        item = parse_item_div(card_divs[0])
        if not item:
            continue
        # print(f"\n[{i}th] - Item == ", item, "\n\n")
        all_items.append(item)
        i += 1
    
    for item in all_items:
        snap_id = item["snap_id"]
        folder1 = item["folder1"]
        folder2 = item["folder2"]

        try:
            upload_json_item(bucket, item, folder1, folder2, f"{snap_id}.json")
            upload_images_for_snap(bucket, folder1, folder2, snap_id, item["img_urls"])

            total_datas.append({
                **item,
                "json_path": f"json/{folder1}/{folder2}/{snap_id}.json",
                "img_paths": [
                    f"images/{folder1}/{folder2}/{snap_id}/{idx}.jpg"
                    for idx in range(1, len(item["img_urls"]) + 1)
                ],
            })
        except Exception as e:
            print(f"[SAVE-ERR] snap_id={snap_id} : {e}")
            continue

    return total_datas


def main():
    driver.get("https://www.musinsa.com/snap/main/recommend?sort=NEWEST&gf=A")
    from datetime import date
    today = date.today().strftime("%m_%d")

    with open(local_snapid_path, "r") as f:
        done_snap_ids = json.load(f)

    new_ids_set = set(done_snap_ids)
    last_set_length = len(new_ids_set)
    end_count = 0
    total_snap_datas = []

    while True:
        for __ in range(5):
            driver.execute_script("window.scrollBy(0, 500);")  # 조금씩 내려감
            time.sleep(0.3)

        time.sleep(1.0)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        snap_divs = soup.select("a[class*='SnapFeedCard__Link']")

        new_snap_ids = set([c.get("href").split("/")[-1] for c in snap_divs])
        new_ids_set.update(new_snap_ids)
        # print("Set : ", len(new_ids_set))

        if len(new_ids_set) - 50 > last_set_length:
            # print("**", len(new_ids_set), last_set_length)
            # 여기서 크롤링 한번
            crawl_snaps_list = list(new_ids_set)[last_set_length:]
            products = crawl_snaps(crawl_snaps_list)
            # print(f"\n\nDone! : {len(products)}\n\n")
            
            # 그리고 ids 저장, 업로드
            with open(local_snapid_path, "w") as f:
                json.dump(list(new_ids_set), f, ensure_ascii=False, indent=2)
            upload_json_item(bucket, list(new_ids_set), "files", "", f"done_snap_ids.json")

            # 그리고 additional_json 저장, 업로드 - 현재 월일
            total_snap_datas.extend(products)
            with open(f"additional_json_{today}.json", "w") as f:
                json.dump(total_snap_datas, f, ensure_ascii=False, indent=2)
            upload_json_item(bucket, total_snap_datas, "files", "", f"additional_json_{today}.json")

        if len(new_ids_set) == last_set_length:
            end_count += 1
            if end_count>3:
                break

        last_set_length = len(new_ids_set)
    print("\n\nDone!\n\n")
    print(f"total_snap_datas: {len(total_snap_datas)}")
    with open(f"additional_json_{today}_done.json", "w") as f:
        json.dump(total_snap_datas, f, ensure_ascii=False, indent=2)
    upload_json_item(bucket, total_snap_datas, "files", "", f"additional_json_{today}_done.json")

if __name__ == "__main__":
    main()
