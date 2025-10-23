import os
import time
import io
import json
import re
import uuid
import argparse
import mimetypes
from typing import List, Dict, Any, Optional, Set

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from google.cloud import storage
from supabase import create_client, Client

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException, JavascriptException

# ---------- Env ----------
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "./first-project-438808-dc1804307b11.json")

# ---------- Constants ----------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko,en;q=0.9",
}

NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")  # stable uuid namespace

client = storage.Client()

# ---------- GCS ----------
def gcs_bucket(name: str = "vton-mss-snap"):
    return storage.Client().bucket(name)

def download_from_gcs(bucket_name: str, source_blob_name: str, destination_file: str):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file)
    print(f"✅ Downloaded gs://{bucket_name}/{source_blob_name} → {destination_file}")

def upload_json_item(bucket, data, folder1: str, folder2: str, filename: str):
    path = f"json/{folder1}/{folder2}/{filename}"
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(data, ensure_ascii=False, indent=2),
                            content_type="application/json")

def _guess_ext(url: str, content_type: Optional[str]) -> str:
    ext = os.path.splitext(url.split("?")[0])[1].lower()
    if ext in {".jpg", ".jpeg", ".png", ".webp"}:
        return ext
    if content_type:
        ext2 = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if ext2 in {".jpg", ".jpeg", ".png", ".webp"}:
            return ext2
    return ".jpg"

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

# ---------- Utils ----------
def stable_uuid(text: str) -> uuid.UUID:
    return uuid.uuid5(NAMESPACE, text)

def musinsa_product_url_from_img(img_url: str) -> Optional[str]:
    m = re.search(r'/goods_img/\d{8}/(\d{6,8})/', img_url or "")
    return f"https://www.musinsa.com/products/{m.group(1)}" if m else None

def get_text_safe(el) -> str:
    try:
        return el.get_text(strip=True)
    except Exception:
        return ""

def select_one_or(soup, selector, default=None):
    el = soup.select_one(selector)
    return el if el else default

# ---------- Selenium ----------
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

def infinite_scroll_collect(driver, max_scrolls: int = 80, sleep_sec: float = 1.2) -> None:
    """
    화면 끝까지 스크롤하며 컨텐츠 로딩을 유도.
    새로운 높이가 안 생기는 경우가 연속 3번 나오면 종료.
    """
    same_count = 0
    last_height = 0
    for i in range(max_scrolls):
        try:
            new_height = driver.execute_script("return document.body.scrollHeight")
        except JavascriptException:
            break

        if new_height <= last_height:
            same_count += 1
        else:
            same_count = 0

        if same_count >= 3:
            break

        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        except JavascriptException:
            break

        time.sleep(sleep_sec)
        last_height = new_height

# ---------- Parsing per item ----------
def parse_item_div(div) -> Optional[Dict[str, Any]]:
    """
    입력: 각 카드 컨테이너(최상위: data-key 속성 보유)
    출력: 일관된 dict (실패 시 None)
    """
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
        user_name = get_text_safe(user_el)
        if user_name == "무신사 코디":
            snap_type = 'mss'

        # 모델 텍스트 (ex: "170/65 · 웜톤")
        meta_el = one_item.select_one("span[class*='sc-7659943b-1']") or select_one_or(one_item, "div[class*='model'] span, span[class*='model']")
        mtext = get_text_safe(meta_el)
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
        like_text = get_text_safe(like_el)
        like_num = int(re.sub(r'[^0-9]', '', like_text) or "0")

        # 설명 텍스트
        desc_el = one_item.select_one("div[class*='sc-7659943b-5']") or select_one_or(one_item, "div[class*='desc'], p[class*='desc']")
        text = get_text_safe(desc_el)

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
                brand = get_text_safe(brand_el) if brand_el else (divp.get('data-item-brand') or '').strip()

                divs = divp.find_all("div")

                spans = divs[4].find_all("span")
                product_name = spans[0].text
                product_name_extra = ""
                if len(spans) > 1:
                    product_name_extra = spans[1].text

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

# ---------- Supabase ----------
def supa_has_url(supabase: Client, url: str) -> bool:
    try:
        resp = supabase.table("logs").select("id").eq("url", url).limit(1).execute()
        data = getattr(resp, "data", None) or []
        return len(data) > 0
    except Exception as e:
        print(f"[SUPA-ERR] search url={url} : {e}")
        # 조회 실패 시 중복 false로 간주(보수적으로 저장 시도)
        return False

def supa_upsert_log(supabase: Client, row: Dict[str, Any]) -> None:
    try:
        supabase.table("logs").upsert(row, on_conflict="url").execute()
    except Exception as e:
        print(f"[SUPA-ERR] upsert logs: {e}")

# ---------- High level: scrape one page with infinite scroll ----------
def scrape_page(driver, page_url: str) -> List[Dict[str, Any]]:
    """
    페이지 내 무한 스크롤을 수행하며 data-key 보유 카드들을 모두 파싱해 반환.
    """
    driver.get(page_url)
    time.sleep(2.0)

    all_items: Dict[str, Dict[str, Any]] = {}
    no_new_rounds = 0
    MAX_ROUNDS = 1

    for round_idx in range(MAX_ROUNDS):
        # infinite_scroll_collect(driver, max_scrolls=1, sleep_sec=2)
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        card_divs = soup.select("div[class*='sc-7659943b-0']")

        new_cnt = 0
        for div in card_divs:
            item = parse_item_div(div)
            if not item:
                continue
            sid = item["snap_id"]
            if sid not in all_items:
                all_items[sid] = item
                new_cnt += 1

        if new_cnt == 0:
            no_new_rounds += 1
        else:
            no_new_rounds = 0

    return list(all_items.values())

# ---------- Main ----------
def main(index: int, supabase_url: str, supabase_key: str):
    supabase = create_client(supabase_url, supabase_key)
    bucket = gcs_bucket()

    local_done_path = "./nos_ramains.json"
    try:
        download_from_gcs(
            bucket_name=bucket.name,
            source_blob_name=f"files/nos_ramains.json",
            destination_file=local_done_path
        )
    except Exception as e:
        print(f"[WARN] cannot download done_ids from GCS: {e}")

    no_products_list = json.load(open(local_done_path, "r"))
    total_products_json = {}
    for p in no_products_list:
        total_products_json[p["snap_id"]] = p

    target_snaps = no_products_list[index*5000:(index+1)*5000]
    print(f"target_snaps : {len(target_snaps)}")

    check_done_ids = set()

    total_datas: List[Dict[str, Any]] = []
    roll_count = 0

    # Selenium 드라이버
    ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
          "AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/120.0.0.0 Safari/537.36")
    driver = build_driver(headless=True, user_agent=ua)

    try:
        for snap in target_snaps:
            page_url = snap["snap_url"]
            items = scrape_page(driver, page_url)
            print(f"[SCRAPE] {len(items)}")

            for item in tqdm(items, desc="items"):
                snap_id = item["snap_id"]
                snap_url = item["snap_url"]
                folder1 = item["folder1"]
                folder2 = item["folder2"]
                
                try:
                    upload_json_item(bucket, item, folder1, folder2, f"{snap_id}.json")
                    if snap_id in total_products_json:
                        for idx, source_path in enumerate(total_products_json[snap_id]['img_paths']):
                            dst_path = f"images/{folder1}/{folder2}/{snap_id}/{idx}.jpg"
                            try:
                                blob = bucket.blob(source_path)
                                new_blob = bucket.copy_blob(blob, bucket, dst_path)
                            except Exception as e:
                                blob = bucket.blob(source_path[:-4] + ".png")
                                new_blob = bucket.copy_blob(blob, bucket, dst_path)
                            # blob.delete()

                        total_datas.append({
                            **item,
                            "json_path": f"json/{folder1}/{folder2}/{snap_id}.json",
                            "img_paths": [
                                f"images/{folder1}/{folder2}/{snap_id}/{idx}.jpg"
                                for idx in range(1, len(item["img_urls"]) + 1)
                            ],
                        })

                        check_done_ids.add(snap_id)
                        roll_count += 1

                    if len(total_datas) % 500 == 0:
                        with open(f"data_added_{index}.json", "w") as f:
                            json.dump(total_datas, f, ensure_ascii=False, indent=2)
                        upload_json_item(bucket, total_datas, "snaps", "additional_tuned", f"data_added_{index}.json")

                except Exception as e:
                    print(f"[SAVE-ERR] snap_id={snap_id} : {e}")
                    continue

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    print(f"[DONE] total_datas : {len(total_datas)}")
    with open(f"data_added_{index}_done.json", "w") as f:
        json.dump(total_datas, f, ensure_ascii=False, indent=2)
    upload_json_item(bucket, total_datas, "snaps", "additional_tuned", f"data_added_{index}_done.json")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Musinsa snap infinite scroll scraper with GCS & Supabase.")
    p.add_argument("--index", type=int, default=100, help="롤링 저장 인덱스")
    p.add_argument("--supabase_url", type=str, required=True, help="Supabase URL")
    p.add_argument("--supabase_key", type=str, required=True, help="Supabase Key")
    args = p.parse_args()

    print("Supabase ", args.supabase_url)
    main(args.index, args.supabase_url, args.supabase_key)
