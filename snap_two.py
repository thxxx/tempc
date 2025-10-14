import os
import time
from bs4 import BeautifulSoup
import requests
import argparse
import re
from google.cloud import storage
import uuid

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

import io
import mimetypes

from google.cloud import storage

os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", "./first-project-438808-dc1804307b11.json")


def download_from_gcs(bucket_name: str, source_blob_name: str, destination_file: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file)

    print(f"✅ Downloaded gs://{bucket_name}/{source_blob_name} → {destination_file}")

def _guess_ext(url: str, content_type: str | None) -> str:
    # URL에서 확장자 추정 → 없으면 content-type으로 → 최종 기본은 .jpg
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

def upload_images_for_snap(bucket, folder1: str, folder2: str, snap_id: str, img_urls: list[str]):
    # images/{folder1}/{folder2}/{snap_id}/{idx}.ext
    base = f"images/{folder1}/{folder2}/{snap_id}"
    for idx, url in enumerate(img_urls, 1):
        gcs_path = f"{base}/{idx}"   # 확장자는 upload_image_from_url 내부에서 결정
        try:
            upload_image_from_url(bucket, url, gcs_path)
        except Exception as e:
            print(f"[IMG-FAIL] {url} -> gs://{bucket.name}/{gcs_path}*  ({e})")



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

def get_html(driver, url: str, wait_sec: int = 6) -> str:
    driver.get(url)
    time.sleep(2)
    return driver.page_source

def gcs_bucket(name: str = "vton-mss-snap"):
    return storage.Client().bucket(name)

def upload_json_item(bucket, data, folder1: str, folder2: str, filename: str):
    path = f"jsons/{folder1}/{folder2}/{filename}"
    blob = bucket.blob(path)
    blob.upload_from_string(json.dumps(data, ensure_ascii=False, indent=2),
                            content_type="application/json")

def musinsa_product_url_from_img(img_url: str) -> str | None:
    m = re.search(r'/goods_img/\d{8}/(\d{6,8})/', img_url)
    return f"https://www.musinsa.com/products/{m.group(1)}" if m else None

def extract_from_url(url: str):
    try:
        ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36")
        driver = build_driver(headless=True, user_agent=ua)
        driver.get(url)
        time.sleep(2.5)
        html  = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        one_item = soup.find("div", attrs={"class": "sc-7659943b-0"})

        user_name = soup.find("div", attrs={"class": "sc-faa3da62-0"}).text
        mtext = soup.find("span", attrs={"class": "sc-7659943b-1"}).text.split("·")
        if len(mtext)>1:
            tone = mtext[-1].strip()
        else:
            tone = ""

        height = mtext[0].split("/")[0]
        weight = mtext[0].split("/")[1]

        likes = soup.find("span", attrs={"class": "sc-7659943b-3"}).text
        like_num = re.sub(r'[^0-9]', '', likes)
        like_num = int(like_num)

        text = soup.find("div", attrs={"class": "sc-7659943b-5"}).text

        slides = one_item.find_all("div", attrs={"class": "sc-8c7680f3-1"})
        images = [re.sub('\?w=1000', '', s.find("img")["src"]) for s in slides]

        items = one_item.find_all("div", attrs={"class": "sc-d46d4af9-0"})
        products = []

        seen = set()

        for div in items:
            item_id = div.get('data-item-id')
            if not item_id or item_id in seen:
                continue
            seen.add(item_id)

            img = div.select_one('img[data-src], img[src]')
            img_url = (img.get('data-src') or img.get('src')) if img else ''
            product_url = musinsa_product_url_from_img(img_url)  # soldout 등 외부이미지는 None

            brand_el = div.select_one('span.text-etc_11px_semibold')
            brand = brand_el.get_text(strip=True) if brand_el else (div.get('data-item-brand') or '').strip()

            # 3) 상품 이름 (이름/색상/옵션이 한 컨테이너 내 여러 span일 수 있음 → 한 번에 결합)
            boxes = div.find_all("div", attrs={"class": "sc-1a38c32-7"})[0].find_all("span")
            product_name = boxes[0].text
            if len(boxes) > 1:
                product_name_extra = boxes[1].text
            else:
                product_name_extra = ""
            products.append({
                "product_url": product_url,
                "brand_name": brand,
                "product_name": product_name,
                "desc": product_name_extra,
            })
        
        user_id = str(stable_uuid(user_name))
        data = {
            "snap_url": url,
            "account_name": user_name,
            "account_uuid": user_id,
            "model_info": f"{height}/{weight}, {tone}",
            "snap_like": like_num,
            "snap_desc": text,
            "img_urls": images,
            "products": products,
        }
        return data

    except Exception as e:
        print(f"[URL : {url}]\nError while loading first driver - {e}")
        return None

dad = {
    'USER_SNAP': "member",
    'CODISHOP_SNAP': "mss",
    "BRAND_SNAP": "brand",
}

NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")  # 고정 namespace (직접 정해놓기)

def stable_uuid(text: str) -> uuid.UUID:
    return uuid.uuid5(NAMESPACE, text)

def main(index: int):
    with open("urls.json", "r") as f:
        datas = json.load(f)
    bucket = gcs_bucket()

    # 사용 예시
    download_from_gcs(
        bucket_name="vton-mss-snap",
        source_blob_name=f"jsons/snaps/all/data_{index}.json",
        destination_file="./prev_datas.json"
    )
    prev_datas = json.load(open("./prev_datas.json", "r"))

    targets = datas[index * 5000 + len(prev_datas) : (index + 1) * 5000]
    total_datas = prev_datas
    for d in tqdm(targets):
        data = extract_from_url(d['url'])
        if not data:
            continue
        snap_id = d['url'][len('https://www.musinsa.com/snap/'):]
        upload_json_item(bucket, data, dad[d['types']], data['account_uuid'], f"{snap_id}.json")

        # 그리고 img_urls에서 이미지 업로드
        upload_images_for_snap(
            bucket=bucket,
            folder1=dad[d['types']],
            folder2=str(data['account_uuid']),
            snap_id=snap_id,
            img_urls=data['img_urls'],
        )

        data = {
            **data,
            'json_path': f"jsons/{dad[d['types']]}/{data['account_uuid']}/{snap_id}.json",
            'img_paths': [f"images/{dad[d['types']]}/{data['account_uuid']}/{snap_id}/{idx}.jpg" for idx in range(1, len(data['img_urls']) + 1)],
        }

        total_datas.append(data)

        if len(total_datas) % 200 == 199:
            with open(f"data_{index}.json", "w") as f:
                json.dump(total_datas, f, ensure_ascii=False, indent=2)
            upload_json_item(bucket, total_datas, "snaps", "all", f"data_{index}.json")
    with open(f"data_{index}_done.json", "w") as f:
        json.dump(total_datas, f, ensure_ascii=False, indent=2)
    upload_json_item(bucket, total_datas, "snaps", "all", f"data_{index}_done.json")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Selenium scrape + upload to GCS (pairs range).")
    p.add_argument("--index", type=int, default=100, help="유효 데이터 묶음 저장 단위")
    args = p.parse_args()

    main(args.index)
