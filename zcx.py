import re
import os
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from google.cloud import storage
import os
import time
import json
from tqdm import tqdm
import argparse

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./first-project-438808-dc1804307b11.json"

def upload_to_gcs(local_file_path: str, bucket_name: str = "vton-mss", folder='', destination_name: str = None):
    """
    local_file_path: 업로드할 로컬 파일 경로
    bucket_name: 업로드할 GCS 버킷 이름
    """
    if destination_name is None:
        destination_name = os.path.basename(local_file_path)

    # GCS 클라이언트 생성 (환경변수 GOOGLE_APPLICATION_CREDENTIALS 필요)
    storage_client = storage.Client()
    blob_name = f"{folder.rstrip('/')}/{destination_name}" if folder else destination_name


    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_filename(local_file_path)

    print(f"✅ Uploaded {local_file_path} to gs://{bucket_name}/{blob_name}")


HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def parse_from_next_data(soup):
    node = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if not node:
        return None
    data = json.loads(node.text)

    d = data["props"]["pageProps"]["meta"]["data"]
    # 원하는 필드들
    style_no = d.get("styleNo", "")
    product_id = d.get("goodsNo", "")
    product_name_korean = d.get("goodsNm", '')
    product_name = d.get('goodsNmEng', '')
    genders = d.get("sex", [])
    brand_name_korean = d.get("brandInfo", {}).get("brandName", "")
    brand_name = d.get("brandInfo", {}).get("brand", "")
    cat1 = d.get("category", {}).get("categoryDepth1Name", "")
    cat2 = d.get("category", {}).get("categoryDepth2Name", "")
    price = d.get("goodsPrice", {}).get("salePrice", 0)
    review_count = d.get('goodsReview', {}).get('totalCount', -1)
    review_score = d.get('goodsReview', {}).get('satisfactionScore', -1)

    mats = d.get('goodsMaterial', {}).get("materials", {})

    extra_infos = []
    if mats != {}:
        for m in mats:
            ifo = []
            ifo.append(m['name'])
            for item in m['items']:
                isSelected = item.get('isSelected', False)
                if isSelected:
                    ifo.append(item['name'])
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
        'review_count':review_count,
        'review_score':review_score
    }

def parse_from_window_state(html):
    # window.__MSS__.product.state = { ... };
    m = re.search(
        r"window\.__MSS__\.product\.state\s*=\s*({.*?});\s*window\.__MSS__",
        html, flags=re.S
    )
    if not m:
        # 마지막에 한 번 더 정의돼 있을 수 있으니 완화된 패턴 시도
        m = re.search(r"window\.__MSS__\.product\.state\s*=\s*({.*?});", html, re.S)
        if not m:
            return None
    state = json.loads(m.group(1))

    style_no = state.get("styleNo", "")
    product_id = state.get("goodsNo", "")
    product_name_korean = state.get("goodsNm", '')
    product_name = state.get('goodsNmEng', '')
    genders = state.get("sex", [])
    brand_name_korean = state.get("brandInfo", {}).get("brandName", "")
    brand_name = state.get("brandInfo", {}).get("brand", "")
    cat1 = state.get("category", {}).get("categoryDepth1Name", "")
    cat2 = state.get("category", {}).get("categoryDepth2Name", "")
    price = state.get("goodsPrice", {}).get("salePrice", 0)
    review_count = state.get('goodsReview', {}).get('totalCount', -1)
    review_score = state.get('goodsReview', {}).get('satisfactionScore', -1)

    mats = state.get('goodsMaterial', {}).get("materials", {})

    extra_infos = []
    if mats != {}:
        for m in mats:
            ifo = []
            ifo.append(m['name'])
            for item in m['items']:
                isSelected = item.get('isSelected', False)
                if isSelected:
                    ifo.append(item['name'])
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
        'review_count':review_count,
        'review_score':review_score
    }

def scrape_product_fields(soup, html):
    soup = BeautifulSoup(html, "html.parser")

    # 1순위: __NEXT_DATA__
    info = parse_from_next_data(soup)
    if info:
        return info

    # 2순위: window.__MSS__.product.state
    info = parse_from_window_state(html)
    if info:
        return info

    raise RuntimeError("상품 정보를 찾지 못했습니다.")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko,en;q=0.9",
}

def normalize(url: str) -> str:
    # 무신사 이미지는 종종 //image... 형태 → https: 붙이기
    if url.startswith("//"):
        return "https:" + url
    # /images/... 형태는 사이트 기준으로 절대화
    return urljoin("https://image.msscdn.net/", url)

def extract_from_next_data(soup):
    node = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if not node: 
        return [], []
    data = json.loads(node.text)
    d = data["props"]["pageProps"]["meta"]["data"]

    # 1) 갤러리/디테일 이미지
    gallery = [normalize(x["imageUrl"]) for x in d.get("goodsImages", []) if "imageUrl" in x]

    # 2) 상세설명 HTML 안의 <img src="...">
    # goods_contents_html = d.get("goodsContents", "") or ""
    # inner_soup = BeautifulSoup(goods_contents_html, "html.parser")
    # content_imgs = [normalize(img.get("src")) for img in inner_soup.find_all("img", src=True)]

    return gallery

def extract_from_window_state(html):
    m = re.search(r"window\.__MSS__\.product\.state\s*=\s*({.*?});", html, re.S)
    if not m:
        return [], []
    state = json.loads(m.group(1))

    gallery = [normalize(x["imageUrl"]) for x in state.get("goodsImages", []) if "imageUrl" in x]

    # goods_contents_html = state.get("goodsContents", "") or ""
    # inner_soup = BeautifulSoup(goods_contents_html, "html.parser")
    # content_imgs = [normalize(img.get("src")) for img in inner_soup.find_all("img", src=True)]

    return gallery

def fetch_images(url):
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    g1 = extract_from_next_data(soup)
    if len(g1) == 0:
        g2 = extract_from_window_state(html)

        images = list(dict.fromkeys(g1 + g2))  # 중복 제거(순서 유지)
    else:
        images = g1

    return images

def extract_meta_thumbnail(soup: BeautifulSoup):
    # <meta property="og:image" ...>
    meta = soup.find("meta", attrs={"property": "og:image"})
    og_img = normalize(meta["content"]) if meta and meta.get("content") else None
    return [og_img] if og_img else []

def get_from_url(url):
    response = requests.get(url)
    response.raise_for_status()  # 요청 실패 시 에러 발생
    soup = BeautifulSoup(response.text, "html.parser")

    info = scrape_product_fields(soup, response.text)
    gal_images = extract_from_next_data(soup)
    thumbnail = extract_meta_thumbnail(soup)

    image_urls = [thumbnail[0]] + [ul for ul in gal_images]
    # image_urls = [re.sub('500.jpg', 'big.jpg?w=1200', thumbnail[0])] + [re.sub('500.jpg', 'big.jpg?w=1200', ul) for ul in gal_images]

    return info, image_urls

def main(fromn, nums):
    total_datas = []
    st = time.time()
    ori = fromn
    for i in tqdm(range(0, nums)):
        url = f'https://www.musinsa.com/products/{ori + i}'
        
        try:
            if i%5000 == 4999:
                json.dump(total_datas, open(f'musinsa_datas_{ori}.json', 'w'), ensure_ascii=False, indent=2)

                print(f"데이터 저장: {ori} ~ {ori + i}")
                print(f"총 {len(total_datas)}개의 상품 정보를 추출했습니다.")
                print(f"총 {time.time() - st}초 소요되었습니다.\n\n")

                # 여기서 GCS에 주기적으로 업로드
                upload_to_gcs(f'musinsa_datas_{ori}.json', folder='temporarysaves')

            if i%20000 == 19999:
                json.dump(total_datas, open(f'musinsa_datas_{ori}_{i}.json', 'w'), ensure_ascii=False, indent=2)

                print(f"데이터 저장: {ori} ~ {ori + i}")
                print(f"총 {len(total_datas)}개의 상품 정보를 추출했습니다.")
                print(f"총 {time.time() - st}초 소요되었습니다.\n\n")

                upload_to_gcs(f'musinsa_datas_{ori}_{i}.json', folder='temporarysaves')
        except Exception as e:
            print(e)
            continue

        try:
            info, image_urls = get_from_url(url)
            # print({**info, 'product_url': url, 'image_urls': image_urls})
            total_datas.append({**info, 'product_url': url, 'image_urls': image_urls})
        except Exception as e:
            # print(e)
            continue
    
    json.dump(total_datas, open(f'musinsa_datas_{ori}_done.json', 'w'), ensure_ascii=False, indent=2)
    upload_to_gcs(f'musinsa_datas_{ori}_done.json', folder='temporarysaves')
    print("Finished!")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Example with two arguments")
    parser.add_argument("--fromn", type=int, required=True, help="Input file path")
    parser.add_argument("--nums", type=int, required=True, help="Output file path")
    
    args = parser.parse_args()

    main(args.fromn*200000 + 1000000, args.nums)