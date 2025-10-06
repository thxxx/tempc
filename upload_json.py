import os, json, re, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from tqdm import tqdm
import requests

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./first-project-438808-dc1804307b11.json"

CATS = {
    '상의': 'top',
    '바지': 'pants',
    '아우터': 'outer',
    '원피스/스커트': 'onepiece',
    '키즈': 'kids',
    '스포츠/레저': 'sports',
}

def gcs():
    return storage.Client().bucket("vton-mss")

def upload_json(bucket, data, folder, filename):
    blob = bucket.blob(f"{folder}/{filename}")
    blob.upload_from_string(json.dumps(data, ensure_ascii=False, indent=2),
                            content_type="application/json")

def upload_image(bucket, url, gcs_path, session):
    try:
        r = session.get(url, timeout=15)
        r.raise_for_status()
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(r.content, content_type="image/jpeg")
        return True
    except Exception:
        return False

def to_img_url(u: str) -> str:
    return re.sub(r'_500\.jpg$', '_big.jpg?w=1200', u)

def main(fromn: int, nums: int):
    with open('./allowed_data.json', 'r') as f:
        allowed_data = json.load(f)

    bucket = gcs()
    end = min(len(allowed_data), fromn + nums)
    print(f"total={len(allowed_data)} | processing={end - fromn}")

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    for d in tqdm(allowed_data[fromn:end]):
        try:
            cat = CATS[d['category_depth1']]
            g = 'uni' if ('남성' in d['genders'] and '여성' in d['genders']) else ('men' if '남성' in d['genders'] else 'women')
            folder1 = f"{g}_{cat}"
            folder2 = str(d['product_id'])[:3]
            pid = str(d['product_id'])

            # 1) JSON 저장 (경로 동일, images만 jsons로 변경)
            json_folder = f"jsons/{folder1}/{folder2}"
            upload_json(bucket, d, json_folder, f"{pid}.json")

            # 2) 이미지 병렬 업로드: images/{folder1}/{folder2}/{pid}/{idx}.jpg
            img_urls = [to_img_url(u) for u in d.get('image_urls', [])]
            base_dir = f"images/{folder1}/{folder2}/{pid}"

            with ThreadPoolExecutor(max_workers=4) as ex:
                futs = []
                for idx, url in enumerate(img_urls):
                    gcs_path = f"{base_dir}/{idx}.jpg"
                    futs.append(ex.submit(upload_image, bucket, url, gcs_path, session))
                # 완료 체크(필요시 결과 사용)
                for _ in as_completed(futs): pass

        except Exception as e:
            print("ERR:", e)
            continue

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--fromn", type=int, required=True, help="시작 인덱스")
    p.add_argument("--nums", type=int, required=True, help="수집 개수")
    a = p.parse_args()
    main(a.fromn * a.nums, a.nums)
