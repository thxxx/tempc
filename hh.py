from huggingface_hub import hf_hub_download
import shutil

# repo_id: "username/repo_name"
# filename: repo 안의 파일 이름
local_path = hf_hub_download(
    repo_id="Daniel777/base_38",
    filename="first-project-438808-dc1804307b11.json"
)

# 원하는 위치로 복사 (이동하려면 shutil.move)
target_path = "./first-project-438808-dc1804307b11.json"
shutil.copy(local_path, target_path)

print("Downloaded to:", local_path)

local_json_path = hf_hub_download(
    repo_id="Daniel777/personals",
    filename="urls.json"
)
target_json_path = "./urls.json"
shutil.copy(local_json_path, target_json_path)
