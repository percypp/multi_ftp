import requests
import hashlib
import sys

VM_SERVICE_URL = "http://<VM_SERVICE_IP>:8000/trigger-transfer"
OUTPUT_FILE = "final_data.dat"

def download_and_verify():
    print(f"Requesting file from {VM_SERVICE_URL}...")
    
    # 1. 開啟串流連線 (stream=True)
    try:
        with requests.get(VM_SERVICE_URL, stream=True, timeout=60) as response:
            response.raise_for_status()
            
            # 2. 取得驗證資訊
            expected_md5 = response.headers.get("X-File-Checksum")
            expected_size = response.headers.get("Content-Length")
            print(f"Header Info -> Size: {expected_size}, MD5: {expected_md5}")
            
            # 準備本地的 MD5 計算器
            local_md5 = hashlib.md5()
            downloaded_bytes = 0
            
            # 3. 開始串流寫入
            with open(OUTPUT_FILE, "wb") as f:
                # 每次讀取 8KB (或是 64KB 都可以)
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        local_md5.update(chunk) # 邊收邊算
                        downloaded_bytes += len(chunk)
                        
            print(f"Download finished. Total bytes: {downloaded_bytes}")

            # 4. 驗證階段 (Sanity Check)
            # 驗證大小
            if expected_size and downloaded_bytes != int(expected_size):
                print(f"[ERROR] Size Mismatch! Expected: {expected_size}, Got: {downloaded_bytes}")
                return False

            # 驗證 MD5
            calculated_md5 = local_md5.hexdigest()
            if expected_md5:
                if calculated_md5 == expected_md5:
                    print(f"[SUCCESS] MD5 Verified: {calculated_md5}")
                    return True
                else:
                    print(f"[ERROR] MD5 Mismatch! Expected: {expected_md5}, Got: {calculated_md5}")
                    return False
            else:
                print("[WARNING] No MD5 in header, skipping hash check.")
                return True

    except Exception as e:
        print(f"[CRITICAL ERROR] Connection failed: {e}")
        return False

if __name__ == "__main__":
    success = download_and_verify()
    if not success:
        sys.exit(1) # 讓 K8s 知道這個 Job 失敗了
