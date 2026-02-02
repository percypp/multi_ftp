import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import paramiko

app = FastAPI()

# --- 設定區 (建議改用環境變數) ---
PHYSICAL_HOST = "192.168.1.100"
PHYSICAL_USER = "admin"
PHYSICAL_PASSWORD = "your_password" # 建議用 SSH Key
REMOTE_FILE_PATH = "/data/logs/big_file.log"
# ------------------------------

def get_remote_md5(ssh_client, filepath):
    """透過 SSH 指令在實體機上計算 MD5"""
    try:
        # 針對 Linux 系統使用 md5sum
        stdin, stdout, stderr = ssh_client.exec_command(f"md5sum {filepath}")
        # 輸出格式通常是 "hash_value  filename"，取第一個欄位
        result = stdout.read().decode().strip().split()[0]
        return result
    except Exception as e:
        print(f"MD5 Calculation Error: {e}")
        return None

def sftp_stream_generator(ssh_client, sftp_client, chunk_size=64*1024):
    """
    負責串流讀取 SFTP 檔案的 Generator
    """
    remote_file = None
    try:
        # 'rb' 關鍵：以二進位讀取，不做任何編碼轉換
        remote_file = sftp_client.open(REMOTE_FILE_PATH, 'rb')
        
        while True:
            chunk = remote_file.read(chunk_size)
            if not chunk:
                break
            yield chunk

    except Exception as e:
        print(f"Streaming Error: {e}")
        # 這裡無法改變已經送出的 Header，只能中斷連線
        raise e
    finally:
        if remote_file: remote_file.close()
        # 注意：SSH/SFTP client 的關閉通常在更外層或依賴 GC，
        # 但在這個簡單範例中，我們讓它們隨 request 結束 (FastAPI background task 可優化此處)

@app.get("/trigger-transfer")
def transfer_file():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        # 1. 建立連線
        print("Connecting to Physical Machine...")
        ssh.connect(PHYSICAL_HOST, username=PHYSICAL_USER, password=PHYSICAL_PASSWORD)
        sftp = ssh.open_sftp()
        
        # 2. Sanity Check 準備：取得檔案大小與 MD5
        # (注意：算大檔 MD5 會花時間，導致 K8s 等待回應，請評估是否必要)
        print("Calculating MD5 & Stat...")
        file_attr = sftp.stat(REMOTE_FILE_PATH)
        file_size = file_attr.st_size
        md5_hash = get_remote_md5(ssh, REMOTE_FILE_PATH)
        
        print(f"Ready to stream. Size: {file_size}, MD5: {md5_hash}")

        # 3. 設定 Headers 供 K8s 驗證
        headers = {
            "Content-Disposition": f"attachment; filename=downloaded_data.dat",
            "Content-Length": str(file_size),
            "X-File-Checksum": md5_hash if md5_hash else "",
            "X-File-Source": "Physical-Machine"
        }

        # 4. 回傳串流回應 (StreamingResponse)
        # 注意：這裡將 ssh 傳入 generator 並不是最佳實踐 (因 scope 問題)，
        # 實務上建議使用 dependencies 或 middleware 管理連線生命週期。
        # 為了簡化，這裡假設 generator 跑完後，Python GC 會處理，或可使用 BackgroundTasks 關閉 SSH。
        
        return StreamingResponse(
            sftp_stream_generator(ssh, sftp),
            media_type="application/octet-stream",
            headers=headers
        )

    except Exception as e:
        print(f"Setup Error: {e}")
        if ssh: ssh.close()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # workers=1 對應 Sync 模式的 ThreadPool
    uvicorn.run(app, host="0.0.0.0", port=8000)
