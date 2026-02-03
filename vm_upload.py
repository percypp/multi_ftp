import uvicorn
from fastapi import FastAPI, Request, HTTPException
import paramiko

app = FastAPI()

# --- 設定區 ---
PHYSICAL_HOST = "192.168.1.100"
PHYSICAL_USER = "admin"
PHYSICAL_PASSWORD = "password"
# 存到實體機的路徑
REMOTE_TARGET_PATH = "/data/uploads/uploaded_from_k8s.dat"

@app.post("/upload-stream")
async def upload_stream(request: Request):
    """
    接收 K8s 的串流，並直接寫入實體機
    注意：這裡必須用 async def 才能使用 request.stream()
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    sftp = None
    remote_file = None
    total_bytes = 0

    try:
        print("1. 連線實體機 SFTP...")
        ssh.connect(PHYSICAL_HOST, username=PHYSICAL_USER, password=PHYSICAL_PASSWORD)
        sftp = ssh.open_sftp()
        
        # 開啟實體機檔案 (Write Binary)
        # 'wb' 會覆蓋舊檔，若要續傳可用 'ab' (append) 但需處理 offset
        remote_file = sftp.open(REMOTE_TARGET_PATH, 'wb')
        
        print("2. 開始接收 K8s 資料流並轉寫...")
        
        # --- 關鍵迴圈 ---
        # request.stream() 會產生 generator，K8s 傳多少，這裡就收到多少
        async for chunk in request.stream():
            # 這一步是 Sync 的 (Paramiko 寫入)，會稍微卡住 Event Loop，
            # 但因為你是單一 Session 傳檔，這完全沒問題。
            remote_file.write(chunk)
            total_bytes += len(chunk)
        
        print(f"3. 傳輸完成。共寫入 {total_bytes} bytes。")
        
        # (選用) 傳完後，可以在這裡叫實體機算 MD5 回傳給 K8s
        # stdin, stdout, stderr = ssh.exec_command(f"md5sum {REMOTE_TARGET_PATH}")
        # remote_md5 = stdout.read().decode().split()[0]

        return {
            "status": "success", 
            "size_written": total_bytes,
            # "remote_md5": remote_md5 
        }

    except Exception as e:
        print(f"Upload Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
        
    finally:
        if remote_file: remote_file.close()
        if sftp: sftp.close()
        ssh.close()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
