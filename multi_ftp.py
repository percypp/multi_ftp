import threading
import os

def parallel_send(files, hosts_ports):
    threads = []
    for file, (host, port) in zip(files, hosts_ports):
        thread = threading.Thread(target=send_file, args=(file, host, port))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

from multiprocessing import Process

def start_multiple_servers(host, ports):
    processes = []
    for port in ports:
        process = Process(target=start_server, args=(host, port))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

def split_file(file_path, num_parts):
    file_size = os.path.getsize(file_path)
    part_size = file_size // num_parts
    file_parts = []

    with open(file_path, 'rb') as f:
        for i in range(num_parts):
            part_path = f"{file_path}.part{i}"
            with open(part_path, 'wb') as part_file:
                if i == num_parts - 1:  # 最後一塊可能會稍微大一點
                    part_file.write(f.read())
                else:
                    part_file.write(f.read(part_size))
            file_parts.append(part_path)

    return file_parts

def merge_files(output_path, part_paths):
    with open(output_path, 'wb') as merged_file:
        for part_path in part_paths:
            with open(part_path, 'rb') as part_file:
                merged_file.write(part_file.read())
                os.remove(part_path)  # 合併完後刪除分割檔案

def multi_ftp_transfer(file_path, ftp_hosts, username, password, max_retries=3):
    """
    使用多重FTP連線並行傳輸檔案,支援失敗重試
    
    Args:
        file_path: 要傳輸的檔案路徑
        ftp_hosts: FTP主機列表,格式為[(host1, port1), (host2, port2),...]
        username: FTP帳號
        password: FTP密碼
        max_retries: 最大重試次數
    """
    from ftplib import FTP, error_perm, error_temp
    import queue
    import time
    
    # 分割檔案
    num_parts = len(ftp_hosts)
    file_parts = split_file(file_path, num_parts)
    
    # 建立任務佇列
    task_queue = queue.Queue()
    for i, (part_path, (host, port)) in enumerate(zip(file_parts, ftp_hosts)):
        task_queue.put((part_path, host, port, 0))  # 最後的0是重試次數計數
        
    def ftp_upload(task_queue):
        while True:
            try:
                # 從佇列取得任務
                part_path, host, port, retry_count = task_queue.get_nowait()
            except queue.Empty:
                break
                
            try:
                ftp = FTP()
                ftp.connect(host, port, timeout=30)
                ftp.login(username, password)
                
                with open(part_path, 'rb') as f:
                    ftp.storbinary(f'STOR {os.path.basename(part_path)}', f)
                    
                ftp.quit()
                print(f"成功上傳: {part_path} 到 {host}:{port}")
                
            except (error_perm, error_temp, ConnectionError, TimeoutError) as e:
                print(f"上傳失敗: {part_path} 到 {host}:{port}, 錯誤: {str(e)}")
                if retry_count < max_retries:
                    # 重新加入佇列等待重試
                    task_queue.put((part_path, host, port, retry_count + 1))
                    time.sleep(1)  # 等待一秒後重試
                else:
                    print(f"已達最大重試次數,放棄上傳: {part_path}")
            
            finally:
                task_queue.task_done()
    
    # 建立多執行緒上傳
    threads = []
    for _ in range(num_parts):
        thread = threading.Thread(target=ftp_upload, args=(task_queue,))
        thread.daemon = True  # 設為守護執行緒,主程式結束時會一併結束
        threads.append(thread)
        thread.start()
        
    # 等待所有任務完成
    task_queue.join()

import threading
import os
import logging
from typing import List, Tuple, Optional
from ftplib import FTP, error_perm, error_temp
from queue import Queue
import time
from contextlib import contextmanager

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@contextmanager
def ftp_connection(host: str, port: int, username: str, password: str) -> FTP:
    """FTP連接的上下文管理器"""
    ftp = FTP()
    try:
        ftp.connect(host, port, timeout=30)
        ftp.login(username, password)
        yield ftp
    finally:
        try:
            ftp.quit()
        except:
            pass

def split_file(file_path: str, num_parts: int) -> List[str]:
    """改進的檔案分割函數"""
    if num_parts <= 0:
        raise ValueError("分割份數必須大於0")
        
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        raise ValueError("檔案大小不能為0")
        
    part_size = max(file_size // num_parts, 1024)  # 確保每個部分至少1KB
    file_parts = []

    try:
        with open(file_path, 'rb') as f:
            for i in range(num_parts):
                part_path = f"{file_path}.part{i}"
                with open(part_path, 'wb') as part_file:
                    content = f.read(part_size) if i < num_parts - 1 else f.read()
                    if not content:  # 如果沒有更多內容，提前結束
                        break
                    part_file.write(content)
                file_parts.append(part_path)
        return file_parts
    except IOError as e:
        logger.error(f"分割檔案時發生錯誤: {e}")
        raise

def multi_ftp_transfer(
    file_path: str,
    ftp_hosts: List[Tuple[str, int]],
    username: str,
    password: str,
    max_retries: int = 3,
    retry_delay: float = 1.0
) -> bool:
    """改進的多重FTP傳輸函數"""
    try:
        num_parts = len(ftp_hosts)
        file_parts = split_file(file_path, num_parts)
        task_queue: Queue = Queue()
        
        for i, (part_path, (host, port)) in enumerate(zip(file_parts, ftp_hosts)):
            task_queue.put((part_path, host, port, 0))
            
        def ftp_upload(task_queue: Queue) -> None:
            while True:
                try:
                    part_path, host, port, retry_count = task_queue.get_nowait()
                except Queue.Empty:
                    break
                    
                try:
                    with ftp_connection(host, port, username, password) as ftp:
                        with open(part_path, 'rb') as f:
                            ftp.storbinary(f'STOR {os.path.basename(part_path)}', f)
                        logger.info(f"成功上傳: {part_path} 到 {host}:{port}")
                        
                except (error_perm, error_temp, ConnectionError, TimeoutError) as e:
                    logger.error(f"上傳失敗: {part_path} 到 {host}:{port}, 錯誤: {str(e)}")
                    if retry_count < max_retries:
                        task_queue.put((part_path, host, port, retry_count + 1))
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"已達最大重試次數,放棄上傳: {part_path}")
                finally:
                    task_queue.task_done()
        
        threads = []
        for _ in range(num_parts):
            thread = threading.Thread(target=ftp_upload, args=(task_queue,))
            thread.daemon = True
            threads.append(thread)
            thread.start()
            
        task_queue.join()
        return True
        
    except Exception as e:
        logger.error(f"傳輸過程中發生錯誤: {e}")
        return False