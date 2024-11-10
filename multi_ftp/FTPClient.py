import io
import os
import time
import threading
import queue
import logging
from ftplib import FTP, error_perm, error_temp, ConnectionError, TimeoutError
from queue import Queue
from typing import List, Tuple, Generator
from contextlib import contextmanager

class FTPClient:
    def __init__(self, ftp_hosts: List[Tuple[str, int]], username: str, password: str,
                 max_retries: int = 3, retry_delay: float = 1.0):
        """Initialize FTPClient class
        
        Args:
            ftp_hosts: List of FTP servers, each element is a (host, port) tuple
            username: FTP username
            password: FTP password
            max_retries: Maximum number of retry attempts
            retry_delay: Retry delay time (seconds)
        """
        self.ftp_hosts = ftp_hosts
        self.username = username
        self.password = password
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.task_queue = Queue()
        self.logger = logging.getLogger(__name__)

    @contextmanager
    def _ftp_connection(self, host: str, port: int) -> Generator[FTP, None, None]:
        """Creates an FTP connection context manager"""
        ftp = FTP()
        try:
            ftp.connect(host, port, timeout=30)
            ftp.login(self.username, self.password)
            yield ftp
        finally:
            try:
                ftp.quit()
            except:
                pass

    def _ftp_upload(self):
        """Worker thread for handling individual FTP upload tasks"""
        while True:
            try:
                host, port, start_pos, end_pos, retry_count = self.task_queue.get_nowait()
            except Queue.Empty:
                break

            try:
                with self._ftp_connection(host, port) as ftp:
                    with open(self.file_path, 'rb') as f:
                        f.seek(start_pos)
                        bytes_remaining = end_pos - start_pos
                        data = f.read(bytes_remaining)
                        data_io = io.BytesIO(data)
                        ftp.storbinary(f'STOR {os.path.basename(self.file_path)}', 
                                     data_io, rest=start_pos)
                    
                    self.logger.info(f"成功上傳: {self.file_path} 的部分 {start_pos}-{end_pos} 到 {host}:{port}")

            except (error_perm, error_temp, ConnectionError, TimeoutError) as e:
                self.logger.error(f"upload failed: {self.file_path} 的部分 {start_pos}-{end_pos} 到 {host}:{port}, error: {str(e)}")
                if retry_count < self.max_retries:
                    self.task_queue.put((host, port, start_pos, end_pos, retry_count + 1))
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error(f"已達最大重試次數,放棄上傳: {self.file_path} 的部分 {start_pos}-{end_pos}")
            finally:
                self.task_queue.task_done()

    def transfer(self, file_path: str) -> bool:
        """Execute multi-threaded FTP transfer
        
        Args:
            file_path: Path to the file to be uploaded
            
        Returns:
            bool: Whether the transfer was successful
        """
        try:
            self.file_path = file_path
            file_size = os.path.getsize(file_path)
            part_size = file_size // len(self.ftp_hosts)

            # 初始化任務隊列
            for i, (host, port) in enumerate(self.ftp_hosts):
                start_pos = i * part_size
                end_pos = file_size if i == len(self.ftp_hosts) - 1 else (i + 1) * part_size
                self.task_queue.put((host, port, start_pos, end_pos, 0))

            # 啟動工作線程
            threads = []
            for _ in range(len(self.ftp_hosts)):
                thread = threading.Thread(target=self._ftp_upload)
                thread.daemon = True
                threads.append(thread)
                thread.start()

            self.task_queue.join()
            return True

        except Exception as e:
            self.logger.error(f"傳輸過程中發生錯誤: {e}")
            return False
        
    # ... existing code ...

    def _ftp_download(self):
        """工作執行緒處理個別 FTP 下載任務"""
        while True:
            try:
                host, port, start_pos, end_pos, retry_count = self.task_queue.get_nowait()
            except Queue.Empty:
                break

            try:
                with self._ftp_connection(host, port) as ftp:
                    # 創建臨時緩衝區接收數據
                    temp_buffer = io.BytesIO()
                    
                    # 設置回調函數來處理數據
                    def callback(data):
                        temp_buffer.write(data)
                    
                    # 使用 REST 命令設置起始位置
                    ftp.voidcmd(f'REST {start_pos}')
                    
                    # 下載指定範圍的數據
                    ftp.retrbinary(f'RETR {os.path.basename(self.remote_path)}', 
                                 callback,
                                 blocksize=8192)
                    
                    # 寫入文件的指定位置
                    with open(self.local_path, 'r+b') as f:
                        f.seek(start_pos)
                        f.write(temp_buffer.getvalue()[:end_pos-start_pos])
                    
                    self.logger.info(f"成功下載: {self.remote_path} 的部分 {start_pos}-{end_pos} 從 {host}:{port}")

            except (error_perm, error_temp, ConnectionError, TimeoutError) as e:
                self.logger.error(f"下載失敗: {self.remote_path} 的部分 {start_pos}-{end_pos} 從 {host}:{port}, 錯誤: {str(e)}")
                if retry_count < self.max_retries:
                    self.task_queue.put((host, port, start_pos, end_pos, retry_count + 1))
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error(f"已達最大重試次數,放棄下載: {self.remote_path} 的部分 {start_pos}-{end_pos}")
            finally:
                self.task_queue.task_done()

    def download(self, remote_path: str, local_path: str) -> bool:
        """執行多執行緒 FTP 下載
        
        Args:
            remote_path: 遠端文件路徑
            local_path: 本地保存路徑
            
        Returns:
            bool: 下載是否成功
        """
        try:
            self.remote_path = remote_path
            self.local_path = local_path
            
            # 獲取遠端文件大小
            with self._ftp_connection(self.ftp_hosts[0][0], self.ftp_hosts[0][1]) as ftp:
                file_size = ftp.size(remote_path)
            
            # 創建空白本地文件
            with open(local_path, 'wb') as f:
                f.truncate(file_size)
            
            part_size = file_size // len(self.ftp_hosts)
            
            # 初始化任務隊列
            for i, (host, port) in enumerate(self.ftp_hosts):
                start_pos = i * part_size
                end_pos = file_size if i == len(self.ftp_hosts) - 1 else (i + 1) * part_size
                self.task_queue.put((host, port, start_pos, end_pos, 0))
            
            # 啟動工作執行緒
            threads = []
            for _ in range(len(self.ftp_hosts)):
                thread = threading.Thread(target=self._ftp_download)
                thread.daemon = True
                threads.append(thread)
                thread.start()
            
            self.task_queue.join()
            return True
            
        except Exception as e:
            self.logger.error(f"下載過程中發生錯誤: {e}")
            return False