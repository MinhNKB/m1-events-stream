import paramiko
import time

class FileWatcher:
    def __init__(self, host, port, username, password):
        self.transport = paramiko.Transport((host,port))
        self.transport.connect(None, username, password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def get_files_to_process(self, folder_path, from_last_seconds = 10, processed_list = []):
        all_files = self.sftp.listdir_attr(folder_path)
        current_unix_time = int(time.time())

        all_files_filtered = filter(lambda file: ((current_unix_time - file.st_mtime) < from_last_seconds) \
                                                 and (file.filename not in processed_list), all_files)

        return all_files_filtered

    def close(self):
        if hasattr(self.sftp, "close") and callable(self.sftp.close):
            self.sftp.close()
        if hasattr(self.transport, "close") and callable(self.transport.close):
            self.transport.close()