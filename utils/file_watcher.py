import paramiko
import time

class FileWatcher:
    def __init__(self, host, port, username, password, ssh_key_path):
        self.transport = paramiko.Transport((host,port))
        if ssh_key_path is not None:
            pkey = paramiko.RSAKey.from_private_key_file(ssh_key_path)
            self.transport.connect(None, username, pkey=pkey)
        else:
            self.transport.connect(None, username, password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def get_files_to_process(self, folder_path, from_last_seconds, processed_list):
        all_files = self.sftp.listdir_attr(folder_path)
        current_unix_time = int(time.time())

        all_files_filtered = filter(lambda file: ((current_unix_time - file.st_mtime) < from_last_seconds) \
                                                 and (("%s/%s" % (folder_path, file.filename)) not in processed_list)
                                                 and ("000000000" not in file.filename) , all_files)

        return all_files_filtered

    def close(self):
        if self.sftp is not None:
            self.sftp.close()
        if self.transport is not None:
            self.transport.close()