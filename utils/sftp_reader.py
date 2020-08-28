import time, paramiko
from io import BytesIO
import logging

class SFTPReader:
    def __init__(self, host, port, username, password, ssh_key_path, max_retry):
        self.transport = paramiko.Transport((host, port))
        if ssh_key_path is not None:
            pkey = paramiko.RSAKey.from_private_key_file(ssh_key_path)
            self.transport.connect(None, username, pkey=pkey)
        else:
            self.transport.connect(None, username, password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        self.max_retry = max_retry

    def load_file(self, file_path):
        for retry in range(self.max_retry):
            try:
                logging.info("Trying to load from SFTP - %s" % retry)
                # breaking up file into chunks to read in parallel
                file_size = self.sftp.stat(file_path).st_size
                chunk_size = pow(4, 12)  # <-- adjust this and benchmark speed
                chunks = [(offset, chunk_size) for offset in range(0, file_size, chunk_size)]
                data_bytes = bytearray()
                with self.sftp.open(file_path, "rb") as file:
                    for chunk in file.readv(chunks):
                        data_bytes += bytearray(chunk)
                data_io = BytesIO(data_bytes)
                logging.info("Done SFTP load - %s" % retry)
                return data_io
            except (EOFError, paramiko.ssh_exception.SSHException, OSError) as ex:
                retry += 1
                # back off in steps of 5, 10, 15.. seconds
                time.sleep(abs(retry) * 5)
            finally:
                if self.sftp is not None:
                    self.sftp.close()
                if self.transport is not None:
                    self.transport.close()
        return None