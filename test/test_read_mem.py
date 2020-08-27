import threading, os, time, paramiko
import pandas as pd
from io import StringIO, BytesIO

#you could make the number of threads relative to file size
NUM_THREADS = 1
MAX_RETRIES = 10

ftp_server = "20.184.62.23"
port = 22
sftp_file = "/home/minhnkb/m1-events-stream/sample-data/randomize_sample_v1.csv.gz"
# sftp_file = "/home/minhnkb/m1-events-stream/sample-data/small_sample.gz"
local_file = "./randomize_sample_v1.csv.gz"
ssh_conn = sftp_client = None
username = "jaadmin"
password = "JAWelcome@2020"

start_time = time.time()

for retry in range(MAX_RETRIES):
    try:
        ssh_conn = paramiko.Transport((ftp_server, port))
        ssh_conn.connect(username=username, password=password)
        # method 1 using sftpfile.get and settings window_size, max_packet_size
        window_size = pow(4, 12)#about ~16MB chunks
        max_packet_size = pow(4, 12)
        sftp_client = paramiko.SFTPClient.from_transport(ssh_conn, window_size=window_size, max_packet_size=max_packet_size)
        sftp_client.get(sftp_file, local_file)
        # method 2 breaking up file into chunks to read in parallel
        sftp_client = paramiko.SFTPClient.from_transport(ssh_conn)
        filesize = sftp_client.stat(sftp_file).st_size
        chunksize = pow(4, 12)#<-- adjust this and benchmark speed
        chunks = [(offset, chunksize) for offset in range(0, filesize, chunksize)]
        all_data = bytearray()
        with sftp_client.open(sftp_file, "rb") as infile:
            for chunk in infile.readv(chunks):
                all_data += bytearray(chunk)
        print("Load all bytes")

        data = BytesIO(all_data)
        raw_df = pd.read_csv(data, compression="gzip", error_bad_lines=False)

        print(raw_df.head(6))
        break
    except (EOFError, paramiko.ssh_exception.SSHException, OSError) as x:
        retry += 1
        print("%s %s - > retrying %s..." % (type(x), x, retry))
        time.sleep(abs(retry) * 10)
        # back off in steps of 10, 20.. seconds
    finally:
        if hasattr(sftp_client, "close") and callable(sftp_client.close):
            sftp_client.close()
        if hasattr(ssh_conn, "close") and callable(ssh_conn.close):
            ssh_conn.close()


print("Loading File %s Took %d seconds " % (sftp_file, time.time() - start_time))