import argparse
import json
from utils.file_watcher import FileWatcher
from utils.process_helper import ProcessHelper
from utils.zvelo_helper import ZveloHelper
import time
import os

import logging
logging.basicConfig(level=logging.INFO)

def start(config_file):
    process_helper = None
    file_watcher = None
    try:
        with open(config_file) as file:
            configs = json.load(file)

        if (configs["processed_files_log"] is not None) and (os.path.exists(configs["processed_files_log"])):
            processed_files = set(line.strip() for line in open(configs["processed_files_log"]))
        else:
            processed_files = set()

        process_helper = ProcessHelper(configs["sftp"], configs["load_configs"], configs["zvelo"],
                                       configs["eventhub"], configs["adls"], configs["metadata"],
                                       configs["max_process_count"], configs["processed_files_log"])

        sftp_configs = configs["sftp"]
        file_watcher = FileWatcher(sftp_configs["host"], sftp_configs["port"],
                                   sftp_configs["username"], sftp_configs["password"],
                                   sftp_configs["ssh_key_path"])
        is_initial_run = True

        while True:
            if is_initial_run:
                files_to_process = file_watcher.get_files_to_process(sftp_configs["folder_path"],
                                                                     sftp_configs["start_reload_from_seconds"], processed_files)
                is_initial_run = False
            else:
                files_to_process = file_watcher.get_files_to_process(sftp_configs["folder_path"],
                                                                     sftp_configs["process_from_last_seconds"], processed_files)

            for file in files_to_process:
                file_path = "%s/%s" % (sftp_configs["folder_path"], file.filename)
                processed_files.add(file_path)
                process_helper.add_file(file_path)

            process_helper.start_process()

            time.sleep(configs["process_interval_seconds"])
    except Exception as ex:
        pass
    finally:
        if process_helper is not None:
            process_helper.join_all()
        if file_watcher is not None:
            file_watcher.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start the streaming process.')
    parser.add_argument('-c', '--config_file', help='Path for the configuration file', required=True)
    args = parser.parse_args()

    start(args.config_file)