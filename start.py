import argparse
import json
from utils.file_watcher import FileWatcher
from utils.process_helper import ProcessHelper
import time

import logging
logging.basicConfig(level=logging.INFO)

def start(config_file):
    process_helper = None
    try:
        with open(config_file) as file:
            configs = json.load(file)
        process_helper = ProcessHelper(configs["sftp"], configs["load_configs"], configs["eventhub"], configs["metadata"])

        sftp_configs = configs["sftp"]
        file_watcher = FileWatcher(sftp_configs["host"], sftp_configs["port"],
                                   sftp_configs["username"], sftp_configs["password"])
        is_initial_run = True
        processed_files = ["randomize_sample_v1.gz"]

        while True:
            if is_initial_run:
                files_to_process = file_watcher.get_files_to_process(sftp_configs["folder_path"],
                                                                     sftp_configs["start_reload_from_seconds"], processed_files)
                is_initial_run = False
            else:
                files_to_process = file_watcher.get_files_to_process(sftp_configs["folder_path"],
                                                                     sftp_configs["process_from_last_seconds"], processed_files)
            for file in files_to_process:
                processed_files.append(file.filename)
                file_path = "%s/%s" % (sftp_configs["folder_path"], file.filename)
                process_helper.start_process(file_path)

            time.sleep(configs["process_interval_seconds"])
    except Exception as ex:
        pass
    finally:
        if process_helper is not None:
            process_helper.join_all()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start the streaming process.')
    parser.add_argument('-c', '--config_file', help='Path for the configuration file', required=True)
    args = parser.parse_args()

    start(args.config_file)