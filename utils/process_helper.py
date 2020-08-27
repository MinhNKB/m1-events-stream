from multiprocessing import Process
from utils.sftp_reader import SFTPReader
from utils.data_loader import DataLoader
from utils.event_sender import EventSender
from datetime import datetime
import logging

class SendDataProcess(Process):
    def __init__(self, sftp_configs, load_configs, eventhub_configs, metadata, file_path):
        super(SendDataProcess, self).__init__()
        self.file_path = file_path

        # SFTP
        self.host = sftp_configs["host"]
        self.port = sftp_configs["port"]
        self.username = sftp_configs["username"]
        self.password = sftp_configs["password"]
        self.sftp_max_retry = sftp_configs["max_retry"]

        # Load
        self.columns_seletion = load_configs["columns_seletion"]
        self.rename_dict = load_configs["rename_dict"]
        self.fill_na_dict = load_configs["fill_na_dict"]
        self.concat_dict = load_configs["concat_dict"]

        # Eventhub
        self.connection_string = eventhub_configs["connection_string"]
        self.eventhub_name = eventhub_configs["eventhub_name"]
        self.max_event_per_batch = eventhub_configs["max_event_per_batch"]
        self.eventhub_max_retry = eventhub_configs["max_retry"]

        # Metadata
        self.metadata = metadata
        self.metadata["filepath"] = file_path

    def run(self):
        start = datetime.now()
        thread_id = start.strftime('%Y%m%d%H%M%S')
        logging.info("Thread %s - %s started" % (thread_id, self.file_path))

        sftp_reader = SFTPReader(self.host, self.port, self.username, self.password, self.sftp_max_retry)
        byte_io = sftp_reader.load_file(self.file_path)

        step = datetime.now()
        logging.info("Thread %s - %s loaded data - Time: %d" % (thread_id, self.file_path, (step - start).seconds))

        data_loader = DataLoader()

        processed_df = data_loader.load(byte_io, self.columns_seletion, fill_na_dict=self.fill_na_dict,
                                        concat_dict=self.concat_dict, rename_dict=self.rename_dict)
        step = datetime.now()
        logging.info("Thread %s - %s parsed data - Time: %d" % (thread_id, self.file_path, (step - start).seconds))

        event_sender = EventSender(self.connection_string, self.eventhub_name, self.max_event_per_batch, self.eventhub_max_retry, self.metadata)

        event_sender.send(processed_df)
        event_sender.close()

        step = datetime.now()
        logging.info("Thread %s - %s stopped - Time: %d" % (thread_id, self.file_path, (step - start).seconds))

class ProcessHelper:
    def __init__(self, sftp_configs, load_configs, eventhub_configs, metadata):
        self.sftp_configs = sftp_configs
        self.load_configs = load_configs
        self.eventhub_configs = eventhub_configs
        self.metadata = metadata
        self.process_list = []

    def start_process(self, file_path):
        process = SendDataProcess(self.sftp_configs, self.load_configs, self.eventhub_configs, self.metadata, file_path)
        process.start()
        self.process_list.append(process)

    def join_all(self):
        for process in self.process_list:
            process.join()