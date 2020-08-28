from multiprocessing import Process
from Queue import Queue
from utils.sftp_reader import SFTPReader
from utils.data_loader import DataLoader
from utils.event_sender import EventSender
from utils.zvelo_helper import ZveloHelper
from datetime import datetime
import logging

class SendDataProcess(Process):
    def __init__(self, sftp_configs, load_configs, zvelo_helper, eventhub_configs, metadata, file_path):
        super(SendDataProcess, self).__init__()
        self.file_path = file_path

        # SFTP
        self.host = sftp_configs["host"]
        self.port = sftp_configs["port"]
        self.username = sftp_configs["username"]
        self.password = sftp_configs["password"]
        self.ssh_key_path = sftp_configs["ssh_key_path"]
        self.sftp_max_retry = sftp_configs["max_retry"]

        # Load
        self.columns_seletion = load_configs["columns_seletion"]
        self.rename_dict = load_configs["rename_dict"]
        self.fill_na_dict = load_configs["fill_na_dict"]
        self.concat_dict = load_configs["concat_dict"]

        # Zvelo
        self.zvelo_helper = zvelo_helper

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

        sftp_reader = SFTPReader(self.host, self.port, self.username, self.password,
                                 self.ssh_key_path, self.sftp_max_retry)
        byte_io = sftp_reader.load_file(self.file_path)

        step = datetime.now()
        logging.info("Thread %s - %s loaded data - Time: %d" % (thread_id, self.file_path, (step - start).seconds))

        data_loader = DataLoader()

        processed_df = data_loader.load(byte_io, self.columns_seletion, fill_na_dict=self.fill_na_dict,
                                        concat_dict=self.concat_dict, rename_dict=self.rename_dict)
        step = datetime.now()
        logging.info("Thread %s - %s parsed data - Time: %d" % (thread_id, self.file_path, (step - start).seconds))

        event_sender = EventSender(self.connection_string, self.eventhub_name, self.max_event_per_batch,
                                   self.eventhub_max_retry, self.metadata, self.zvelo_helper)

        event_sender.send(processed_df)
        event_sender.close()

        step = datetime.now()
        logging.info("Thread %s - %s stopped - Time: %d" % (thread_id, self.file_path, (step - start).seconds))

class ProcessHelper:
    def __init__(self, sftp_configs, load_configs, eventhub_configs, metadata,
                 max_process = 4, processed_file_log = None):
        self.sftp_configs = sftp_configs
        self.load_configs = load_configs
        self.eventhub_configs = eventhub_configs
        self.metadata = metadata

        self.process_queue = Queue(max_process)
        self.file_queue = Queue()
        self.processed_file_log = processed_file_log

        path = "/data/zvelo_urldb"
        host = ""
        serial = ""
        self.zvelo_helper = ZveloHelper(path, 0, host, serial)


    def add_file(self, file_path):
        self.file_queue.put(file_path)

    def join_all(self):
        for process in iter(self.process_queue.get, None):
            if process is not None:
                process.join()

    def start_process(self):
        if self.processed_file_log is not None:
            log_writer = open(self.processed_file_log, "a+")
        else:
            log_writer = None

        while (not self.process_queue.empty()) \
                and (not self.process_queue.queue[0].is_alive()):
            process = self.process_queue.get()
            if log_writer is not None:
                log_writer.write(process.file_path + '\n')

        if log_writer is not None:
            log_writer.close()

        while (not self.process_queue.full()) \
                and (not self.file_queue.empty()):
            file_path = self.file_queue.get()
            process = SendDataProcess(self.sftp_configs, self.load_configs, self.zvelo_helper,
                                      self.eventhub_configs, self.metadata, file_path)
            process.start()
            self.process_queue.put(process)