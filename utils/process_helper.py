from multiprocessing import Process
from utils.data_loader import DataLoader
from utils.event_sender import EventSender
from datetime import datetime
import logging

class SendDataProcess(Process):
    def __init__(self, file_path):
        super(SendDataProcess, self).__init__()
        self.file_path = file_path

        # Hardcode for now
        self.columns_seletion = ["UserID", "MSISDN", "StartDate(ms)", "Duration(ms)", "UploadVolume", "DownloadVolume",
                            "HttpHost", "HttpUri"]
        self.rename_dict = {"StartDate(ms)": "StartDate", "Duration(ms)": "Duration"}
        self.fill_na_dict = {"HttpHost": "", "HttpUri": ""}
        self.concat_dict = {"Url": ["HttpHost", "HttpUri"]}

        self.connection_str = 'Endpoint=sb://jademo-eventhubs.servicebus.windows.net/;SharedAccessKeyName=FullAccessPolicy;SharedAccessKey=At6ZRNcxdsdIO2m2Xx1kpHhAjwDK6ht4C2gObpEiaHY=;EntityPath=m1-eventhub-test-minhnkb'
        self.eventhub_name = 'm1-eventhub-test-minhnkb'

    def run(self):
        start = datetime.now()
        thread_id = start.strftime('%Y%m%d%H%M%S')
        logging.info("Thread %s - %s started" % (thread_id, self.file_path))

        data_loader = DataLoader()

        processed_df = data_loader.load(self.file_path, self.columns_seletion, rename_dict=self.rename_dict,
                                        fill_na_dict=self.fill_na_dict, concat_dict=self.concat_dict)
        step = datetime.now()
        logging.info("Thread %s - %s loaded data - Time: %d" % (thread_id, self.file_path, (step - start).seconds))

        event_sender = EventSender(self.connection_str, self.eventhub_name, 4000)
        event_sender.send(processed_df)
        event_sender.close()

        step = datetime.now()
        logging.info("Thread %s - %s stopped - Time: %d" % (thread_id, self.file_path, (step - start).seconds))

class ProcessHelper:
    def __init__(self):
        self.process_list = []

    def start_process(self, file_path):
        process = SendDataProcess(file_path)
        process.start()
        self.process_list.append(process)

    def join_all(self):
        for process in self.process_list:
            process.join()