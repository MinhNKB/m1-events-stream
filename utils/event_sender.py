from azure.eventhub import EventHubProducerClient, EventData
import json
import sys
import logging
import os

class EventSender:
    def __init__(self, connection_str, eventhub_name, max_event_per_batch, max_retry, metadata, zvelo_helper):
        self.event_sender = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)
        self.event_count = max_event_per_batch
        self.max_retry = max_retry # Retry maximum 5 times before throw exeption
        self.metadata = metadata

        self.BATCH_LIMIT_SIZE = 1048576  # 1MB
        self.EVENT_COUNT_REDUCE_PERCENTAGE = 0.95
        self.METADATA_NAME = "metadata"
        self.COLUMNS_NAME = "columns"
        self.VALUES_NAME = "values"
        self.ZVELO_NAME = "zvelo"

        self.zvelo_helper = zvelo_helper

    def send(self, df):
        for i in range(0, len(df.index), self.event_count):
            start = i
            end = i + self.event_count
            if end > len(df.index):
                end = len(df.index)
            splitted_df = df.iloc[start : end]
            self.__send_batch(splitted_df)

    def __send_batch(self, df):
        columns = df.columns.tolist()
        values = df.values.tolist()
        urls = df["httphost"].unique().tolist()

        zvelo_data = self.zvelo_helper.process_list_urls(urls)

        data_dict = {self.COLUMNS_NAME : columns, self.VALUES_NAME : values, self.ZVELO_NAME : zvelo_data}

        if self.metadata is not None:
            data_dict[self.METADATA_NAME] = self.metadata

        json_object_array = []
        # Double check size of data
        json_string = json.dumps(data_dict)

        # logging.info("Message size %d" % sys.getsizeof(json_string))
        if sys.getsizeof(json_string) >= self.BATCH_LIMIT_SIZE:
            # Split to two before sending data
            data_dict_1 = {}
            data_dict_2 = {}
            half_length = len(data_dict[self.VALUES_NAME]) // 2
            data_dict_1[self.COLUMNS_NAME] = data_dict[self.COLUMNS_NAME]
            data_dict_1[self.VALUES_NAME] = data_dict[self.VALUES_NAME][:half_length]
            data_dict_1[self.ZVELO_NAME] = data_dict[self.ZVELO_NAME]

            data_dict_2[self.COLUMNS_NAME] = data_dict[self.COLUMNS_NAME]
            data_dict_2[self.VALUES_NAME] = data_dict[self.VALUES_NAME][half_length:]
            data_dict_2[self.ZVELO_NAME] = data_dict[self.ZVELO_NAME]

            if self.metadata is not None:
                data_dict_1[self.METADATA_NAME] = self.metadata
                data_dict_2[self.METADATA_NAME] = self.metadata

            json_object_array.append(json.dumps(data_dict_1))
            json_object_array.append(json.dumps(data_dict_2))

            self.event_count = int(self.event_count * self.EVENT_COUNT_REDUCE_PERCENTAGE)
        else:
            # Send whole batch
            json_object_array.append(json_string)

        for json_object in json_object_array:
            batch_data = self.event_sender.create_batch()
            batch_data.add(EventData(json_object))
            success = False
            retry_count = self.max_retry
            while success == False:
                try:
                    self.event_sender.send_batch(batch_data)
                    success = True
                except:
                    retry_count -= 1
                    if retry_count == 0:
                        raise RuntimeError("Cannot send data to Event Hub")

    def close(self):
        self.event_sender.close()