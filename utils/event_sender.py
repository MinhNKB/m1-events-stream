from azure.eventhub import EventHubProducerClient, EventData
import json
import sys
import logging

class EventSender:
    def __init__(self, connection_str, eventhub_name, event_per_batch):
        self.event_sender = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=eventhub_name)
        self.event_count = event_per_batch
        self.BATCH_LIMIT_SIZE = 1048576 # 1MB
        self.MAX_RETRY = 5 # Retry maximum 5 times before throw exeption


    def send(self, df):
        for i in range(0, df.size, self.event_count):
            splitted_df = df.iloc[i:i + self.event_count]
            self.__send_batch(splitted_df)

    def __send_batch(self, df):
        data_dict = {}

        for column in df.columns:
            data_dict[column] = df[column].tolist()

        json_object_array = []
        # Double check size of data
        # logging.info("Message size: %d" % sys.getsizeof(json.dumps(data_dict)))
        if sys.getsizeof(json.dumps(data_dict)) > self.BATCH_LIMIT_SIZE:
            # Split to two before sending data
            data_dict_1 = {}
            data_dict_2 = {}
            half_length = len(list(data_dict.values())[0]) // 2
            for key in data_dict.keys():
                data_dict_1[key] = data_dict[key][:half_length]
                data_dict_2[key] = data_dict[key][half_length:]

            json_object_array.append(json.dumps(data_dict_1))
            json_object_array.append(json.dumps(data_dict_2))
        else:
            # Send whole batch
            json_object_array.append(json.dumps(data_dict))

        for json_object in json_object_array:
            batch_data = self.event_sender.create_batch()
            batch_data.add(EventData(json_object))
            success = False
            retry_count = self.MAX_RETRY
            while success == False:
                try:
                    self.event_sender.send_batch(batch_data)
                    success = True
                except:
                    retry_count -= 1
                    if retry_count == 0:
                        raise ConnectionError("Cannot send data to Event Hub")

    def close(self):
        self.event_sender.close()
