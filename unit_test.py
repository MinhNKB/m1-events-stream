from utils.process_helper import ProcessHelper
import logging
logging.basicConfig(level=logging.INFO)
def main():
    file_path = "./sample-data/random_sample.csv.gz"
    process_helper = ProcessHelper()
    for i in range(1):
        process_helper.start_process(file_path)
    process_helper.join_all()

if __name__ == '__main__':
    logging.debug("Starting......")
    main()