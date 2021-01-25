import os
import requests
import json
import csv 
import pandas as pd
from datetime import datetime
from pathlib import Path


import logging


##########
# Logging
#
log_filename = os.getcwd() + "/logs/scraping.log"
os.makedirs(os.path.dirname(log_filename), exist_ok=True)

logger = logging.getLogger("scraping")
file_handler = logging.FileHandler(log_filename)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)
##########

target_file = 'vacinacao.csv'

class VaccinesScrapping:
    """
    Class to manage scrapping of the official vacinnes data of Portugal, 
    Source from dashboard of min-saude
    """

    def start(self):
        self.__get_vacines_status()

    def __get_vacines_status(self):
        """
        Handles current information form vaccines and save it
        """

        data = self.__get_data()
        data = self.__parse(data)
        self.__write_backup(data)
        self.__update_report(data)

    def __parse(self, data):
        """
        Parses the timestamp to human friendly
        """

        parsed = []

        for day in data: 
            if 'attributes' in day: 
                attributes = day['attributes']
                if 'Data' in attributes:
                    timestamp = attributes['Data']
                    attributes['DataISO'] = datetime.fromtimestamp(timestamp / 1000).isoformat()
                parsed.append(attributes)

        return parsed

    def __get_data(self):
        """
        Does a GET to the endpoint with portugal vacinnes the data and returns 
        a json
        """

        url = os.getenv('ENV_VACC', '')
        response = requests.get(url)

        assert (
            response.ok
        ), f"Cannot get data from {url}: HTTP response code {response.status_code}"

        data = json.loads(response.text)

        if 'features' in data:
            current = data['features']
            logger.info("Data collected")
            return current

        logger.e("There is no 'features' information")

        return ''

    
    def __update_report(self, data):
        """
        Updates the new CSV with the data recently collected
        """

        if self.backup_filename:
            df = pd.read_csv(self.backup_filename)

            if Path(target_file).is_file():
                df_current = pd.read_csv(target_file)
                df_current.merge(df, left_on='Data', right_on='Data')
                df_current.to_csv(target_file, index=False)
            else:
                df.to_csv(target_file, index=False)


        logger.info("CSV Created")

    def __write_backup(self, data):
        """
        Writes the CSV in a backup
        """

        self.backup_filename = f"{os.getcwd()}/backup/{datetime.now()}.csv"
        os.makedirs(os.path.dirname(self.backup_filename), exist_ok=True)
        self.__write_file(data, self.backup_filename) 

    def __write_file(self, data, filename):
        """
        Write the data into %filename.
        """

        data_file = open(filename, 'w') 
        csv_writer = csv.writer(data_file) 
        count = 0
        
        for idx, day in enumerate(data):
            if count == 0:
                header = day.keys() 
                csv_writer.writerow(header) 
                count += 1
            csv_writer.writerow(day.values()) 
        
        data_file.close()


if __name__ == "__main__":
    scrap = VaccinesScrapping()
    scrap.start()