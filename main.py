"__author__ = 'Micah Parker'"
"__credits__ = 'Decisive Data 2018'"
"__project__ = 'kbc_writer_powerbi'"

"""
Python 3 environment 
"""

import pip
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'logging_gelf'])

import sys
import os
import logging
import csv
import json
import datetime
import pandas as pd
import logging_gelf.formatters
import logging_gelf.handlers
import httplib2
from keboola import docker

### Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

### Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

### Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()

### Get proper list of tables
cfg = docker.Config('/data/')
in_tables = cfg.get_input_tables()
logging.info("IN tables mapped: "+str(in_tables))

### destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"


def get_tables(in_tables):
    """
    Evaluate input table names.
    """
    input_list = []

    ### input file
    for table in in_tables:
        in_name = table["full_path"]
        in_destination = table["destination"]
        logging.info("Data table: " + str(in_name))
        logging.info("Input table source: " + str(in_destination))
        input_list.append(in_name)
    
    return input_list

def main():
    """
    Main execution script.
    """
    table_list = get_tables(in_tables)
    body = ""
    for i in table_list:
        filename = i.split("/data/in/tables/")[1]
        table = filename.split(".").pop()
        logging.info("Inputting: {0}".format(filename))
        #with open(i, mode="rt") as in_file:
        with open(i, mode="rt", encoding="utf-8") as in_file:
            lazy_lines = (line.replace("\0", "") for line in in_file)
            reader = csv.DictReader(lazy_lines, lineterminator="\n")
            logging.info("Outputting: {0}".format(filename))
            for row in reader:
                if (len(body)):
                    body += ","
                body += json.dumps(row)
        h = httplib2.Http(".cache")
        #upload to powerbi!
        logging.info("Uploading: " + "https://api.powerbi.com/v1.0/myorg/datasets/" + params["dataset_id"] + "/tables/" + table + "/rows")
        (resp, content) = h.request("https://api.powerbi.com/v1.0/myorg/datasets/" + params["dataset_id"] + "/tables/" + table + "/rows",
                        "POST", 
                        body = "{\"rows\":[" + body + "]}",
                        headers = {
                            "content-type": "application/json",
                            "Authorization": "Bearer " + params["token"]
                        })
    return


if __name__ == "__main__":

    main()

    logging.info("Done.")
