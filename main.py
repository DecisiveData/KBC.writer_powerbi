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

def truncate(workspace_id, dataset_id, table, token):
    #gen url
    url = "https://api.powerbi.com/v1.0/myorg"
    if workspace_id:
        url += "/groups/" + workspace_id
    #run truncate
    h = httplib2.Http(".cache")
    logging.info("Truncate: " + url + "/datasets/" + dataset_id + "/tables/" + table + "/rows")
    (resp, content) = h.request(url + "/datasets/" + dataset_id + "/tables/" + table + "/rows",
                    "DELETE", 
                    headers = {
                        "content-type": "application/json",
                        "Authorization": "Bearer " + token
                    })
    if resp["status"] != "200":
        raise Exception('Error truncating table: ' + table + "\n\n" + str(content))

def upload(workspace_id, dataset_id, table, body, token):
    #gen url
    url = "https://api.powerbi.com/v1.0/myorg"
    if workspace_id:
        url += "/groups/" + workspace_id
    #run upload
    h = httplib2.Http(".cache")
    logging.info("Uploading: " + url + "/datasets/" + dataset_id + "/tables/" + table + "/rows  (" + str(len(body)) + " bytes)")
    (resp, content) = h.request(url + "/datasets/" + dataset_id + "/tables/" + table + "/rows",
                    "POST", 
                    body = "{\"rows\":[" + body + "]}",
                    headers = {
                        "content-type": "application/json",
                        "Authorization": "Bearer " + token
                    })
    if resp["status"] != "200":
        raise Exception('Error uploading data into table: ' + table + "\n\n" + str(content))

def main():
    """
    Main execution script.
    """
    batchSize = 9999
    if params["batchSize"]:
        batchSize = int(params["batchSize"])
    table_list = get_tables(in_tables)
    for i in table_list:
        filename = i.split("/data/in/tables/")[1]
        filename_split = filename.split(".")
        ext = filename_split.pop()
        table = filename_split.pop()
        logging.info("Processing Table: {0}".format(table))
        #with open(i, mode="rt") as in_file:
        with open(i, mode="rt", encoding="utf-8") as in_file:
            rowNum = 0
            body = ""
            lazy_lines = (line.replace("\0", "") for line in in_file)
            reader = csv.DictReader(lazy_lines, lineterminator="\n")
            #truncate the table first
            if params["truncate"]:
                truncate(params["workspace_id"], params["dataset_id"], table, params["token"])
            #batch add data back in
            for row in reader:
                if len(body) > 0:
                    body += ","
                #truncate any values that are > 4000 chars
                for key in row:
                    if len(str(row[key])) > 4000:
                        row[key] = str(row[key])[0:4000]
                body += json.dumps(row)
                rowNum += 1
                #upload in batches of 10k as per pbi api limits
                if rowNum == batchSize:
                    upload(params["workspace_id"], params["dataset_id"], table, body, params["token"])
                    rowNum = 0
                    body = ""
            #upload remaining data
            if len(body) > 0:
                upload(params["workspace_id"], params["dataset_id"], table, body, params["token"])

    return

if __name__ == "__main__":

    main()

    logging.info("Done.")
