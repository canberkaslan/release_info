#Info for relational and mongodb Dbs
#Relational concept	MongoDB equivalent
#Database	Database
#Tables	Collections
#Rows	Documents
#Index	Index


import os
import sys
import json
import pymongo
from bson import BSON
from bson import json_util
import logging

instance_id = ""
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()




def read_settings(args):
    number_of_arguments = 5
    if len(args) != number_of_arguments:
        logger.error('Invalid number of script arguments. Expected %d, got %d.', number_of_arguments, len(args))
        sys.exit(1)

    try:
        db_host = str(args[1])
        db_uri = str(args[1])
        db_name = str(args[2])
        db_username = str(args[3])
        db_passwd = str(args[4])
        collection = str(args[5])
    except json.JSONDecodeError as ex:
        logger.error('Cannot decode json in scripts settings: %s', ex.msg)
        sys.exit(1)


def read_table(db_uri, db_name, collection):
    client = pymongo.MongoClient(db_uri)
    data_db = client[db_name]
    data_collection = data_db[collection]
    list(data_db.data_collection.find({}))

def test_db_connection(db_host, db_uri, db_name, db_username):
    DATABASE_NAME = db_name
    DATABASE_HOST = db_host

    DATABASE_USERNAME = db_username
    DATABASE_PASSWORD = db_username

    try:
        myclient = pymongo.MongoClient(DATABASE_HOST)
        myclient.test.authenticate(DATABASE_USERNAME, DATABASE_PASSWORD)
        mydb = myclient[DATABASE_NAME]

        print("[+] Database connected!")
        return 0
    except Exception as e:
        print("[+] Database connection error!")
        raise e



def main(args):
    db_host, db_uri, db_name, db_username, db_passwd, collection = read_settings(args)
    status = test_db_connection(db_host, db_uri, db_name, db_username, db_passwd)
    if status == 0:
        read_table(db_uri, db_name, collection)

if __name__ == '__main__':
    main(sys.argv[1:])
