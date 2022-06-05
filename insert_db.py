import json
import sys
import logging
import pymongo
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def read_settings(args):
  number_of_arguments = 5
  if len(args) != number_of_arguments:
    logger.error('Invalid number of script arguments. Expected %d, got %d.', number_of_arguments, len(args))
    sys.exit(1)

  try:
    db_url = str(args[0])
    db_name = str(args[1])
    collection = str(args[2])
  except json.JSONDecodeError as ex:
    logger.error('Cannot decode json in scripts settings: %s', ex.msg)
    sys.exit(1)

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

def insert_table(db_uri, db_name, collection):
  with open('data.json') as f:
    data = json.load(f)
#uri = "mongodb://user:password@example.com/?authSource=the_database&authMechanism=SCRAM-SHA-1"
#client = MongoClient(uri)
  client = pymongo.MongoClient(db_uri)
  mydb = client[db_name]
  collection = mydb["student_info"]
  collection.insert_many(data)

def main(args):
    db_host, db_uri, db_name, db_username, db_passwd, collection = read_settings(args)
    status = test_db_connection(db_host, db_uri, db_name, db_username, db_passwd)
    if status == 0:
      insert_table(db_uri, db_name, collection)

if __name__ == '__main__':
  main(sys.argv[1:])