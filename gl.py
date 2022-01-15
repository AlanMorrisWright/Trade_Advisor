import sqlite3


# set up db file and global variable to it
DB_PATH = 'C:\\!\\CODING\\ED\\Trade_Advisor\\EDDB_Data\\2022_01_15\\'
DB_NAME = 'moz.db'
DB = DB_PATH + DB_NAME
DB_CONNECTION = sqlite3.connect(DB_PATH + DB_NAME)

# folder where EDDB source files are held
# source files obtained from here: https://eddb.io/api
#   systems.csv
#   stations.jsonl
#   listings.csv
#   commodities.json
EDDB_PATH = DB_PATH
