#! Python35
# Script to load most recent FTP Server Log to database FTP_log_entries
import os
import re
import datetime
import logging
from logging.config import fileConfig
import pymssql


# set log file configuration
fileConfig('logging.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)
logger.info("Starting scheduled run...")

# database connection credentials
host = r"myserver\sqlserver"
user = "username"
password = "password"
database = "FTP_Server_Logs"

# set regex variables
regex = re.compile(r"(\(\d+\)) (\d+/\d+/\d+) (\d+:\d+:\d+) ([A|P]M) - (\w+[-]?\w+) \((\d+\.\d+\.\d+\.\d+)\)")
date_regex = r"(\d+)\/(\d+)\/(\d+)"


def establish_database_connection():
    """
    Establish connection to MSSQL database.
    Raises SystemExit if connection fails
    :return: connection object 'conn'
    """
    try:
        logging.debug("Connecting to MSSQL database...")
        conn = pymssql.connect(host, user, password, database)  # set connection
        logging.info("connection established.")
        return conn
    except pymssql.Error as error:
        logging.error("Error connecting to database: {}".format(error))
        raise SystemExit


def get_latest_file():
    """
    finds yesterday's log file
    :return: absolute pathname of yesterday's file
    """
    log_loc = r"\\uk-ftp\FTP Server Logs"
    yesterday = datetime.date.fromordinal(datetime.date.today().toordinal() - 1)
    file_name = "fzs-{}.log".format(yesterday)
    target_file = os.path.join(log_loc, file_name)
    logging.info("checking file {} for entries".format(target_file))
    return target_file


def read_file(log_file):
    """
    read log file entries to Python list
    :param log_file: log file to be read
    :return: list of useful entries
    """
    all_entries = []
    with open(log_file) as file_in:
        for line in file_in:
            if "(not logged in)" not in line:  # skip unwanted entries
                if line.startswith("("):
                    all_entries.append(line)
    # terminate if no useful entries found
    if not all_entries:
        logger.info("Found no entries to load. Terminating.")
        raise SystemExit
    return all_entries


def parse_data(entries):
    """
    parse each log entry into format readable by database
    uses regular expression to find fields
    :param entries: list of log entries
    :return: messages - a list of tuples
    """
    messages = []
    for entry in entries:
        try:
            match = regex.match(entry)
            date, log_time, user, ip = match.group(2, 3, 5, 6)
            uk_date = re.sub(date_regex, format_date_regex, date)
            msg_start = entry.rfind(">") + 2
            message = entry[msg_start:-1]
            data = uk_date, log_time, user, ip, message
            messages.append(data)
        except Exception as e:
            logger.error("Exception raised for entry: {} \n Exception: {}"
                         .format(entry, e))
    return messages


def format_date_regex(match):
    """
    helper function to format date into format
    readable by database
    :param match: regex match object
    :return: new date string
    """
    match1 = match.group(1)
    match2 = match.group(2)
    match3 = match.group(3)
    return "{}-{}-{}".format(match3, match1, match2)


def insert_entries_to_table(entries):
    """
    SQL insert statement to load entries to database
    :param entries:
    :return:
    """
    sql_insert = "INSERT INTO FTP_log_entries VALUES (%s, %s, %s, %s, %s)"
    conn = establish_database_connection()
    try:
        with conn:
            cursor = conn.cursor()
            cursor.executemany(sql_insert, entries)
            conn.commit()
            logger.info("Inserted {} entries to database."
                        .format(len(entries)))
    except Exception as e:
        logger.error("Problem encountered during insert to database: {}"
                     .format(e))
    finally:
        if conn:
            conn.close()


def main():
    log = get_latest_file()
    entries = read_file(log)
    parsed_data = parse_data(entries)
    insert_entries_to_table(parsed_data)


if __name__ == "__main__":
    main()
