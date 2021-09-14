#!/usr/bin/python3

import pymysql
from datetime import date
import pandas as pd

# Drop table before starting scan if set up true.
# START_WITH_NEW_TABLE = TRUE
START_WITH_NEW_TABLE = False

SKU_START_WITH = "X00"
BAD_ITEM_MARKER = "BAD"
END_SIGNAL = "QUIT"
MULTI_COUNT = "*"
RESULT_FILE_PATH = "results.csv"

# mysql database name
DATABASE_NAME = "vinmaxstock"
# use your own user name and password for mysql
USER_NAME = ""
PASS_WORD = ""

class PackageItem:

    def __init__(self, track, rd):
        self.tracking = track
        self.items = {}
        self.received_date = rd
        self.current = ""
        self.count = 1

    def __str__(self):
        return self.received_date + " : " + self.tracking + "\n" + self.items.__str__()

    def bad_item(self):
        if self.current in self.items.keys() and self.items[self.current] >= 1:
            self.items[self.current] -= self.count
            if self.items[self.current] == 0:
                self.items.pop(self.current)
            self.items[self.current + "_" + BAD_ITEM_MARKER] = self.count

    def add_new_items(self, sku):
        self.count = 1
        self.current = sku
        if sku in self.items.keys():
            self.items[sku] += 1
        else:
            self.items[sku] = 1

    def multi_last_item(self, count):
        try:
            count = int(count) - 1
            if self.current in self.items.keys():
                self.items[self.current] += count
            self.count = count + 1
        except ValueError:
            print("[X]Failed convert input to a integer.")

    def finish_this_package(self, db):
        cursor = db.cursor()
        for key, value in self.items.items():
            cond = True
            if BAD_ITEM_MARKER in key:
                cond = False
            sql = """INSERT INTO RETURNS(inDate,scanDate,packageTracking,sku,count,cond)
                     VALUES ('%s', '%s', '%s', '%s', %s, %s)""" % \
                (self.received_date, date.today(), self.tracking, key, value, cond)
            try:
                cursor.execute(sql)
                db.commit()
            except:
                db.rollback()


def write_to_file(db):
    frame = pd.read_sql("select * from RETURNS", db)
    df = frame.groupby(["packageTracking", "sku", "count", "cond", "inDate", "scanDate"])\
        .sum().reset_index()
    df['cond'] = df['cond'].apply(str).str.replace("1", " ").replace("0", "BAD")
    df.to_csv(RESULT_FILE_PATH)


def connect_to_db():
    db = pymysql.connect(host="localhost",
                         user=USER_NAME,
                         password=PASS_WORD,
                         database= DATABASE_NAME)
    cursor = db.cursor()
    if START_WITH_NEW_TABLE:
        cursor.execute("DROP TABLE IF EXISTS RETURNS")
        cursor.execute(
            """CREATE TABLE RETURNS (
             inDate date,
             scanDate date,
             packageTracking  varchar(50),
             sku varchar(50),
             count INT,
             cond bool);""")
    return db


def main():
    db = connect_to_db()
    received_date = input("Please Input Date to Receive:  ")
    current_package = PackageItem("placeholder", received_date)
    while(True):
        tracking = input().upper()
        if tracking.startswith(SKU_START_WITH):
            current_package.add_new_items(tracking)
        elif tracking.upper().startswith(BAD_ITEM_MARKER):
            current_package.bad_item()
        elif tracking.upper().startswith(MULTI_COUNT):
            current_package.multi_last_item(tracking[1:])
        else:
            print(current_package)
            current_package.finish_this_package(db)
            if tracking.upper() == END_SIGNAL:
                break
            current_package = PackageItem(tracking, received_date)
    write_to_file(db)
    db.close()


if __name__ == "__main__":
    main()
