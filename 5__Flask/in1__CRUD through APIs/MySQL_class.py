from flask import Flask, request, jsonify
from flask.views import MethodView
from mysql.connector import *
import os, csv
import logging as lg


class CrudSQL(MethodView):  # a class for MySQL CRUD operations.

    def __int__(self):

        self.db_con = None
        self.mydb = None
        self.cur = None
        self.table_name = None

    def set_con(self):
        if request.method == 'POST':
            try:
                host = request.json['Host']
                user = request.json['User Name']
                passwd = request.json['Password']
                self.db_con = connect(host=host, user=user, passwd=passwd, use_pure="True")  # connection object
                lg.info( self.db_con)
                self.cur = self.db_con.cursor()

            except Exception as e:
                lg.error(e)
                return jsonify("Error:" + str(e))

            else:
                lg.info("So from now on, you'll be interacting with the MySQL database!")
                return jsonify("So from now on, you'll be interacting with the MySQL database!")

    def use_db(self):
        if request.method == 'POST':
            try:
                self.mydb = request.json["Database Name"]  # name of the database to be created.
                self.cur.execute(f"CREATE DATABASE IF NOT EXISTS {self.mydb};")
                self.cur.execute(f"USE {self.mydb};")

            except Exception as e:
                lg.error(e)
                return jsonify("Error: " + str(e))

            else:
                lg.info(f"You are now using the database {self.mydb}!")
                return jsonify(f"You are now using the database {self.mydb}!")

    def create_table(self):
        if request.method == "POST":
            try:
                self.table_name = request.json["Table Name"]
                cols = request.json["Columns"]
                self.cur.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} ({cols});")

            except Exception as e:
                lg.info(e)
                return jsonify("Error: " + str(e))

            else:
                lg.info(f"Your table {self.table_name} has been created!")
                return jsonify(f"Your table {self.table_name} has been created!")

    def insert_row(self):
        if request.method == "POST":
            try:
                row = request.json["Row"]
                self.cur.execute(f"INSERT INTO {self.table_name} VALUES ({row});")
                self.db_con.commit()

            except Exception as e:
                lg.info(e)
                return jsonify("Error: " + str(e))

            else:
                lg.info("Desired record is inserted!")
                return jsonify("Desired record is inserted!")

    def update_table(self):
        if request.method == "POST":
            try:
                self.table_name = request.json["Table Name"]
                updation = request.json["Updation"]
                self.cur.execute(f"UPDATE {self.table_name} SET {updation};")
                self.db_con.commit()
            except Exception as e:
                lg.error(e)
                return jsonify("Error: " + str(e))
            else:
                lg.info("Your desired record(s) has/have been updated!")
                return jsonify("Your desired record(s) has/have been updated!")

    def bulk_insert(self):
        if request.method == "POST":
            try:
                global dataset
                path = request.json["Dataset Path"]
                dataset = request.json["Dataset to be Bulk Inserted"]
                curDir = os.getcwd()
                os.chdir(path)

                if not dataset in os.listdir():
                    lg.error("No such dataset exists in the given directory!")
                    return jsonify("No such dataset exists in the given directory!")


                def __fetch_col():  # a protected function
                    """
                    A functionality for fetching the first row of the desired database that we know by default is
                    columns names.
                    :return: columns names for the dataset desired for bulk insertion.
                    """
                    try:
                        global dataset
                        with open(dataset, "r") as data:
                            data = csv.reader(data)
                            col = ['']  # a list for having its index zero for holding columns names.
                            for row in data:
                                for i in row:
                                    col[0] += f"{i} varchar(100), "
                                return col[0][:-2]  # just to avoid an additional ", " at the last index.
                    except Exception as e:
                        lg.error(e)

                def create_table():
                    """
                    A functionality for creating a table using the fetched columns from fetch_col()
                    as its column names.
                    :return: None
                    """
                    global dataset
                    self.table_name = dataset[:-4].replace(" ", "_")
                    try:
                        self.cur.execute(f"DROP TABLE IF EXISTS {self.table_name};")
                        self.cur.execute(f"CREATE TABLE {self.table_name} ({__fetch_col()});")
                        lg.info(f"The table {self.table_name} has been created for the bulk insertion!")
                    except Exception as e:
                        lg.error(e)

                def insert_rows():
                    try:
                        with open(dataset, "r") as data:
                            data = csv.reader(data, delimiter=',')
                            next(data)
                            r_i = 0  # record index

                            for row in data:
                                insRow = ['']  # zeroth index for holding records in insertable form.
                                for i in row:
                                    insRow[0] += f"'{i}', "

                                x = insRow[0][:-2]
                                self.cur.execute(f"INSERT INTO {self.table_name} VALUES ({x})")
                                lg.info(f"Bulk Insertion: Record {r_i} inserted!")
                                r_i += 1
                    except Exception as e:
                        self.db_con.commit()
                        lg.error(e)
                    else:
                        self.db_con.commit()
                        lg.info("All records inserted!")

                create_table()
                insert_rows()

            except Exception as e:
                lg.error(e)
                return jsonify(str(e))

            else:
                os.chdir(curDir)  # switching back to the current directory.
                lg.info("BULK INSERTION COMPLETED!")
                return jsonify("BULK INSERTION COMPLETED!")

    def delete(self):
        if request.method == "POST":
            try:
                n = request.json["How many records?"]
                self.table_name = request.json["Table Name"]

                if n == "All":
                    self.cur.execute(f"DELETE FROM {self.table_name};")

                elif n == "Some":
                    cond = request.json["Delete records from where? "]  # Rows to be deleted specified by condition.
                    self.cur.execute(f"DELETE FROM {self.table_name} WHERE {cond}")

                else:
                    return jsonify("The input for how many records to be deleted ain't appropriate. Kindly edit!")

            except Exception as e:
                lg.error(e)
                return jsonify(str(e))
            else:
                self.db_con.commit()
                lg.info("Desired record(s) has(have) been deleted.")
                return jsonify("Desired record(s) has(have) been deleted.")

    def download(self):
        if request.method == 'POST':
            try:
                import pandas as pd
                curDir = os.getcwd()  # current directory
                self.table_name = request.json["Table Name"]

                df = pd.read_sql(f"SELECT * FROM {self.table_name};",  self.db_con)

                download_path = r'C:\Users\Suryansh Grover\Downloads'  # location where file is to be downloaded.
                os.chdir(f"{download_path}")
                file_name = f"{self.table_name}.csv"

                # Naming convention if the same file is to be downloaded multiple times
                if os.path.exists(f"{download_path}//{self.table_name}.csv"):
                    # Assuming no one would exceed downloading the same file for more than 100 times.
                    for i in range(1, 100):
                        if not os.path.exists(f"{download_path}//{self.table_name} ({i}).csv"):
                            file_name = f"{self.table_name} ({i}).csv"
                            break
                    df.to_csv(file_name, index=False)

                else:
                    df.to_csv(file_name, index=False)
            except Exception as e:
                lg.error(e)
                return jsonify(str(e))
            else:
                os.chdir(curDir)  # switching back to the original directory.
                lg.info(f"{file_name} downloaded!")
                return jsonify(f"{file_name} downloaded!")
