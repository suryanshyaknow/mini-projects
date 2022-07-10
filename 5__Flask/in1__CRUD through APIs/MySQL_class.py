from flask import Flask, request, jsonify
from flask.views import MethodView
from mysql.connector import *
import os, csv
import logging as lg



class CrudSQL(MethodView):  # a class for MySQL CRUD operations.

    def set_con(self):
        if request.method == 'POST':
            try:
                global mydb, cur, db_name
                host = request.json['Host']
                user = request.json['User Name']
                passwd = request.json['Password']
                mydb = connect(host=host, user=user, passwd=passwd, use_pure="True")  # connection object
                lg.info(mydb)
                cur = mydb.cursor()

            except Exception as e:
                lg.error(e)
                return jsonify("Error:" + str(e))

            else:
                lg.info("So from now on, you'll be interacting with the MySQL database!")
                return jsonify("So from now on, you'll be interacting with the MySQL database!")

    def use_db(self):
        if request.method == 'POST':
            try:
                global mydb, cur, db_name
                db_name = request.json["Database Name"]  # name of the database to be created.
                cur.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
                cur.execute(f"USE {db_name};")

            except Exception as e:
                lg.error(e)
                return jsonify("Error: " + str(e))

            else:
                lg.info(f"You are now using the database {db_name}!")
                return jsonify(f"You are now using the database {db_name}!")

    def create_table(self):
        if request.method == "POST":
            try:
                global table_name
                table_name = request.json["Table Name"]
                cols = request.json["Columns"]
                cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols});")

            except Exception as e:
                lg.info(e)
                return jsonify("Error: " + str(e))

            else:
                lg.info(f"Your table {table_name} has been created!")
                return jsonify(f"Your table {table_name} has been created!")

    def insert_row(self):
        if request.method == "POST":
            try:
                row = request.json["Row"]
                cur.execute(f"INSERT INTO {table_name} VALUES ({row});")
                mydb.commit()

            except Exception as e:
                lg.info(e)
                return jsonify("Error: " + str(e))

            else:
                lg.info("Desired record is inserted!")
                return jsonify("Desired record is inserted!")

    def update_table(self):
        if request.method == "POST":
            try:
                table_name = request.json["Table Name"]
                updation = request.json["Updation"]
                cur.execute(f"UPDATE {table_name} SET {updation};")
                mydb.commit()
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
                dataset = request.json["Dataset for Bulk Insertion"]
                curDir = os.getcwd()
                os.chdir(path)
                for i in os.listdir():
                    if i.endswith(".csv"):
                        dataset = i  # as of now, we are considering the first .csv file in the dir for the bulk insertion.
                        break

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
                    global dataset, table_name
                    table_name = dataset[:-4].replace(" ", "_")
                    try:
                        global cur, mydb, db_name
                        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
                        cur.execute(f"CREATE TABLE {table_name} ({__fetch_col()});")
                        lg.info(f"The table {table_name} has been created for the bulk insertion!")
                    except Exception as e:
                        lg.error(e)

                def insert_rows():
                    global dataset, cur, mydb, db_name, table_name
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
                                cur.execute(f"INSERT INTO {table_name} VALUES ({x})")
                                lg.info(f"Bulk Insertion: Record {r_i} inserted!")
                                r_i += 1
                    except Exception as e:
                        mydb.commit()
                        lg.error(e)
                    else:
                        mydb.commit()
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
                global cur, mydb
                n = request.json["How many records?"]
                table_name = request.json["Table Name"]

                if n == "All":
                    cur.execute(f"DELETE FROM {table_name};")

                elif n == "Some":
                    cond = request.json["Delete records from where? "]  # Rows to be deleted specified by condition.
                    cur.execute(f"DELETE FROM {table_name} WHERE {cond}")

                else:
                    return jsonify("The input for how many records to be deleted ain't appropriate. Kindly edit!")

            except Exception as e:
                lg.error(e)
                return jsonify(str(e))
            else:
                mydb.commit()
                lg.info("Desired record(s) has(have) been deleted.")
                return jsonify("Desired record(s) has(have) been deleted.")

    def download(self):
        if request.method == 'POST':
            try:
                import pandas as pd
                global mydb
                curDir = os.getcwd()  # current directory
                table_name = request.json["Table Name"]

                df = pd.read_sql(f"SELECT * FROM {table_name};", mydb)

                download = request.json["Download Path"]  # location where file is to be downloaded.
                os.chdir(f"{download}")
                file_name = f"{table_name}.csv"
                if os.path.exists(f"{download}//{table_name}.csv"):
                    for i in range(1,
                                   100):  # Assuming no one would exceed downloading the same file for more than 100 times.
                        if not os.path.exists(f"{download}//{table_name} ({i}).csv"):
                            file_name = f"{table_name} ({i}).csv"
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