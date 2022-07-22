from flask import Flask, request, jsonify
from flask.views import MethodView
import pymongo
import os, csv
import json
import logging as lg


class CrudMongo(MethodView):

    def __init__(self):

        self.client = None
        self.mydb = None

    def set_con_(self):
        """
        Establishing a connection with MongoDB Atlas.
        :return:
        """
        if request.method == 'POST':
            try:
                user_name = request.json["User Name"]
                password = request.json["Password"]

                self.client = pymongo.MongoClient(
                    f"mongodb+srv://{user_name}:{password}@cluster0.zvp3o.mongodb.net/?retryWrites=true&w=majority")
                test_con = self.client.test  # test connection
                lg.info(test_con)

                if test_con is not None:
                    lg.info(f"Connection with MongoDB is successful!")
                    return jsonify("Connection with MongoDB is successful!")

            except Exception as e:
                lg.error(e)
                return jsonify(str(e))

    def use_db_(self):
        if request.method == 'POST':
            try:
                db_name = request.json["Database Name"]
                self.mydb = self.client[db_name]

            except Exception as e:
                lg.info(e)
                return jsonify(str(e))

            else:
                lg.info(f"You are now using the database '{db_name}'.")
                return f"You are now using the database '{db_name}'."

    def create_collec(self):
        if request.method == 'POST':
            try:
                collec_name = request.json["Collection Name"]

                if collec_name in self.mydb.list_collection_names():
                    collec = self.mydb[collec_name]  # using the already existing collection
                    return jsonify(f"Collection '{collec_name}' already exists! Kindly choose any other name or "
                                   f"just ignore to continue making changes to this already existing collection.")

                collec = self.mydb[collec_name]  # creating the collection

            except Exception as e:
                lg.info(e)
                return jsonify(str(e))

            else:
                lg.info(f"Collection '{collec_name}' has been created!")
                return f"Collection '{collec_name}' has been created!"

    def insert_rec(self):
        if request.method == "POST":
            try:
                collec_name = request.json["Collection Name"]
                collec = self.mydb[collec_name]

                rec = request.json['Record'].replace("'", '"')  # replacing single quotes w doubles

                # Converting the string 'rec' into 'record' having json format,
                # so that it can be inserted into the collection.
                record = json.loads(rec)

                collec.insert_one(record)

            except Exception as e:
                lg.error(e)
                return jsonify(str(e))
            else:
                lg.info('Desired record has been inserted!')
                return jsonify('Desired record has been inserted!')

    def update_collec(self):
        if request.method == 'POST':
            try:
                collec_name = request.json["Collection Name"]
                collec = self.mydb[collec_name]  # collection in which updation is to be made.
                n = request.json["How many records?"].lower()

                where = request.json["Where?"].replace("'", '"')
                where = json.loads(where)
                print(where, type(where))

                updation = request.json["Updation"].replace("'", '"')
                updation = json.loads(updation)
                print(updation, type(updation))

                if n == 'all':
                    for i in collec.find():
                        collec.find_one_and_update({where, {'$set': updation}})

                elif n == 'one' or n == 'single':
                    collec.find_one_and_update({where, {'$set': updation}})

                else:
                    lg.info("Kindly specify how many records you wish to update properly!")
                    return jsonify("Kindly specify how many records you wish to update properly!")

            except Exception as e:
                lg.error(e)
                return jsonify(str(e))

            else:
                lg.info("Your desired updation has been made!")
                return jsonify("Your desired updation has been made!")

    def bulk_insert_(self):
        if request.method == 'POST':
            try:
                collec_name = request.json["Collection Name"]
                collec = self.mydb[collec_name]
                curDir = os.getcwd()
                path = request.json["Dataset Path"]
                os.chdir(path)
                dataset = request.json["Dataset to be Bulk Inserted"]

                if not dataset in os.listdir():
                    lg.error("No such dataset exists in the given directory")
                    return jsonify("No such dataset exists in the given directory")

                with open(dataset, "r") as f:
                    data = csv.DictReader(f)
                    for row in data:
                        collec.insert_one(row)
                    os.chdir(curDir)  # getting back to our original directory
                    lg.info("BULK INSERTION COMPLETE!")
                    return jsonify("BULK INSERTION COMPLETE!")

            except Exception as e:
                lg.error(e)
                return jsonify(str(e))

    def download_(self):
        if request.method == "POST":
            try:
                collec_name = request.json["Collection Name"]
                collec = self.mydb[collec_name]
                curDir = os.getcwd()
                download_path = r'C:\Users\Suryansh Grover\Downloads'  # location where file is to be downloaded.
                os.chdir(download_path)

                os.chdir(download_path)
                file_name = collec_name + ".json"  # name of the file that is to be downloaded.
                data = []  # will hold records
                for i in collec.find():
                    data.append(str(i))

                # Naming convention if the same file is to be downloaded multiple times
                if os.path.exists(f"{download_path}//{file_name}"):
                    # Assuming no one would exceed downloading the same file for more than 100 times.
                    for i in range(1, 100):
                        if not os.path.exists(f"{download_path}//{file_name[:-5]} ({i}).json"):
                            file_name = f"{file_name[:-5]} ({i}).json"
                            break

                with open(file_name, "w") as f:
                    json.dump(data, f)  # dumping the json data

                lg.info(f"{file_name} has been downloaded!")
                return jsonify(f"{file_name} has been downloaded!")

            except Exception as e:
                lg.error(e)
                return jsonify(str(e))

    def delete_(self):
        if request.method == "POST":
            try:
                collec_name = request.json["Collection Name"]
                collec = self.mydb[collec_name]

                n = request.json["How many records?"].lower()

                if n == "one" or n == "single":
                    where = request.json["Where?"].replace("'", '"')
                    where = json.loads(where)
                    collec.delete_one(where)

                elif n == "all":
                    where = request.json["Where?"].replace("'", '"')
                    where = json.loads(where)
                    collec.delete_many(where)

                else:
                    lg.warning("Kindly mention how many records are to be deleted in an appropriate way!")
                    return jsonify("Kindly mention how many records are to be deleted in an appropriate way!")

                lg.info("The desired record(s) has(have) been deleted!")
                return jsonify("The desired record(s) has(have) been deleted!")

            except Exception as e:
                lg.error(e)
                return jsonify(str(e))






