from flask import Flask, request, jsonify

from Log_class import Log
from MySQL_class import CrudSQL
from MongoDB_class import CrudMongo

app = Flask(__name__)

Log()
crudSQL = CrudSQL()  # creating an object of the CrudMySQL class.

# creating MySQL APIs down below:
app.add_url_rule('/MySQL', view_func=crudSQL.set_con, methods=['POST'])
app.add_url_rule('/MySQL/use_database', view_func=crudSQL.use_db, methods=['POST'])
app.add_url_rule('/MySQL/use_database/create_table', view_func=crudSQL.create_table, methods=['POST'])
app.add_url_rule('/MySQL/use_database/create_table/insert_record', view_func=crudSQL.insert_row, methods=['POST'])
app.add_url_rule('/MySQL/use_database/update_table', view_func=crudSQL.update_table, methods=['POST'])
app.add_url_rule('/MySQL/use_database/bulk_insert', view_func=crudSQL.bulk_insert, methods=['POST'])
app.add_url_rule('/MySQL/use_database/delete_records', view_func=crudSQL.delete, methods=['POST'])
app.add_url_rule('/MySQL/use_database/download', view_func=crudSQL.download, methods=['POST'])


crudMongo = CrudMongo()

# creating MongoDB APIs down below:
app.add_url_rule('/mongodb', view_func=crudMongo.set_con_, methods=['POST'])
app.add_url_rule('/mongodb/use_db', view_func=crudMongo.use_db_, methods=['POST'])
app.add_url_rule('/mongodb/use_db/create_collection', view_func=crudMongo.create_collec, methods=['POST'])
app.add_url_rule('/mongodb/use_db/insert_record', view_func=crudMongo.insert_rec, methods=['POST'])
app.add_url_rule('/mongodb/use_db/update', view_func=crudMongo.update_collec, methods=['POST'])
app.add_url_rule('/mongodb/use_db/bulk_insert', view_func=crudMongo.bulk_insert_, methods=['POST'])
app.add_url_rule('/mongodb/use_db/download', view_func=crudMongo.download_, methods=['POST'])
app.add_url_rule('/mongodb/use_db/delete', view_func=crudMongo.delete_, methods=['POST'])



if __name__ == '__main__':
    app.run(debug=True)
