from flask import Flask, request, jsonify

from Log_class import Log
from MySQL_class import CrudSQL

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

if __name__ == '__main__':
    app.run(debug=True)
