
import csv
from flask import Flask, request, jsonify
from DB_Connect import Sql_conn

app= Flask(__name__)
@app.route('/db_create',methods=['POST'])
def db_connect():
    if(request.method=="POST"):
        db1=request.json['db_name']
        table_name=request.json['table_name']
        col1= request.json['col1']
        col1type= request.json['col1type']
        col2 = request.json['col2']
        col2type = request.json['col2type']
        col3 = request.json['col3']
        col3type = request.json['col3type']
        if(db1=='sql'):
            dd = Sql_conn()
            cursor = dd.cursor()
            query = "CREATE TABLE {} ({} {},{} {},{} {});".format(table_name,col1,col1type,col2,col2type,col3,col3type)
            cursor.execute(query)
            print("Table Created!!")
            return jsonify('sql connection established'+str(dd))

@app.route('/db_insert', methods=['POST'])
def db_insert():
    if(request.method=="POST"):
        db1=request.json['db_name']
        table_name=request.json['table_name']
        emp_id=int(request.json['emp_id'])
        emp_name=request.json['emp_name']
        emp_job=request.json['emp_job']
        if db1=="sql":
            dd = Sql_conn()
            cursor = dd.cursor()
            sq = "INSERT INTO {} (emp_id,emp_name,emp_job) VALUES (%s,%s,%s);".format(table_name)
            args= (emp_id, emp_name, emp_job)
            cursor.execute(sq,args)
            dd.commit()
            print("Values inserted into the table!!")
            return jsonify(str(dd))

@app.route('/db_bulkInsert', methods=['POST'])
def db_bulkInsert():
    if(request.method=="POST"):
        db1=request.json['db_name']
        table_name=request.json['table_name']
        if db1=="sql":
            dd=Sql_conn()
            with open('D:\emp_data.csv', 'r') as test:
                next(test)
                ls = []
                data_hr = csv.reader(test, delimiter=',')
                ls=[]
                for i in data_hr:
                    val = (i[0],i[1],i[2])
                    ls.append(val)
            cursor=dd.cursor()
            sq = "INSERT INTO {} VALUES (%s,%s,%s)".format(table_name)
            cursor.executemany(sq, ls)
            dd.commit()
            print("Bulk Data Insertion Completed")
            return jsonify("Bulk Data Insertion Completed in MYSQL")




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    app.run()


