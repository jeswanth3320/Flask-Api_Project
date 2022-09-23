import csv

import mysql.connector as connection

with open('D:\emp_data.csv') as test:

    next(test)
    ls=[]
    data_hr = csv.reader(test, delimiter=',')
    for i in data_hr:
        val=(i[0],i[1],i[2])
        print(val)
        ls.append(val)
    print(ls)



def Sql_conn():
    mydb=connection.connect(host="localhost", database='jeswanth123', user="root", passwd="Jeswanth@4248", use_pure=True)
    return mydb