#!/usr/bin/env python3
import mysql.connector

cnx = mysql.connector.connect(user='mysqluser', password='hemligt',
                              host='127.0.0.1',
                              database='ferroamps')
cursor = cnx.cursor()

query = ("SELECT device_id, facility_id, base_load_pre_filter, base_load_precision FROM ehubs "
         "WHERE device_id = %s")

cursor.execute(query, ("78:a5:04:ff:40:bb",))

for (device_id, facility_id, base_load_pre_filter, base_load_precision) in cursor:
  print("{}, has preFilt={} and precision={}".format(
    device_id, base_load_pre_filter, base_load_precision))

cursor.close()
cnx.close()