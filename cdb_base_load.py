#!/usr/bin/env python3
import mysql.connector
import analysis_config



def get_base_load_config(deviceid):
    cnx = mysql.connector.connect(user=analysis_config.config_db_user, password=analysis_config.config_db_password,
                              host=analysis_config.main_config_db,
                              database=analysis_config.CONFIG_DB)
    cursor = cnx.cursor()
    query = ("SELECT device_id, facility_id, base_load_pre_filter, base_load_precision FROM ehubs "
         "WHERE device_id = %s")
    cursor.execute(query, (deviceid,))
    res = cursor.fetchall()
    cursor.close()
    cnx.close()
    if len(res)==0:
        res=[(deviceid, 0, 2, 2)]
    return res[0]

"78:a5:04:ff:40:bb"
if __name__ == "__main__":
    # execute only if run as a script
    print(get_base_load_config("78:a5:04:ff:40:bb"))
   