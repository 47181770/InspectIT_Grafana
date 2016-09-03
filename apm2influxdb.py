# -*-coding:utf-8-*-
import json
import requests
import time
import datetime
import pandas as pd
from pandas import Series
from pandas.io.json import json_normalize
from influxdb import DataFrameClient

# Your CMR server configuration
host_cmr = '10.0.0.2'
host_cmr_port = 8182

# Your InfluxDB database server configuration
host = '10.0.0.1'
port = 8086
user = 'root'
password = 'root'
dbname = 'mydb'

client = DataFrameClient(host, port, user, password, dbname)

# Get the Last Read Id from InfluxDB
# Use the last read id to compare with CMR Web service data

MaxIdInDB = Series([1])
print(type(MaxIdInDB))
print('MaxIdInDB is ', MaxIdInDB)
print('MaxIdInDB [0] is ', MaxIdInDB[0])

MaxIdInDB = pd.Series(client.query('select max(id) from Z').get('Z'))

if MaxIdInDB.empty:
    MaxIdInDB = Series([1])
    print(type(MaxIdInDB))
    time.sleep(3)
    print('The initial value of Max ID in database is ', MaxIdInDB[0])


def _get_last_read_id():
    """
    To get the last read id from InfluxDB

    """
    webservice_url_initial = 'http://' + host_cmr + ':' + str(host_cmr_port) + '/rest/data/invocations/overview?latestReadId=' \
                             + str(MaxIdInDB[0])

    print('Web Service Url Initial for Last Read id is ', webservice_url_initial)
    response_summary = requests.get(webservice_url_initial)

    data = response_summary.json()
    df = pd.DataFrame(json_normalize(data))
    lastreadid_max = df[['id']].max()
    lastreadid_min = df[['id']].min()
    print('Last Read id VALUE in apm is ', lastreadid_max['id'])
    print('the min id VALUE in apm this json  ', lastreadid_min['id'])

    if int(lastreadid_max) >= MaxIdInDB[0]:
        print("Send data to influx and MaxIDINDB[0] is from ", MaxIdInDB[0], ' to LastReadId:', int(lastreadid_max))
        a = lastreadid_max['id']
        print('a is ', a)
        return a
        time.sleep(1)


def apm2influx(lastid):
    """
    :param lastid: is from the function of _get_last_read_id
    "/rest/data/invocations/overview?latestReadId" is the web service of InspectIT

    """
    webservice_url = 'http://' + host_cmr + ':' + str(host_cmr_port) + '/rest/data/invocations/overview?latestReadId=' + str(lastid)
    print(webservice_url)
    request_stage = requests.get(webservice_url)
    request_stage_json = request_stage.json()
    apm_stage_data = pd.DataFrame(json_normalize(request_stage_json))

    apm_stage_data['time'] = map(lambda x: [pd.to_datetime(x / 1000, unit='s')], apm_stage_data['timeStamp'])
    print('apm stage data including time column ', type(apm_stage_data))
    apm_stage_data.set_index(['time'])
    z = apm_stage_data[['time', 'timeStamp', 'duration', 'id']]

    client.write_points(z, 'Z')


if __name__ == '__main__':

    while True:
        LastReadId = _get_last_read_id()
        print("The Running Read Id + 100 is ", LastReadId, "...Begin...")
        apm2influx(LastReadId)
        time.sleep(10)
        print("The Running Read Id + 100 is ", LastReadId, "...End!!!")
