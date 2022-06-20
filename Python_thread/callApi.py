# Databricks notebook source
# Databricks notebook source
import http
import json


def call_rest_api(table_name, url):

    status = ''
    execution_log = ''
    try:
        conn = http.client.HTTPSConnection(url)
        payload = ''
        headers = {
            'Cookie': '__cfduid=d2e270bea97599e2fbde210bf483fcd491615195032'
        }
        for val in range(2):
            conn.request("GET", "/random/postcodes", payload, headers)
            execution_log += 'connection is done'
            res = conn.getresponse()
            data = res.read().decode("utf-8")
            jsondata = json.loads(json.dumps(data))
            execution_log += 'JSON payload is done'
            df = spark.read.json(sc.parallelize([jsondata]))
            if val == 0:
                df_temp = df.selectExpr("string(status) as status", "result['country'] as country", "result['european_electoral_region'] as european_electoral_region", "string(result['latitude']) as latitude",
                                    "string(result['longitude']) as longitude", "result['parliamentary_constituency'] as parliamentary_constituency", "result['region'] as region", "'' as vld_status", "'' as vld_status_reason")
                df_union = df_temp
            else:
                df_union = df_union.union(df_temp)
                df_union.write.format("delta").mode("append").saveAsTable(f"{table_name}")
        # your work
        status = 'success'
        execution_log = f"call_publicapi - success - created successfully"
    except Exception as execution_error:
        status = 'failed'
        execution_log = f"call_publicapi - failed - with error {str(execution_error)}"
    return status, execution_log


# COMMAND ----------

url ="https://vbuttons.net/stagingapi/listing.php?authToken=AMERFST7543567OP&mlsSid=238&searchBy=office&searchStr=All&fromDate=12-23-2020&status=All&recLimit=100"
call_rest_api('test_public_api', url)
