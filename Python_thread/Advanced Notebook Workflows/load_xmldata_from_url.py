# Databricks notebook source
import csv
import requests
import pandas as pd
import xml.etree.ElementTree as et
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from datetime import datetime, timedelta
from multiprocessing.pool import ThreadPool
import traceback
import os

# COMMAND ----------

# DBTITLE 1,Dictionnary of schemas of each endpoint expected XML structure
endpoint_config_schema = {
  "agent-move-1.0":StructType([
     StructField("mlsSid", StringType(), True),
     StructField("moveDate", StringType(), True),
     StructField("agentId", StringType(), True),
     StructField("firstName", StringType(), True),
     StructField("lastName", StringType(), True),
     StructField("currOfficeId", StringType(), True),
     StructField("currOffice", StringType(), True),
     StructField("currOfficeAddress", StringType(), True),
     StructField("currCity", StringType(), True),
     StructField("currState", StringType(), True),
     StructField("currZip", StringType(), True),
     StructField("formerOfficeId", StringType(), True),
     StructField("formerOffice", StringType(), True),
     StructField("formerOfficeAddress", StringType(), True),
     StructField("formerCity", StringType(), True),
     StructField("formerState", StringType(), True),
     StructField("formerZip", StringType(), True)
   ]),
  "agent-new-1.0":StructType([
     StructField("mlsSid", StringType(), True),
     StructField("agentId", StringType(), True),
     StructField("firstName", StringType(), True),
     StructField("lastName", StringType(), True),
     StructField("agentEmail", StringType(), True),
     StructField("agentPhone1", StringType(), True),
     StructField("dateFirstAppeared", StringType(), True),
     StructField("officeId", StringType(), True),
     StructField("officeName", StringType(), True),
     StructField("officeAddress", StringType(), True),
     StructField("officeCity", StringType(), True),
     StructField("officeState", StringType(), True),
     StructField("officeZip", StringType(), True)
   ]),
  "agent-2.0":StructType([
     StructField("mlsSid", StringType(), True),
     StructField("listAgentId", StringType(), True),
     StructField("listAgentFirstName", StringType(), True),
     StructField("listAgentLastName", StringType(), True),
     StructField("listAgentPhone1", StringType(), True)
   ]),
  "agents-all-1.0":StructType([
     StructField("mlsSid", StringType(), True),
     StructField("listAgentId", StringType(), True),
     StructField("listAgentFirstName", StringType(), True),
     StructField("listAgentLastName", StringType(), True),
     StructField("listAgentPhone1", StringType(), True)
   ]),
  "closed-listings-1.0":StructType([
     StructField("mlsSid", StringType(), True),
     StructField("listAgentId", StringType(), True),
     StructField("listAgentFirstName", StringType(), True),
     StructField("listAgentLastName", StringType(), True),
     StructField("listAgentPhone1", StringType(), True)
   ]),
  "listings-2.0":StructType([
     StructField("mlsSid", StringType(), True),
     StructField("listAgentId", StringType(), True),
     StructField("listAgentFirstName", StringType(), True),
     StructField("listAgentLastName", StringType(), True),
     StructField("listAgentPhone1", StringType(), True),
     StructField("coListAgentId", StringType(), True),
     StructField("coListAgentFirstName", StringType(), True),
     StructField("coListAgentLastName", StringType(), True),
     StructField("coListAgentPhone1", StringType(), True),
     StructField("sellAgentId", StringType(), True),
     StructField("sellAgentFirstName", StringType(), True),
     StructField("sellAgentLastName", StringType(), True),
     StructField("sellAgentPhone1", StringType(), True),
     StructField("coSellAgentId", StringType(), True),
     StructField("coSellAgentFirstName", StringType(), True),
     StructField("coSellAgentLastName", StringType(), True),
     StructField("coSellAgentPhone1", StringType(), True),
     StructField("dom", StringType(), True),
     StructField("city", StringType(), True),
     StructField("dateList", StringType(), True),
     StructField("listPrice", StringType(), True),
     StructField("state", StringType(), True),
     StructField("address", StringType(), True),
     StructField("bank", StringType(), True),
     StructField("statusCode", StringType(), True),
     StructField("dateStatusChange", StringType(), True),
     StructField("zipCode", StringType(), True),
     StructField("listingId", StringType(), True),
     StructField("officeId", StringType(), True),
     StructField("officeName", StringType(), True),
     StructField("streetName1", StringType(), True),
     StructField("officeCity", StringType(), True),
     StructField("officeZip", StringType(), True),
     StructField("officeState", StringType(), True)
   ]),
  "office-2.0":StructType([
     StructField("mlsSid", StringType(), True),
     StructField("officeId", StringType(), True),
     StructField("officeName", StringType(), True),
     StructField("agents", StringType(), True),
     StructField("officeAddress1", StringType(), True),
     StructField("officeAddress2", StringType(), True),
     StructField("officeCity", StringType(), True),
     StructField("officeState", StringType(), True),
     StructField("officeZip", StringType(), True),
     StructField("officeGeoCode", StringType(), True),
     StructField("officeCounty", StringType(), True),
     StructField("officeStatus", StringType(), True),
     StructField("officePhone", StringType(), True),
     StructField("officeFax", StringType(), True),
     StructField("officeEmail", StringType(), True)
   ]),
  "listings-3.0":StructType([
     StructField("mlsSid", StringType(), True),
     StructField("listAgentId", StringType(), True),
     StructField("listAgentFirstName", StringType(), True),
     StructField("listAgentLastName", StringType(), True),
     StructField("listAgentPhone1", StringType(), True),
     StructField("coListAgentId", StringType(), True),
     StructField("coListAgentFirstName", StringType(), True),
     StructField("coListAgentLastName", StringType(), True),
     StructField("coListAgentPhone1", StringType(), True),
     StructField("sellAgentId", StringType(), True),
     StructField("sellAgentFirstName", StringType(), True),
     StructField("sellAgentLastName", StringType(), True),
     StructField("sellAgentPhone1", StringType(), True),
     StructField("coSellAgentId", StringType(), True),
     StructField("coSellAgentFirstName", StringType(), True),
     StructField("coSellAgentLastName", StringType(), True),
     StructField("coSellAgentPhone1", StringType(), True),
     StructField("dom", StringType(), True),
     StructField("city", StringType(), True),
     StructField("dateList", StringType(), True),
     StructField("listPrice", StringType(), True),
     StructField("state", StringType(), True),
     StructField("address", StringType(), True),
     StructField("addressUnit", StringType(), True),
     StructField("bank", StringType(), True),
     StructField("statusCode", StringType(), True),
     StructField("dateStatusChange", StringType(), True),
     StructField("zipCode", StringType(), True),
     StructField("listOfficeId", StringType(), True),
     StructField("listOfficeName", StringType(), True),
     StructField("listOfficeStreetName1", StringType(), True),
     StructField("listOfficeCity", StringType(), True),
     StructField("listOfficeZip", StringType(), True),
     StructField("listOfficeState", StringType(), True),
     StructField("sellOfficeId", StringType(), True),
     StructField("sellOfficeName", StringType(), True),
     StructField("sellOfficeStreetName1", StringType(), True),
     StructField("sellOfficeCity", StringType(), True),
     StructField("sellOfficeZip", StringType(), True),
     StructField("sellOfficeState", StringType(), True)
   ])
}

# COMMAND ----------

# DBTITLE 1,Dictionnary of Columns for each endpoint 
## StructField(<>, StringType(), True),
endpoint_config_cols = {
   "agent-move-1.0":[
      "mlsSid",
      "moveDate",
      "agentId",
      "firstName",
      "lastName",
      "currOfficeId",
      "currOffice",
      "currOfficeAddress",
      "currCity",
      "currState",
      "currZip",
      "formerOfficeId",
      "formerOffice",
      "formerOfficeAddress",
      "formerCity",
      "formerState",
      "formerZip"
   ],
   "agent-new-1.0":[
      "mlsSid",
      "agentId",
      "firstName",
      "lastName",
      "agentEmail",
      "agentPhone1",
      "dateFirstAppeared",
      "officeId",
      "officeName",
      "officeAddress",
      "officeCity",
      "officeState",
      "officeZip"
   ],
   "agent-2.0":[
      "mlsSid",
      "listAgentId",
      "listAgentFirstName",
      "listAgentLastName",
      "listAgentPhone1"
   ],
   "agents-all-1.0":[
      "mlsSid",
      "listAgentId",
      "listAgentFirstName",
      "listAgentLastName",
      "listAgentPhone1"
   ],
   "closed-listings-1.0":[
      "mlsSid",
      "listAgentId",
      "listAgentFirstName",
      "listAgentLastName",
      "listAgentPhone1"
   ],
   "listings-2.0":[
     
      "mlsSid",
      "listAgentId",
      "listAgentFirstName",
      "listAgentLastName",
      "listAgentPhone1",
      "coListAgentId",
      "coListAgentFirstName",
      "coListAgentLastName",
      "coListAgentPhone1",
      "sellAgentId",
      "sellAgentFirstName",
      "sellAgentLastName",
      "sellAgentPhone1",
      "coSellAgentId",
      "coSellAgentFirstName",
      "coSellAgentLastName",
      "coSellAgentPhone1",
      "dom",
      "city",
      "dateList",
      "listPrice",
      "state",
      "address",
      "bank",
      "statusCode",
      "dateStatusChange",
      "zipCode",
      "listingId",
      "officeId",
      "officeName",
      "streetName1",
      "officeCity",
      "officeZip",
      "officeState"
   ],
   "office-2.0":[
      "mlsSid",
      "officeId",
      "officeName",
      "agents",
      "officeAddress1",
      "officeAddress2",
      "officeCity",
      "officeState",
      "officeZip",
      "officeGeoCode",
      "officeCounty",
      "officeStatus",
      "officePhone",
      "officeFax",
      "officeEmail"
   ],
   "listings-3.0":[
      "mlsSid",
      "listAgentId",
      "listAgentFirstName",
      "listAgentLastName",
      "listAgentPhone1",
      "coListAgentId",
      "coListAgentFirstName",
      "coListAgentLastName",
      "coListAgentPhone1",
      "sellAgentId",
      "sellAgentFirstName",
      "sellAgentLastName",
      "sellAgentPhone1",
      "coSellAgentId",
      "coSellAgentFirstName",
      "coSellAgentLastName",
      "coSellAgentPhone1",
      "dom",
      "city",
      "dateList",
      "listPrice",
      "state",
      "address",
      "addressUnit",
      "bank",
      "statusCode",
      "dateStatusChange",
      "zipCode",
      "listOfficeId",
      "listOfficeName",
      "listOfficeStreetName1",
      "listOfficeCity",
      "listOfficeZip",
      "listOfficeState",
      "sellOfficeId",
      "sellOfficeName",
      "sellOfficeStreetName1",
      "sellOfficeCity",
      "sellOfficeZip",
      "sellOfficeState"
   ]
}


# COMMAND ----------

# DBTITLE 1,URL Config dictionnaries
## StructField(<>, StringType(), True),
## 0: endpoint, 1: authToken, 2: mlsSid, 3: fromDate, 4: toDate( if applicable)

uatEndpoints = {
  "agent-move-1.0" : 'http://apis.terradatum.com//firstam/firstam/agent-move-1.0.xml',
  "agent-new-1.0" : 'http://apis.terradatum.com//firstam/firstam/agent-new-1.0.xml',
  "agent-2.0" : 'http://apis.terradatum.com//firstam/firstam/agent-2.0.xml',
  "agents-all-1.0" : 'http://apis.terradatum.com//firstam/firstam/agents-all-1.0.xml',
  "closed-listings-1.0" : 'http://apis.terradatum.com//firstam/firstam/closed-listings-1.0.xml',
  "listings-2.0" : 'http://apis.terradatum.com//firstam/firstam/listings-2.0.xml',
  "office-2.0" : 'http://apis.terradatum.com//firstam/firstam/office-2.0.xml',
  "listings-3.0" : 'http://apis.terradatum.com//firstam/firstam/listings-3.0.xml'
}

localEndpoints = {
  "agent-move-1.0" : 'https://vbuttons.net/api/agentMove.php',
  "agent-new-1.0" : 'https://vbuttons.net/api/agentNew.php',
  "agent-2.0" : 'https://vbuttons.net/api/agent.php',
  "agents-all-1.0" : 'https://vbuttons.net/api/agentsAll.php',
  "closed-listings-1.0" : 'https://vbuttons.net/api/closedListing.php',
  "listings-2.0" : 'https://vbuttons.net/api/listing.php',
  "office-2.0" : 'https://vbuttons.net/api/office.php',
  "listings-3.0" : 'https://vbuttons.net/api/listing3.php'
}

endpoint_url_params = {
  "agent-move-1.0" : '{0}?authToken={1}&mlsSid={2}&status=All&searchStr=All&searchBy=office&fromDate={3}&recLimit=1000000',
  "agent-new-1.0" : '{0}?authToken={1}&mlsSid={2}&status=All&searchStr=All&searchBy=office&fromDate={3}&recLimit=1000000',
  "agent-2.0" : '{0}?authToken={1}&mlsSid={2}&status=A&searchStr=All&fromDate={3}',
  "agents-all-1.0" : '{0}?authToken={1}&mlsSid={2}&status=A&searchStr=All&fromDate={3}',
  "closed-listings-1.0" : '{0}?authToken={1}&mlsSid={2}&status=A&searchStr=All&fromDate={3}',
  "listings-2.0" : '{0}?authToken={1}&mlsSid={2}&status=All&searchStr=All&searchBy=office&fromDate={3}&recLimit=1000000',
  "office-2.0": '{0}?authToken={1}&mlsSid={2}&status=A&searchStr=All&fromDate={3}',
  "listings-3.0": '{0}?authToken={1}&mlsSid={2}&status=All&searchStr=All&searchBy=office&fromDate={3}&toDate={4}&recLimit=1000000'
}


# COMMAND ----------

'hello {0}, I am {1} {2}'.format('Adil', 'happy to meet you', '!')

# COMMAND ----------

spark = SparkSession.builder.appName('PySpark APP').getOrCreate()

# COMMAND ----------

class MlsEndpointHelper:
#     t_mls_ca =[278,14,4,29,238,125,165,16,167,45,5,201,36,92,1,156,6,3,295,36,345,65,63]
    t_mls_ca =[278,14,4]
    t_apis=['agent-move-1.0','agent-new-1.0','agent-2.0','agents-all-1.0','closed-listings-1.0','listings-2.0','office-2.0','listings-3.0']
    #baseurl='https://vbuttons.net/stagingapi/'
    authTokenauthToken='AMERFST7543567OP'
    env = 'dev'
    
    def parse_XML(self, xml_file, df_cols): 
        """Parse the input XML file and store the result in a pandas 
        DataFrame with the given columns. 

        The first element of df_cols is supposed to be the identifier 
        variable, which is an attribute of each node element in the 
        XML data; other features will be parsed from the text content 
        of each sub-element. 
        """
        try:
            xtree = et.parse(xml_file)
            xroot = xtree.getroot()
            rows = []

            for node in xroot:
                res = []
               # res.append(node.attrib.get(df_cols[0]))
                for el in df_cols[0:]:
                    if node is not None and node.find(el) is not None:
                        res.append(node.find(el).text)
                    else:
                        res.append(None)
                rows.append({df_cols[i]: res[i]
                             for i, _ in enumerate(df_cols)})

            out_df = pd.DataFrame(rows, columns=df_cols)

            return out_df
        except Exception as e:
            print('Exception in parse_XML():['+xml_file.name+']', e)
            traceback.print_exc()
        return out_df
    
        
    def getSchema (self, endpoint):
        mySchema = endpoint_config_schema[endpoint]
        return mySchema
    
    def getListMls_CA (self):
        return self.t_mls_ca
    
    def getRowCols (self, endpoint):
        print('getRowCols '+endpoint)
        cols=[]
        try:
            cols = endpoint_config_cols[endpoint]
        except Exception as e:
            print('Exception in getRowCols():['+endpoint+']', e)       
        return cols
    
        
    def buildUrl (self, endpoint, mlsSid, fromDate, toDate):
        
        print('buildUrl')
        url=''
        if self.env == 'dev':
            baseUrl=localEndpoints[endpoint]
        else:
            baseUrl=uatEndpoints[endpoint]
        print('buildUrl : '+baseUrl)   
        yesterday = datetime.now() - timedelta(1)
        if not fromDate:
            fromDate = datetime.strftime(yesterday, '%m-%d-%Y')
        if not toDate:
            toDate = datetime.strftime(datetime.now(), '%m-%d-%Y')
        try:
            url = endpoint_url_params[endpoint].format(baseUrl,authToken, mlsSid, fromDate,toDate)
        except Exception as e:
            print('Exception in buildUrl():['+endpoint+']', e)   
        return url
                        
    def loadUrlToFile(self, endpoint, url):
        # url of xml data
        print(url)
        # creating HTTP response object from given url
        try:
            resp = requests.get(url)

            if resp.status_code == requests.codes.ok:
                #print('Headers'+resp.headers['Content-Disposition'])
                #resp.headers['Content-Disposition'].split(';')[1].split('=')[1]
                filename = endpoint+'.'+datetime.strftime(datetime.now(), '%m%d%Y')+'.xml'
                print('file '+filename+' was downloaded.')
                # saving the xml file
                with open(filename, 'wb') as fl:
                    fl.write(resp.content)
                
        except Exception as e:
            print('Exception in loadUrlToFile():['+ url +']', e)
            return None 
        return fl  
    
    def download_data_for_endpoint (self, endpoint, fromDay, toDay):
        print('download_data_for_endpoint '+endpoint+' '+fromDay)        
        g_df = pd.DataFrame()
        #frames = []
        f=None
        t_mls_ca = self.getListMls_CA ()
        print(t_mls_ca)
        char_to_replace = {'-': '','.': ''} # ? why?
        try:
            spark_df=None
            for mls in t_mls_ca:
                cols = self.getRowCols (endpoint)
                url = self.buildUrl(endpoint, mls, fromDay, toDay)
                f = self.loadUrlToFile(endpoint, url)
                if f :
                    try:
                        f =open(f.name)
                        df = self.parse_XML(f, cols) 
                        #frames.append(df)
                        g_df=pd.concat([g_df, df], axis=0)
                        print(g_df)
                    except Exception as ei:
                        print('Exception in download_data_for_endpoint(In):', ei)  
                    finally:                        
                        f.close ()
            os.remove(f.name)
            table_name = endpoint 
            for key, value in char_to_replace.items():
                table_name = table_name.replace(key, value)
            mySchema = self.getSchema(endpoint)                
            spark_df = spark.createDataFrame(g_df, schema=mySchema)
            spark_df.write.mode("append") \
                        .saveAsTable(table_name)
        except Exception as e:
            print('Exception in download_data_for_endpoint():', e)    
            
        return spark_df
      

# COMMAND ----------

import concurrent.futures
import time


#authToken='AMERFST7543567OP' # dev
authToken='HVJOXQNIVXKOGOBGLOWU' #preprod



def multi_run_wrapper(args):
    tool = MlsEndpointHelper()
    # [278,14,4,29,238,125,165,16,167,45,5,201,36,92,1,156,6,3,295,36,345,65,63]
    tool.t_mls_ca = [278,14,238,125,167,36,92]
    tool.env='uat'
    return tool.download_data_for_endpoint(*args)  
    


#t_apis=['office-2.0','agent-2.0','listings-3.0','closed-listings-1.0','listings-2.0','agent-move-1.0','agent-new-1.0','agents-all-1.0']
#

endpoint = dbutils.widgets.get("endpoint")
fromDate = dbutils.widgets.get("fromDate")
toDate = dbutils.widgets.get("toDate")

arguments =[(endpoint,fromDate,toDate)]
start = time.perf_counter()

# Test with one endpoint
multi_run_wrapper(arguments[0])
#
#with concurrent.futures.ProcessPoolExecutor() as executor:
#    pool = [executor.submit(multi_run_wrapper, i) for i in arguments]
#    for i in concurrent.futures.as_completed(pool):
#        sdf=i.result()
#        sdf.show()
#        sdf.stop()
        #
end = time.perf_counter()
print(f'Finished in {round(end-start, 2)} second(s)')  

