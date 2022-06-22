// Databricks notebook source
// MAGIC %md This notebook illustrates various ways of running notebooks concurrently. To see print outs from the child notebooks, click the ``Notebook job #nnn`` links.

// COMMAND ----------

// MAGIC %md  
// MAGIC A caveat: Outputs from notebooks cannot interact with each other. If you're writing to files/tables, output paths should be parameterized via widgets. This is similar to two users running same notebook on the same cluster at the same time; they should share no state.

// COMMAND ----------

// MAGIC %md The ``parallel-notebooks`` notebook defines functions as if they were in a library.

// COMMAND ----------

// MAGIC %run "./parallel-notebooks-utils"

// COMMAND ----------

// MAGIC %md 
// MAGIC The remaining commands invoke ``NotebookData`` functions in various ways.

// COMMAND ----------

// MAGIC %md The first version creates a sequence of notebook, timeout, parameter combinations and passes them into the ``parallelNotebooks`` function defined in the ``parallel notebooks`` notebook. The ``parallelNotebooks`` function prevents overwhelming the driver by limiting the the number of threads that are running.

// COMMAND ----------

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

val fromDate ="06-16-2022";
val toDate ="06-17-2022";
val env = "dev"
val notebooks = Seq(
  NotebookData("load_xmldata_from_url", 600, Map("env" -> env, "endpoint" -> "agent-move-1.0","fromDate" -> fromDate,"toDate" -> toDate)),
  NotebookData("load_xmldata_from_url", 600, Map("env" -> env, "endpoint" -> "agent-new-1.0","fromDate" -> fromDate,"toDate" -> toDate)),
  NotebookData("load_xmldata_from_url", 600, Map("env" -> env, "endpoint" -> "agent-2.0","fromDate" -> fromDate,"toDate" -> toDate)),
  NotebookData("load_xmldata_from_url", 600, Map("env" -> env, "endpoint" -> "agents-all-1.0","fromDate" -> fromDate,"toDate" -> toDate)),
  NotebookData("load_xmldata_from_url", 600, Map("env" -> env, "endpoint" -> "closed-listings-1.0","fromDate" -> fromDate,"toDate" -> toDate)),
  NotebookData("load_xmldata_from_url", 600, Map("env" -> env, "endpoint" -> "listings-2.0","fromDate" -> fromDate,"toDate" -> toDate)),
  NotebookData("load_xmldata_from_url", 600, Map("env" -> env, "endpoint" -> "office-2.0","fromDate" -> fromDate,"toDate" -> toDate)),
  NotebookData("load_xmldata_from_url", 600, Map("env" -> env, "endpoint" -> "listings-3.0","fromDate" -> fromDate,"toDate" -> toDate))
)

val res = parallelNotebooks(notebooks)

Await.result(res, 1000 seconds) // this is a blocking call.
res.value

// COMMAND ----------

// MAGIC %python
// MAGIC feedList=['agent-move-1.0','agent-new-1.0','agent-2.0','agents-all-1.0','closed-listings-1.0','listings-2.0','office-2.0','listings-3.0']
// MAGIC 
// MAGIC for item in feedList:
// MAGIC   print("""NotebookData("load_xmldata_from_url", 600, Map("endpoint" -> "{0}","fromDate" -> fromDate,"toDate" -> toDate))""".format(item))
// MAGIC  

// COMMAND ----------

// MAGIC %python
// MAGIC dataList=['mlsSid','agentId','firstName','lastName','agentStatus','agentPhone1','agentPhoneType1','agentPhone2','agentPhoneType2','agentPhone3','agentPhoneType3','agentFax','agentEmail','officeId','officeName','officeAddress','officeCity','officeState','officeZip']
// MAGIC for item in dataList:
// MAGIC   print('StructField("{0}", StringType(), True),'.format(item))

// COMMAND ----------

// MAGIC %md This next version defines individual parallel notebooks. It takes the same parameters as above except does it on an individual basis and you have to specify the result sequence that has to run. That has to be a Future for you to receive.
