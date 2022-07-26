// Databricks notebook source
// MAGIC %md
// MAGIC ## Purpose
// MAGIC The entire purpose of this notebook is to create a json config that can be used to run Overwatch. 
// MAGIC 
// MAGIC This notebook intends to simplify the generic path construction and eliminate common errors in the standard JSON config. 
// MAGIC 
// MAGIC The entirety of what is happening is:
// MAGIC * A json string is being constructed
// MAGIC * The json config string is being used to construct the `workspace` object used by Overwatch which can be passed into the Bronze/Silver/Gold execution pipelines
// MAGIC * The `*.run()` functions are using the workspace object to run the referenced pipeline (i.e. `Bronze(workspace).run()`)

// COMMAND ----------

import com.databricks.labs.overwatch.pipeline.{Initializer, Bronze, Silver, Gold}
import com.databricks.labs.overwatch.utils._
import com.databricks.labs.overwatch.pipeline.TransformFunctions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC ## Some Helper Functions / Vars

// COMMAND ----------

val workspaceID = if (dbutils.notebook.getContext.tags("orgId") == "0") {
  dbutils.notebook.getContext.tags("browserHostName").split("\\.")(0)
} else dbutils.notebook.getContext.tags("orgId")

def pipReport(etlDB: String): DataFrame = {
  spark.table(s"${etlDB}.pipReport")
    .filter('organization_id === workspaceID)
    .orderBy('Pipeline_SnapTS.desc)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Setup Widgets For Simple Adjustments Or Job Configs
// MAGIC * Initiallize the widgets if running interactively
// MAGIC * Pull the widgets into usable variables to construct the config

// COMMAND ----------

//  dbutils.widgets.removeAll
//  dbutils.widgets.text("storagePrefix", "", "1. ETL Storage Prefix")
// dbutils.widgets.text("etlDBName", "overwatch_etl", "2. ETL Database Name")
// dbutils.widgets.text("consumerDBName", "overwatch", "3. Consumer DB Name")
// dbutils.widgets.text("secretsScope", "my_secret_scope", "4. Secret Scope")
// dbutils.widgets.text("dbPATKey", "my_key_with_api", "5. Secret Key (DBPAT)")
// dbutils.widgets.text("ehKey", "overwatch_eventhub_conn_string", "6. Secret Key (EH)")
// dbutils.widgets.text("ehName", "my_eh_name", "7. EH Topic Name")
// dbutils.widgets.text("primordialDateString", "2021-04-01", "8. Primordial Date")
//  dbutils.widgets.text("maxDaysToLoad", "60", "9. Max Days")
// dbutils.widgets.text("scopes", "all", "A1. Scopes")

// COMMAND ----------

val storagePrefix = dbutils.widgets.get("storagePrefix").toLowerCase // PRIMARY OVERWATCH OUTPUT PREFIX
val etlDB = dbutils.widgets.get("etlDBName").toLowerCase
val consumerDB = dbutils.widgets.get("consumerDBName")
val secretsScope = dbutils.widgets.get("secretsScope")
val dbPATKey = dbutils.widgets.get("dbPATKey")
val ehName = dbutils.widgets.get("ehName")
val ehKey = dbutils.widgets.get("ehKey")
val primordialDateString = dbutils.widgets.get("primordialDateString")
val maxDaysToLoad = dbutils.widgets.get("maxDaysToLoad").toInt
val scopes = if (dbutils.widgets.get("scopes") == "all") {
  "audit,sparkEvents,jobs,clusters,clusterEvents,notebooks,pools,accounts".split(",")
} else dbutils.widgets.get("scopes").split(",")

if (storagePrefix.isEmpty || consumerDB.isEmpty || etlDB.isEmpty || ehName.isEmpty || secretsScope.isEmpty || ehKey.isEmpty || dbPATKey.isEmpty) {
  throw new IllegalArgumentException("Please specify all required parameters!")
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Construct the Overwatch Params and Instantiate Workspace

// COMMAND ----------

private val dataTarget = DataTarget(
  Some(etlDB), Some(s"${storagePrefix}/${etlDB}.db"), Some(s"${storagePrefix}/global_share"),
  Some(consumerDB), Some(s"${storagePrefix}/${consumerDB}.db")
)

private val tokenSecret = TokenSecret(secretsScope, dbPATKey)
private val ehConnString = s"{{secrets/${secretsScope}/${ehKey}}}"

private val ehStatePath = s"${storagePrefix}/${workspaceID}/ehState"
private val badRecordsPath = s"${storagePrefix}/${workspaceID}/sparkEventsBadrecords"
private val azureLogConfig = AzureAuditLogEventhubConfig(connectionString = ehConnString, eventHubName = ehName, auditRawEventsPrefix = ehStatePath)
private val interactiveDBUPrice = 0.275
private val automatedDBUPrice = 0.15
private val customWorkspaceName = "cdodbelt-elt1" // customize this to a custom name if custom workspace_name is desired

val params = OverwatchParams(
  auditLogConfig = AuditLogConfig(azureAuditLogEventhubConfig = Some(azureLogConfig)),
  dataTarget = Some(dataTarget), 
  tokenSecret = Some(tokenSecret),
  badRecordsPath = Some(badRecordsPath),
  overwatchScope = Some(scopes),
  maxDaysToLoad = maxDaysToLoad,
  databricksContractPrices = DatabricksContractPrices(interactiveDBUPrice, automatedDBUPrice),
  primordialDateString = Some(primordialDateString),
  workspace_name = Some(customWorkspaceName), 
  externalizeOptimize = true
)

private val args = JsonUtils.objToJson(params).compactString
val workspace = Initializer(args, debugFlag = true)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Show the Config Strings
// MAGIC This is a good time to run the following commands for when you're ready to convert this to run as a job as a main class

// COMMAND ----------

// use this to run Overwatch as a job
JsonUtils.objToJson(params).escapedString

// COMMAND ----------

// use this to get a compact string of the created parameters for reference and/or creating workspace object on the fly
JsonUtils.objToJson(params).compactString

// COMMAND ----------

// Prints a pretty string of the entire config to be used in the run
JsonUtils.objToJson(params).prettyString

// COMMAND ----------

// MAGIC %md
// MAGIC ## Execute The Pipeline
// MAGIC If running Overwatch as a notebook, use the following commands

// COMMAND ----------

Bronze(workspace).run()

// COMMAND ----------

Silver(workspace).run()

// COMMAND ----------

Gold(workspace).run()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Show The Run Report
// MAGIC The `pipReport` is the Pipeline run status report which shows the run details for each configured module. The function definition is at the top of this notebook but is essentially a select * from your overwatch_etl_database.pipReport. `pipReport` is a view atop the overwatch_etl_database.pipeline_report table.

// COMMAND ----------

display(
  pipReport(etlDB)
)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from overwatchconsumerDB.clusterstatefact where workspace_name=="cdodbelt-elt1"

// COMMAND ----------


