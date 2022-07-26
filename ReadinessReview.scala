// Databricks notebook source
import com.databricks.labs.overwatch.ApiCall
import com.databricks.labs.overwatch.utils.ApiEnv
import org.apache.spark.sql.functions._
import java.time.{LocalDate, ZoneId}
import scala.collection.parallel.ForkJoinTaskSupport
import java.util.concurrent.ForkJoinPool
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

// COMMAND ----------

// MAGIC %md
// MAGIC # Validate the Overwatch Mount Points Are Present
// MAGIC * Do the Overwatch mounts exist and are they mounted/mapped to the right location

// COMMAND ----------

// MAGIC %fs mounts

// COMMAND ----------

val DBATSECRETSCOPE = "dl-eastus2-prd-sec-kv-scope"
val DBPATKEY = "dbpatkey-etl1"
val OVERWATCHVERSION = "0.6.0.4"
val PARALLELISM = 4 // ENTIRE workspace only has 32 threads, so don't put this too high

// AZURE ONLY
val EHSECRETSCOPE = "dl-eastus2-prd-sec-kv-scope"
val EHCONNSTRINGKEYNAME = "ehconnectionstring-etl1"
val EHNAME = "rdw-workspace-eventhub"

// COMMAND ----------

val apiEnv = ApiEnv(false, dbutils.notebook.getContext.apiUrl.get, dbutils.secrets.get(DBATSECRETSCOPE, DBPATKEY), OVERWATCHVERSION)
// val apiEnv = ApiEnv(false, dbutils.notebook.getContext.apiUrl.get, dbutils.notebook.getContext.apiToken.get, OVERWATCHVERSION)

// COMMAND ----------

val apiEnv = ApiEnv(false, dbutils.notebook.getContext.apiUrl.get, "dapi46808fed54d561b85ac2619a24b0db9d-2", OVERWATCHVERSION)

// COMMAND ----------

// MAGIC %md
// MAGIC # Get Cluster Current State Details
// MAGIC * Ensure that all the clusters you want Overwatch to monitor show up in the list below

// COMMAND ----------

// MAGIC %python
// MAGIC # import os
// MAGIC # old_no_proxy = os.getenv("no_proxy")
// MAGIC # new_no_proxy = old_no_proxy + ",paloma.palantirfoundry.com," + "eastus2-c2.azuredatabricks.net"
// MAGIC # os.environ["no_proxy"] = new_no_proxy
// MAGIC # os.environ["NO_PROXY"] = new_no_proxy

// COMMAND ----------

// no_proxy=127.0.0.1,.local,168.63.129.16,azuredatabricks.net,*.blob.core.windows.net,*.database.windows.net,*.dfs.core.windows.net,*.servicebus.windows.net,*.vaultcore.azure.net,*.svc.local,*.queue.core.windows.net,paloma.palantirfoundry.com,steam.h2o.web.att.com,auth.h2o.web.att.com,steam.h2o.dev.att.com,auth.h2o.dev.att.com,,paloma.palantirfoundry.com,eastus2-c2.azuredatabricks.net

// COMMAND ----------

// MAGIC %sh env|grep no_proxy

// COMMAND ----------

// MAGIC %python
// MAGIC # import socket
// MAGIC # ip_list = []
// MAGIC # ais = socket.getaddrinfo("eastus2-c2.azuredatabricks.net",0,0,0,0)
// MAGIC # for result in ais:
// MAGIC #   ip_list.append(result[-1][0])
// MAGIC # ip_list = list(set(ip_list))
// MAGIC # ip_list

// COMMAND ----------

val endpoint = "clusters/list"
val clustersList = ApiCall(endpoint, apiEnv, None)
  .executeGet().asDF
display(clustersList)

// COMMAND ----------

val endpoint = "clusters/list"
val clustersList = ApiCall(endpoint, apiEnv, None)
  .executeGet().asDF

val simpleClusters = clustersList
  .select('cluster_id, 'cluster_name, $"cluster_log_conf.dbfs.destination".alias("cluster_log_target"), 'state, from_unixtime('start_time / 1000).cast("timestamp").alias("created_ts"))

display(
  simpleClusters
)

// COMMAND ----------

val clusterCount = clustersList.count
println(s"THE PROVIDED API HAS FOUND ${clusterCount} CLUSTERS")

// COMMAND ----------

// MAGIC %md
// MAGIC # Validate Overwatch Access to Clusters
// MAGIC * Read access (can attach to) is required for the Overwatch user to get the cluster details. One way to validate this is to ensure the clusters events can be read
// MAGIC * NOTE - events are only availalbe for 
// MAGIC   * 30 days
// MAGIC   * clusters that have not been `permanentlyDeleted`

// COMMAND ----------

val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(PARALLELISM))
val recentClusterIDs = simpleClusters.select('cluster_id).distinct.as[String].collect().par
recentClusterIDs.tasksupport = taskSupport

val jobsClusters = simpleClusters.filter('cluster_name.like("job-%-run-%"))
val interactiveClusters = simpleClusters.filter(!'cluster_name.like("job-%-run-%"))

val jobsClustersCount = jobsClusters.count
val interactiveClustersCount = interactiveClusters.count

val clusterGetResults = recentClusterIDs.map(clusterId => {
  val clusterGetEndpoint = "clusters/get"
  val q = Some(Map(
    "cluster_id" -> clusterId
  ))
  val apiResult = ApiCall(clusterGetEndpoint, apiEnv, q)
    .executeGet().asStrings
  if (apiResult.nonEmpty) (clusterId, apiResult.head) else (clusterId, "ERROR: UNABLE TO ACCESS CLUSTER DETAILS")
}).toArray.toSeq.toDF("cluster_id", "clusterSpec")

val unreachableClusters = clusterGetResults.filter('clusterSpec.like("ERROR:%"))
val reachableClusters = clusterGetResults.filter(!'clusterSpec.like("ERROR:%"))
val unreachableClustersCount = unreachableClusters.count
val reachableClustersCount = reachableClusters.count

val mapper = new ObjectMapper() with ScalaObjectMapper
mapper.registerModule(DefaultScalaModule)

val eventsEndpoint = "clusters/events"

// cluster events
val lastEventByCluster = recentClusterIDs.map(clusterId => {
  val q = Some(Map(
    "cluster_id" -> clusterId,
    "order" -> "DESC",
    "limit" -> 1
  ))
  val apiResult = ApiCall(eventsEndpoint, apiEnv, q, paginate = false)
    .executePost().asStrings
  val newRecords = apiResult
    .map(r => mapper.readValue[Map[String, Any]](r)) // parse row
    .map(record => record.get("events").asInstanceOf[Option[List[Map[String, Any]]]]) // parse events
    .map(events => {
      events match {
        case Some(event) => (clusterId, events.get.head.get("timestamp").asInstanceOf[Option[Long]], events.get.head.get("type").asInstanceOf[Option[String]])
        case None => (clusterId, None, None)
      }
    })
    newRecords
}).toArray.flatten.toSeq.toDF("cluster_id", "last_event_ts", "cluster_event")

val clustersWithoutEvents = lastEventByCluster.filter('last_event_ts.isNull)
val clustersWithEvents = lastEventByCluster.filter(!'last_event_ts.isNull)
val clustersWithoutEventsCount = clustersWithoutEvents.count
val clustersWithEventsCount = clustersWithEvents.count

// COMMAND ----------

println(s"OF $clusterCount CURRENT CLUSTERS SPECS FOUND:")
println(s"\tJOBS ClUSTERS: $jobsClustersCount")
println(s"\tINTERACTIVE ClUSTERS: $interactiveClustersCount")

// COMMAND ----------

println(s"CLUSTER GET SUCCESS TOTAL COUNT: $reachableClustersCount")
println(s"CLUSTER GET FAILRES TOTAL COUNT: $unreachableClustersCount")
println(s"FAILED CLUSTERS INCLUDE: ${unreachableClusters.select(col("cluster_id")).as[String].collect.mkString(", ")}")

// COMMAND ----------

println(s"CLUSTER EVENT SUCCESS TOTAL COUNT: $clustersWithEventsCount")
println(s"CLUSTER EVENT FAILRES TOTAL COUNT: $clustersWithoutEventsCount")
println(s"FAILED CLUSTERS INCLUDE: ${clustersWithoutEvents.select(col("cluster_id")).as[String].collect.mkString(", ")}")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Cluster Counts By Log Target
// MAGIC * Ensure that your clusters are logging to the right location. 
// MAGIC * To control this 
// MAGIC   * enable [cluster policies](https://docs.databricks.com/administration-guide/clusters/policies.html) for adhoc clusters
// MAGIC   * enable [cluster policies](https://docs.databricks.com/administration-guide/clusters/policies.html#job-only-policy) for jobs clusters
// MAGIC   * enable internal CI/CD processes to set logging targets correctly

// COMMAND ----------

display(
  simpleClusters
    .groupBy('cluster_log_target).count
)

// COMMAND ----------

// MAGIC %md
// MAGIC # Validate Job Logging Targets
// MAGIC * Ensure that all the jobs you want Overwatch to monitor show up in the list below
// MAGIC * Ensure that all the jobs have an appropriate logging target

// COMMAND ----------

val endpoint = "jobs/list"
val jobsList = ApiCall(endpoint, apiEnv, None)
  .executeGet().asDF

val clusterLogLocLookup = simpleClusters.select('cluster_id, 'cluster_log_target)//.alias("job_log_target"))
val jobLogLocations = jobsList
  .selectExpr("*", "settings.*").drop("settings")
//   .select('job_id, 'name.alias("job_name"), $"new_cluster.cluster_log_conf.dbfs.destination".alias("cluster_log_target"))

display(
  jobLogLocations
)

// COMMAND ----------

private val jobsCount = jobsList.count
println(s"THE PROVIDED API HAS ACCESS TO ${jobsCount} JOBS")

// COMMAND ----------

// MAGIC %md
// MAGIC # Pools Access

// COMMAND ----------

val poolsEndpoint = "instance-pools/list"
val poolsList = ApiCall(poolsEndpoint, apiEnv)
  .executeGet()
  .asDF

// COMMAND ----------

private val poolsCount = poolsList.count
println(s"THE PROVIDED API HAS ACCESS TO ${poolsCount} POOLS")

// COMMAND ----------

// MAGIC %md
// MAGIC # EH Test (Azure Only)
// MAGIC Validate access to Event Hub and that a stream can be enabled using the EH credentials. If the connection string is stored correctly, the stream should begin and data should begin flowing at 100 records per batch

// COMMAND ----------

import java.time.Duration
import org.apache.spark.eventhubs.{ConnectionStringBuilder, EventHubsConf, EventPosition}
val connectionString = ConnectionStringBuilder(dbutils.secrets.get(EHSECRETSCOPE, EHCONNSTRINGKEYNAME))
  .setEventHubName(EHNAME)
  .setOperationTimeout(Duration.ofSeconds(1200))
  .build

val ehConf = EventHubsConf(connectionString)
  .setMaxEventsPerTrigger(100)
  .setStartingPosition(EventPosition.fromEndOfStream)

val rawEHDF = spark.readStream
  .format("eventhubs")
  .options(ehConf.toMap)
  .load()
  .withColumn("deserializedBody", 'body.cast("string"))

// COMMAND ----------

// MAGIC %sh telnet eastus2-c2.azuredatabricks.net 443

// COMMAND ----------

display(rawEHDF)

// COMMAND ----------

// MAGIC %sh
// MAGIC curl -n -X GET -H 'Authorization: Bearer dapi27188fe83eeac16a192ba0e20e935680-2' https://eastus2-c2.azuredatabricks.net/api/2.0/clusters/list

// COMMAND ----------

https://eastus2-c2.azuredatabricks.net/api/2.0/clusters/list
