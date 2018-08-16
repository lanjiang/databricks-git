// Databricks notebook source
// MAGIC %fs ls /lan5

// COMMAND ----------

// MAGIC %sh
// MAGIC cp /databricks/jars/spark--versions--2.3--avro_2.11_deploy.jar /dbfs/lan/spark--versions--2.3--avro_2.11_deploy.jar

// COMMAND ----------

// MAGIC %sh 
// MAGIC aws /databricks/jars/spark--versions--2.3--avro_2.11_deploy.jar

// COMMAND ----------

// MAGIC %fs ls s3a://da-databricks-training/SparkEssentials/

// COMMAND ----------

// MAGIC %sql 
// MAGIC drop table deltatest

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC CREATE TABLE deltatest (
// MAGIC   eventId STRING,
// MAGIC   data STRING)
// MAGIC USING DELTA
// MAGIC LOCATION '/lan/delta/deltatest'

// COMMAND ----------

// MAGIC %sql
// MAGIC describe formatted deltatest

// COMMAND ----------

spark.sql("analyze table deltatest COMPUTE STATISTICS")

// COMMAND ----------

// MAGIC %sql
// MAGIC describe formatted deltatest

// COMMAND ----------

spark.sql("analyze table deltatest COMPUTE STATISTICS FOR COLUMNS eventid, data")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from deltatest limit 5

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC describe formatted deltatest eventid

// COMMAND ----------

spark.sql("analyze table parquettest compute statistics for columns duration")

// COMMAND ----------

// MAGIC %sql
// MAGIC vacuum deltatest

// COMMAND ----------

// MAGIC %fs head /lan/delta/deltatest/_delta_log/00000000000000000000.json

// COMMAND ----------

// MAGIC %sql
// MAGIC inse

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into deltatest values ("2", "BBB")

// COMMAND ----------

// MAGIC %sql
// MAGIC optimize deltatest

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into table deltatest 
// MAGIC select "3", "CCC"
// MAGIC union all
// MAGIC select "4", "DDD"

// COMMAND ----------

// MAGIC %sql
// MAGIC update  deltatest
// MAGIC set data="AAAA"
// MAGIC where eventId="1"

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC CREATE TABLE eventsource (
// MAGIC   eventId STRING,
// MAGIC   data STRING)
// MAGIC USING DELTA
// MAGIC LOCATION '/lan/delta/eventsource'

// COMMAND ----------

// MAGIC %sql 
// MAGIC CREATE TABLE eventtarget (
// MAGIC   eventId STRING,
// MAGIC   data STRING,
// MAGIC   flag STRING)
// MAGIC USING DELTA
// MAGIC LOCATION '/lan/delta/eventtarget'

// COMMAND ----------

// MAGIC %sql 
// MAGIC insert into eventsource
// MAGIC values ("1", "AAA")

// COMMAND ----------

// MAGIC %sql
// MAGIC update eventsource 
// MAGIC set data="AAAAAA"
// MAGIC where eventId= '1'

// COMMAND ----------

// MAGIC %sql 
// MAGIC insert into eventsource
// MAGIC values ("2", "BBB")

// COMMAND ----------

// MAGIC %sql 
// MAGIC insert into eventsource
// MAGIC values ("4", "DDD")

// COMMAND ----------

// MAGIC %sql 
// MAGIC insert into eventtarget
// MAGIC values ("1", "AAA", "I")

// COMMAND ----------

// MAGIC %sql 
// MAGIC insert into eventtarget
// MAGIC values ("2", "BBB", "I")

// COMMAND ----------

// MAGIC %sql 
// MAGIC insert into eventtarget
// MAGIC values ("3", "CCC", "I")

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from eventtarget

// COMMAND ----------

// MAGIC %sql
// MAGIC update eventtarget set flag = "D"
// MAGIC from eventsource right join eventtarget
// MAGIC on eventsource.eventId = eventtarget.eventId
// MAGIC where eventsource.eventId is null

// COMMAND ----------

// MAGIC %md
// MAGIC UPDATE table1 JOIN table2 
// MAGIC ON table1.id = table2.id
// MAGIC SET table1.name = table2.name,
// MAGIC table1.`desc` = table2.`desc`

// COMMAND ----------

// MAGIC %sql
// MAGIC update eventsource right join eventtarget
// MAGIC on eventsource.eventId = eventtarget.eventId
// MAGIC where eventsource.eventId is null
// MAGIC set eventtarget.flag="D"

// COMMAND ----------

// MAGIC %sql
// MAGIC update eventtarget set eventtarget.flag = "D"
// MAGIC where eventtarget.eventId in 
// MAGIC (select eventtarget.eventid 
// MAGIC from eventsource right join eventtarget
// MAGIC on eventsource.eventId = eventtarget.eventId
// MAGIC where eventsource.eventId is null
// MAGIC 
// MAGIC )

// COMMAND ----------

// MAGIC 
// MAGIC %sql 
// MAGIC update eventtarget set flag="U"
// MAGIC from eventsource right join eventtarget
// MAGIC on eventsource.eventId = eventtarget.eventId
// MAGIC where eventsource.eventId is null

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC update eventtarget set flag="U"
// MAGIC from eventsource  join eventtarget
// MAGIC on eventsource.eventId = eventtarget.eventId

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC MERGE INTO eventtarget
// MAGIC USING eventsource 
// MAGIC WHERE dest.OFFICE_ID=events.OFFICE_ID and dest.ID=events.ID
// MAGIC WHEN MATCHED then UPDATE <all-fields>

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC select *  from eventsource right join eventtarget
// MAGIC on eventsource.eventId = eventtarget.eventId
// MAGIC where eventsource.eventId is null

// COMMAND ----------

// MAGIC %sql
// MAGIC /* newly added */
// MAGIC select s.eventId, s.data, "I"
// MAGIC from eventsource s left join eventtarget t
// MAGIC on s.eventId = t.eventId
// MAGIC where t.eventId is null

// COMMAND ----------

// MAGIC %sql 
// MAGIC /* unchanged and updated */
// MAGIC select s.eventId, s.data, case when md5(s.data)= md5(t.data) then t.flag else "U" end
// MAGIC from eventsource s join eventtarget t
// MAGIC on s.eventId = t.eventId

// COMMAND ----------

// MAGIC %sql 
// MAGIC /* deleted */
// MAGIC select t.eventId, t.data, "D"
// MAGIC from eventsource s right join eventtarget t
// MAGIC on s.eventId = t.eventId
// MAGIC where s.eventid is null

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from eventsource

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from eventtarget

// COMMAND ----------

// MAGIC %sql
// MAGIC /* newly added */
// MAGIC select s.eventId, s.data, "I"
// MAGIC from eventsource s left join eventtarget t
// MAGIC on s.eventId = t.eventId
// MAGIC where t.eventId is null
// MAGIC union 
// MAGIC /* unchanged and updated */
// MAGIC select s.eventId, s.data, case when md5(s.data)= md5(t.data) then t.flag else "U" end
// MAGIC from eventsource s join eventtarget t
// MAGIC on s.eventId = t.eventId
// MAGIC union
// MAGIC /* deleted */
// MAGIC select t.eventId, t.data, "D"
// MAGIC from eventsource s right join eventtarget t
// MAGIC on s.eventId = t.eventId
// MAGIC where s.eventid is null

// COMMAND ----------

