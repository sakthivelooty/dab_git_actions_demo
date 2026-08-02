# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists spark_kafka_projects;
# MAGIC create schema if not exists spark_kafka_projects.wikidata;
# MAGIC create volume if not exists spark_kafka_projects.wikidata.wikimedia_checkpoint;
# MAGIC use spark_kafka_projects.wikidata;

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests

# COMMAND ----------

master_payload_template = """
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "meta": {"uri": "", "request_id": "", "id": "", "domain": "", "stream": "", "dt": "", "topic": "", "partition": 0, "offset": 0},
  "id": 0,
  "type": "edit",
  "namespace": 0,
  "title": "",
  "title_url": "",
  "comment": "",
  "timestamp": 0,
  "user": "",
  "bot": true,
  "log_id": 0,
  "log_type": "",
  "log_action": "",
  "log_params": {
    "action": "",
    "filter": "",
    "actions": "",
    "log": 0
  },
  "log_action_comment": "",
  "notify_url": "",
  "minor": false,
  "patrolled": true,
  "server_url": "",
  "server_name": "",
  "server_script_path": "",
  "wiki": "",
  "parsedcomment": "",
  "length": {"old": 0, "new": 0},
  "revision": {"old": 0, "new": 0}
}
"""

dynamic_schema = schema_of_json(lit(master_payload_template))

# COMMAND ----------

kafka_wikidata_checkpoint = "/Volumes/spark_kafka_projects/wikidata/wikimedia_checkpoint/enriched_data"

# COMMAND ----------

kafka_options = {
    "kafka.bootstrap.servers": "pkc-7prvp.centralindia.azure.confluent.cloud:9092",
    "subscribe": 'wikimedia_analisys',
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="WPCWVW2LEOXHIQFP" password="cfltOnBkKguREeo9GNvIXOXKSDrCoeFXI3ERqeIsI6re0ssj03v+b4UwjiFE68CA";'
    )
}

# COMMAND ----------

kafka_stream_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# COMMAND ----------

# display(kafka_stream_df, checkpointLocation="/Volumes/spark_kafka_projects/wikidata/wikimedia_checkpoint/testing")

# COMMAND ----------

parsed_kafka_df = (
    kafka_stream_df
    .selectExpr("cast(key as STRING) as message_key", "cast(value as STRING) as json_payload")
    .withColumn("parsed_json_value", from_json(col("json_payload"), dynamic_schema))
    .select("message_key", "parsed_json_value.*")
    .withColumn("qid",regexp_extract(col("title_url"), r"Q\d+", 0)) 
)

# COMMAND ----------

display(parsed_kafka_df, checkpointLocation="/Volumes/spark_kafka_projects/wikidata/wikimedia_checkpoint/testing/topic_55")

# COMMAND ----------

meta_schema = StructType([
    StructField("qid", StringType(), True),
    StructField("label", StringType(), True),
    StructField("desc", StringType(), True)
])


# COMMAND ----------

def fetch_qid_data(qids):
    url = f"https://www.wikidata.org/w/api.php?action=wbgetentities&ids={'|'.join(qids)}&languages=en&format=json"
    header ={
                'User-Agent': "WikidataBatchFetcher/1.0 (yourmailid@examole.com)"
            }
    output = {}
    try:
        res = requests.get(url, headers=header, timeout=10)
        if res.status_code == 200:
            for qid, ent in res.json().get("entities",{}).items():
                label = ent.get("labels",{}).get('en',{}).get('value',"")
                desc = ent.get("descriptions",{}).get('en',{}).get('value',"")
                output[qid] = (label, desc)
            return output
        else:
            return output
    except Exception as e:
        return output


# COMMAND ----------

def enrich_data(iterator):
    cache = {}
    for pdf in iterator:
        missing = [q for q in pdf['qid'].unique() if q not in cache]
        if missing:
            for i in range(0, len(missing), 50):
                chunk = missing[i:i+50]
                try:
                    cache.update(fetch_qid_data(chunk))
                except Exception as e:
                    cache.update({q : (str(e), None, None) for q in chunk})
        
        pdf["label"] = pdf['qid'].map(lambda q: cache.get(q, (None, None))[0])
        pdf["desc"] = pdf['qid'].map(lambda q: cache.get(q, (None, None))[1])

        yield pdf[["qid", "label", "desc"]]

        

# COMMAND ----------

def process_and_enrich_data(microdf, batchid):
    
    if microdf.isEmpty():
        return
    
    #microdf.cache() # no chache is available in serverless compute

    # get unique qids
    df_qids_unique = (microdf
                      .select("qid")
                      .where(col("qid").isNotNull() & (col("qid") != ""))
                      .distinct())
    
    if df_qids_unique.count() > 0:
        meta_df = (
            df_qids_unique
            .mapInPandas(enrich_data, schema=meta_schema)
        )

        final_enriched_df = microdf.join(meta_df, on="qid", how="left")
    else:
        final_enriched_df = (
            microdf
            .withColumn("label", lit(None).cast(StringType()))
            .withColumn("desc", lit(None).cast(StringType()))
        )
    
    (
        final_enriched_df
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("spark_kafka_projects.wikidata.wikimedia_data")
    )


# COMMAND ----------

wikidata_enriched = (
    parsed_kafka_df
    .writeStream
    .foreachBatch(process_and_enrich_data)
    .option("checkpointLocation", kafka_wikidata_checkpoint)
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spark_kafka_projects.wikidata.wikimedia_data;

# COMMAND ----------


