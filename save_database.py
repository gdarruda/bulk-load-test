import logging
from mysql import get_engine as get_engine_mysql
from postgres import get_engine as get_engine_postgres
from pyspark.sql import SparkSession

packages = [
      "org.postgresql:postgresql:42.5.1",
      "com.mysql:mysql-connector-j:8.0.31",
      "org.mongodb.spark:mongo-spark-connector:10.0.5"
]

spark = (SparkSession.builder 
           .appName("Populating databases") 
           .config("spark.jars.packages", ",".join(packages))
           .config("spark.mongodb.write.connection.uri", "mongodb://root:example@127.0.0.1")
           .getOrCreate())

def prepare_database(conn):
      logging.info("Droping table...")
      conn.execute('DROP TABLE IF EXISTS predicts')

      logging.info("Creating table...")
      conn.execute('''CREATE TABLE predicts 
                  (id_client varchar(36),
                  id_class smallint,
                  predict_value float)''')

def create_index(conn):
      logging.info("Adding keys...")
      conn.execute('''ALTER TABLE predicts ADD PRIMARY KEY (id_client, id_class)''')

def insert_database(url: str,
                    driver: str,
                    user: str,
                    password: str):

      logging.info("Saving dataframe...")
      (spark.read
            .parquet("normalized.parquet")
            .write
            .format("jdbc")
            .option("url", url)
            .option("driver", driver)
            .option("dbtable", 'predicts')
            .option("user", user)
            .option("password", password)
            .option("rewriteBatchedStatements", "true")
            .mode("append")
            .save())

conn_mysql = get_engine_mysql()
conn_postgres = get_engine_postgres()

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(message)s')

logging.info("Testing Postgres...")
prepare_database(conn_postgres)
insert_database("jdbc:postgresql://localhost:5432/postgres",
                "org.postgresql.Driver",
                "postgres",
                "example")
create_index(conn_postgres)

logging.info("Testing MySQL...")
prepare_database(conn_mysql)
insert_database("jdbc:mysql://localhost:3306/sample",
                "com.mysql.cj.jdbc.Driver",
                "root",
                "example")
create_index(conn_mysql)

logging.info("Testing MongoDB...")

(
      spark
         .read
         .parquet("inline.parquet")
         .withColumnRenamed("id_client", "_id")
         .write
         .format("mongodb")
         .mode("overwrite")
         .option("database", "mongo")
         .option("collection", "predicts")
         .option("writeConcern.journal", "false")
         .save()
)

logging.info("Finished!")
spark.stop()