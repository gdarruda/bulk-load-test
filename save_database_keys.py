import logging
from mysql import get_engine as get_engine_mysql
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import row_number,lit
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

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

def create_database(conn):


    logging.info("Droping table predicts...")
    conn.execute('DROP TABLE IF EXISTS predicts')

    logging.info("Creating table predicts...")
    conn.execute('''CREATE TABLE predicts 
                    (id_client varchar(36),
                    id_class smallint,
                    predict_value float)''')

    logging.info("Droping table predicts...")
    conn.execute('DROP TABLE IF EXISTS classes')
    
    logging.info("Creating table classes...")
    conn.execute('''CREATE TABLE classes 
                    (id_class smallint,
                    name text)''')

def create_index(conn):

      logging.info("Adding primary key predicts...")
      conn.execute('''ALTER TABLE predicts ADD PRIMARY KEY (id_client, id_class)''')

      logging.info("Adding primary key classes...")
      conn.execute('''ALTER TABLE classes ADD PRIMARY KEY (id_class)''')

      logging.info("Adding foreign key from predicts to classes...")
      conn.execute('''ALTER TABLE predicts ADD FOREIGN KEY (id_class) REFERENCES classes(id_class)''')

def insert_database(df: DataFrame,
                    url: str,
                    driver: str,
                    user: str,
                    password: str,
                    table: str):

      logging.info("Saving dataframe...")
      (df
            .write
            .format("jdbc")
            .option("url", url)
            .option("driver", driver)
            .option("dbtable", table)
            .option("user", user)
            .option("password", password)
            .option("rewriteBatchedStatements", "true")
            .option("batchsize", 10_000)
            .mode("append")
            .save())

conn_mysql = get_engine_mysql()

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(message)s')

logging.info("Creting classes table MySQL...")

window = Window().orderBy(lit('dummy'))

raw_file = (spark
    .read
    .parquet("normalized.parquet")
    .cache())

# Create class table from raw file
classes = (raw_file
    .select("class_name")
    .distinct()
    .withColumn("id_class", row_number().over(window))
    .cache())

# Making the final table from the original classes
predicts = (raw_file.alias("rf")
                .join(broadcast(classes.alias('c')),
                      classes.class_name == raw_file.class_name)
                .select("rf.id_client",
                        "c.id_class",
                        "rf.predict_value"))

logging.info("Creting classes table MySQL...")
create_database(conn_mysql)

logging.info("Inserting classes in the table...")
insert_database(classes.withColumnRenamed("class_name", "name"),
                "jdbc:mysql://localhost:3306/sample",
                "com.mysql.cj.jdbc.Driver",
                "root",
                "example",
                "classes")

logging.info("Inserting predicts in the table...")
insert_database(predicts,
                "jdbc:mysql://localhost:3306/sample",
                "com.mysql.cj.jdbc.Driver",
                "root",
                "example",
                "predicts")

create_index(conn_mysql)

logging.info("Finished!")
spark.stop()