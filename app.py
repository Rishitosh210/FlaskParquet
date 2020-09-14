# initialise sparkContext
import os

import boto3
import flask
from flask import jsonify
from flask import request
from pyspark.sql.types import StructType

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = "--packages=com.amazonaws:aws-java-sdk-bundle:1.11.271,org.apache.hadoop:hadoop-aws:3.1.2 pyspark-shell"

import findspark

findspark.init()

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

app = flask.Flask(__name__)
app.config["DEBUG"] = True

spark = SparkSession.builder \
    .master('local') \
    .appName('ParquetName') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", 'AKIAIWLQIZBK2CMZ6IUQ')
spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", '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')
sc = spark.sparkContext

sqlContext = SQLContext(sc)

s3 = boto3.resource("s3")


@app.route('/api/parquet', methods=['POST'])
def api_all():
    if request.method == 'POST':
        file_key = request.data.get('file_key')
        schema = StructType([])
        file_folder = "/".join(file_key.rsplit("/")[:-1])
        output_data = sqlContext.createDataFrame(sc.emptyRDD(), schema)
        for index, val in enumerate(s3.Bucket("layers3parqut").objects.filter(Prefix=file_folder + "/")):
            if index != 0:
                if str(val.key).endswith(".parquet"):
                    print(f's3://layers3parqut/{val.key}')
                    df1 = sqlContext.read.parquet("s3a://layers3parqut/ubx.dev.mcpidevint1+3+0000622214-snappy.parquet")
                    # df2 = sqlContext.read.parquet("s3a://layers3parqut/ubx-dev-mcpidevint1+2+0000619666-snappy.parquet")
                    output_data.union(df1)

        output_data.toPandas().to_parquet(
            f's3://layers3parqut//{file_folder}/{"_".join(file_key.rsplit("/")[:-1])}_MERGE.parquet')

    return jsonify({
        "status": "success"
    })


if __name__ == '__main__':
    app.run()
