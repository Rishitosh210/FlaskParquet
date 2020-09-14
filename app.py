# initialise sparkContext
import os

import boto3
import flask
from flask import jsonify
from flask import request
from pyspark.sql.types import StructType



from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

app = flask.Flask(__name__)
app.config["DEBUG"] = True


s3 = boto3.resource("s3")


@app.route('/api/parquet', methods=['POST'])
def api_all():
    import findspark

    findspark.init()
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=com.amazonaws:aws-java-sdk-bundle:1.11.271,org.apache.hadoop:hadoop-aws:3.1.2 pyspark-shell"
    spark = SparkSession.builder \
        .master('local') \
        .appName('ParquetName') \
        .config('spark.executor.memory', '5gb') \
        .config("spark.cores.max", "6") \
        .getOrCreate()

    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", str(os.getenv("AWS_ACCESS_KEY_ID")))
    spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", str(os.getenv("AWS_SECRET_ACCESS_KEY")))
    sc = spark.sparkContext

    sqlContext = SQLContext(sc)
    bucket = "mytest3838"
    print(bucket)
    if request.method == 'POST':
        print(request.json)
        file_key = request.json.get('file_key')
        #schema = StructType([])
        file_folder = "/".join(file_key.rsplit("/")[:-1])
        #output_data = sqlContext.createDataFrame(sc.emptyRDD(), schema)
        dff = None
        for index, val in enumerate(s3.Bucket(bucket).objects.filter(Prefix=file_folder + "/")):
            if index == 1:
                if str(val.key).endswith(".parquet"):
                    print(f's3://{bucket}/{val.key}')
                    df = sqlContext.read.parquet(f"s3a://{bucket}/{val.key}")
                    dff = df
            elif index != 0:
               # df2 = sqlContext.read.parquet("s3a://layers3parqut/ubx-dev-mcpidevint1+2+0000619666-snappy.parquet")
                    df = sqlContext.read.parquet(f"s3a://{bucket}/{val.key}")
                    dff=dff.unionAll(df)
                    #output_data.union(df1)
        print(dff,"---")
        dff.toPandas().to_parquet(
            f's3://{bucket}//{file_folder}/{"_".join(file_key.rsplit("/")[:-1])}_MERGE.parquet')

    return jsonify({
        "status": "success"
    })


if __name__ == '__main__':
   # from gevent.pywsgi import WSGIServer
    #app.debug = True 
    #http_server = WSGIServer(('0.0.0.0', 5000), app)
    #http_server.serve_forever()
    
    app.run(host='0.0.0.0')
