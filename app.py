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
        file_folder = "/".join(file_key.rsplit("/")[:-1])
        dff = None
        flag = 0
        key_to_delete = []
        for index, val in enumerate(s3.Bucket(bucket).objects.filter(Prefix=file_folder + "/")):
            if index == 1:
                if str(val.key).endswith(".parquet"):
                    print(f's3://{bucket}/{val.key}')
                    df = sqlContext.read.parquet(f"s3a://{bucket}/{val.key}")
                    dff = df
            elif index != 0:
                    print(f's3===>{val.key}')
                    df = sqlContext.read.parquet(f"s3a://{bucket}/{val.key}")
                    dff=dff.unionAll(df)
            flag += 1
            key_to_delete.append(val.key)

        print(dff,"---")
        if dff:
            dff.coalesce(1).repartition(1).write.parquet("merge_parquet",mode="overwrite")
            prefixed = [filename for filename in os.listdir('merge_parquet') if filename.startswith("p")]
            print(prefixed[0],"Filename")
            print(flag,"HERE",key_to_delete)
            if flag != 2:
                s3.meta.client.upload_file(f"merge_parquet/{prefixed[0]}", bucket, f'{file_folder}/{"_".join(file_key.rsplit("/")[:-1])}_MERGE.parquet')
            import shutil
            shutil. rmtree("merge_parquet")
        if len(key_to_delete) > 1:
            for i in key_to_delete:
                print("key to delet", i)
                if not i.endswith("_MERGE.parquet"):
                    print("Delete object")
                    s3.Object(bucket, i).delete()
    
    return jsonify({
        "status": "success"
    })


if __name__ == '__main__':
   # from gevent.pywsgi import WSGIServer
    #app.debug = True 
    #http_server = WSGIServer(('0.0.0.0', 5000), app)
    #http_server.serve_forever()
    
    app.run(host='0.0.0.0')
