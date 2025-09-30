from pyspark.sql import SparkSession
import os


access_key = os.environ.get("AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

spark = (
    SparkSession.builder
    .appName("DadosFIFA21-PySpark-MinIo")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()

)

# Testar a conexão listando buckets (pasta raiz)
try:
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    path = spark._jvm.org.apache.hadoop.fs.Path("s3a://")
    files = fs.listStatus(path)
    print("Buckets encontrados no MinIO:")
    for f in files:
        print(f.getPath())
except Exception as e:
    print("Erro ao conectar no MinIO:", e)

spark.stop()