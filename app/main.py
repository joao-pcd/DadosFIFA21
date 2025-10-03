from pyspark.sql import SparkSession
from minio import Minio
import os
import kagglehub
import re

access_key = os.environ.get("AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

spark = (
    SparkSession.builder
    .appName("DadosFIFA21-PySpark-MinIo")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "True")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.6,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.508")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

def parse_time_to_ms(value):
    if isinstance(value, str):
        match = re.match(r"(\d+)([smh])$", value.strip())
        if match:
            number, unit = match.groups()
            number = int(number)
            if unit == "s":  # segundos
                return str(number * 1000)
            elif unit == "m":  # minutos
                return str(number * 60 * 1000)
            elif unit == "h":  # horas
                return str(number * 60 * 60 * 1000)
    return value

conf = spark._jsc.hadoopConfiguration()
for item in conf.iterator():
    key = item.getKey()
    value = item.getValue()
    if "s3a" in key.lower():
        new_value = parse_time_to_ms(value)
        if new_value != value:
            print(f"Corrigindo {key}: {value} -> {new_value}")
            conf.set(key, new_value)


# Conectar ao MinIO
client = Minio(
    "minio:9000",
    access_key=access_key,
    secret_key=secret_key,
    secure=False
)

bucket_name = "dados-fifa21"

# Cria o bucket se não existir
found = client.bucket_exists(bucket_name)
if not found:
    client.make_bucket(bucket_name)
    print("\nBucket criado ",bucket_name)
else:
    print("\nBucket", bucket_name, "já existe")


print("\nListando todos os buckets encontrados:")
buckets = client.list_buckets()
for bucket in buckets:
    print("Bucket name: ", bucket.name)


# Download latest version
path = kagglehub.dataset_download("yagunnersya/fifa-21-messy-raw-dataset-for-cleaning-exploring")

print("\nPath to dataset files:", path)

df = spark.read.format("csv")\
    .option("header", "True")\
    .option("inferSchema","True")\
    .csv(path)

print("\nImprime os dados do Kaggle:")
print(df.show(5))

print("\nImprime o schema:")
print(df.printSchema())

print("\nSalvando os arquivos como parquet na camada SOR...")
df.write.format("parquet")\
        .mode("overwrite")\
        .save("s3a://"+bucket_name+"/sor/df-fifa21-parquet-file.parquet")
print("\nArquivos salvos com sucesso!")

spark.stop()