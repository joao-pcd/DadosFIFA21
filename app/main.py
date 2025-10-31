from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from minio import Minio
import os
import re

access_key = os.environ.get("AWS_ACCESS_KEY_ID")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

spark = (
    SparkSession.builder
    .appName("DadosFIFA21-PySpark-MinIo")

    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .config("spark.hadoop.fs.s3a.path.style.access", "True")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5")

    .config("spark.sql.extensions", 
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")

    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v2") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://dados-fifa21/iceberg/") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

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


df = (
    spark.read
    .option("header", True)
    .option("multiLine", True)  
    .option("escape", "\"")      
    .csv("/workspace/dados/fifa21 raw data v2.csv")
    .repartition(20)
)

for coluna in df.columns:
    df = df.withColumn(coluna, regexp_replace(col(coluna), "[\r\n]", ""))

df = df.withColumnRenamed("↓OVA", "OVA")

for coluna in df.columns:
    df = df.withColumnRenamed(coluna, coluna.lower().replace('/', '_').replace(' ', '_'))

print("\nImprime os dados do DF:")
print(df.show(5)) 

print("\nImprime o schema:")
print(df.printSchema())

print("\nSalvando os arquivos como parquet na camada raw...")
df.write.format("parquet")\
        .mode("overwrite")\
        .save(f"s3a://{bucket_name}/raw/df-fifa21-parquet/")
print("\nArquivos salvos com sucesso!")

df_parquet = spark.read.parquet(f"s3a://{bucket_name}/raw/df-fifa21-parquet/")

try:
    spark.sql("CREATE NAMESPACE nessie.fifa21")
    print("Namespace nessie.fifa21 criado com sucesso.")
except Exception as e:
    # Handle the case if the namespace already exists or other errors
    if "Namespace already exists" in str(e):
        print("Namespace nessie.fifa21 já existe.")
    else:
        print(f"Erro ao criar namespace: {e}")


print("Salvando na tabela Iceberg 'nessie.fifa21.tb_sor'...")

df_parquet.writeTo("nessie.fifa21.tb_sor")\
    .createOrReplace()      # Cria ou substitui a tabela


spark.stop()