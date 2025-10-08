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

print("\nImprime o schema raiz:")
print(df.printSchema())

for coluna in df.columns:
    nova_coluna = coluna.lower()
    if " " in coluna or "&" in coluna or "/" in coluna or "↓" in coluna:
        nova_coluna = nova_coluna.replace(" ","_").replace("&","and").replace("/","_").replace("↓","")
    df = df.withColumnRenamed(coluna, nova_coluna)

print("\nImprime o schema formatado:")
print(df.printSchema())

print("\nSalvando os arquivos como parquet na camada raw...")
df.write.format("parquet")\
        .mode("overwrite")\
        .save(f"s3a://{bucket_name}/raw/df-fifa21-parquet-file.parquet")
print("\nArquivos salvos com sucesso!")

df_parquet = spark.read.parquet(f"s3a://{bucket_name}/raw/df-fifa21-parquet-file.parquet")
df_parquet.createOrReplaceTempView("tb_fifa21_raw")

try:
    spark.sql("CREATE NAMESPACE nessie.fifa21")
    print("Namespace nessie.fifa21 criado com sucesso.")
except Exception as e:
    # Handle the case if the namespace already exists or other errors
    if "Namespace already exists" in str(e):
        print("Namespace nessie.fifa21 já existe.")
    else:
        print(f"Erro ao criar namespace: {e}")

create_query = f"""
    CREATE TABLE nessie.fifa21.tb_sor
    USING iceberg
    PARTITIONED BY (team_and_contract) 
    AS
    SELECT
        photourl,
        longname,
        playerurl,
        nationality,
        positions,
        name,
        age,
        ova,
        pot,
        team_and_contract,
        id,
        height,
        weight,
        foot,
        bov,
        bp,
        growth,
        joined,
        loan_date_end,
        value,
        wage,
        release_clause,
        attacking,
        crossing,
        finishing,
        heading_accuracy,
        short_passing,
        volleys,
        skill,
        dribbling,
        curve,
        fk_accuracy,
        long_passing,
        ball_control,
        movement,
        acceleration,
        sprint_speed,
        agility,
        reactions,
        balance,
        power,
        shot_power,
        jumping,
        stamina,
        strength,
        long_shots,
        mentality,
        aggression,
        interceptions,
        positioning,
        vision,
        penalties,
        composure,
        defending,
        marking,
        standing_tackle,
        sliding_tackle,
        goalkeeping,
        gk_diving,
        gk_handling,
        gk_kicking,
        gk_positioning,
        gk_reflexes,
        total_stats,
        base_stats,
        w_f,
        sm,
        a_w,
        d_w,
        ir,
        pac,
        sho,
        pas,
        dri,
        def,
        phy,
        hits
    FROM tb_fifa21_raw
"""
spark.sql(create_query)


# Código para sobrescrever (usado em todas as execuções)
overwrite_query = f"""
    INSERT OVERWRITE nessie.fifa21.tb_sor
    SELECT
        photourl,
        longname,
        playerurl,
        nationality,
        positions,
        name,
        age,
        ova,
        pot,
        team_and_contract,
        id,
        height,
        weight,
        foot,
        bov,
        bp,
        growth,
        joined,
        loan_date_end,
        value,
        wage,
        release_clause,
        attacking,
        crossing,
        finishing,
        heading_accuracy,
        short_passing,
        volleys,
        skill,
        dribbling,
        curve,
        fk_accuracy,
        long_passing,
        ball_control,
        movement,
        acceleration,
        sprint_speed,
        agility,
        reactions,
        balance,
        power,
        shot_power,
        jumping,
        stamina,
        strength,
        long_shots,
        mentality,
        aggression,
        interceptions,
        positioning,
        vision,
        penalties,
        composure,
        defending,
        marking,
        standing_tackle,
        sliding_tackle,
        goalkeeping,
        gk_diving,
        gk_handling,
        gk_kicking,
        gk_positioning,
        gk_reflexes,
        total_stats,
        base_stats,
        w_f,
        sm,
        a_w,
        d_w,
        ir,
        pac,
        sho,
        pas,
        dri,
        def,
        phy,
        hits
    FROM tb_fifa21_raw
"""
df = spark.sql(overwrite_query)

spark.stop()