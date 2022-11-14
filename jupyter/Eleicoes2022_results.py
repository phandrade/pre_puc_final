from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w


def read_parquet(spark, folder):
    data = (
        spark
        .read
        .parquet(folder)
    )
    return data

spark = ( SparkSession.\
        builder.\
        appName("pyspark-eleicoes2022").\
#        master("spark://spark-master:7077").\
#        config("spark.executor.memory", "512m").\
        getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
parquet_folder_path = "s3://prepuceleicoes2022/parquet/eleicoes2022/"

# Dataset Completo
parquet_path_votos = parquet_folder_path+"votos/"
votosparquet = read_parquet(spark, parquet_folder_path)
votosparquet.show()

# ###     1. Número de Votos dos candidatos que foram ao segundo turno; número de votos brancos e número de votos nulos, todos por UF, no primeiro turno
parquet_folder_votosUFCandidato1t = parquet_folder_path+'votosUFCandidato1t/'
votosUFCandidato1t = read_parquet(spark, parquet_folder_votosUFCandidato1t)
votosUFCandidato1t.show()


# ###     2. Número de Votos dos candidatos que foram ao segundo turno; número de votos brancos e número de votos nulos, todos por UF, no segundo turno
parquet_folder_votosUFCandidato2t = parquet_folder_path+'votosUFCandidato2t/'
votosUFCandidato2t = read_parquet(spark, parquet_folder_votosUFCandidato2t)
votosUFCandidato2t.show()

# ###    3. Representação em % dos votos desses candidatos, dos votos brancos e dos votos nulos em cada UF (votos dos dois candidatos + votos brancos + votos nulos representam 100% dos votos de cada UF), no primeiro turno
parquet_folder_percVotosCandidatoUF1t = parquet_folder_path+'votospercVotosCandidatoUF1t/'
votospercVotosCandidatoUF1t = read_parquet(spark, parquet_folder_percVotosCandidatoUF1t)
votospercVotosCandidatoUF1t.show()

# ###    4. Representação em % dos votos desses candidatos, dos votos brancos e dos votos nulos em cada UF (votos dos dois candidatos + votos brancos + votos nulos representam 100% dos votos de cada UF), no segundo turno
parquet_folder_percVotosCandidatoUF2t = parquet_folder_path+'percVotosCandidatoUF2t/'
percVotosCandidatoUF2t = read_parquet(spark, parquet_folder_percVotosCandidatoUF2t)
percVotosCandidatoUF2t.show()

# ###    5. Diferença entre os números de votos obtidos (votos segundo turno - votos primeiro turno ) para cada um dos candidatos, para os votos nulos e para os votos brancos, por UF 
parquet_folder_difVotosUFCandidato = parquet_folder_path+'difVotosUFCandidato/'
difVotosUFCandidato = read_parquet(spark, parquet_folder_difVotosUFCandidato)
difVotosUFCandidato.show()

# ###    6. Diferença % entre os valores % de votos (% segundo turno - % primeiro turno ) para cada um dos candidatos, para os votos nulos e para os votos brancos, por UF 
parquet_folder_difPercVotosCandidatoUF = parquet_folder_path+'difPercVotosCandidatoUF/'
difPercVotosCandidatoUF = read_parquet(spark, parquet_folder_difPercVotosCandidatoUF)
difPercVotosCandidatoUF.show()

# ###    7. Junção das tabelas
parquet_folder_tabela_final = parquet_folder_path+'tabela_final/'
tabela_final = read_parquet(spark, parquet_folder_tabela_final)
tabela_final.show()