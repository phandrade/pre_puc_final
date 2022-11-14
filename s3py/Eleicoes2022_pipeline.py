# # Analises Finais:

# Para candidatos à presidencia que foram para o segundo turno, branco e nulos:
# 
#     1. Número de Votos dos candidatos que foram ao segundo turno; número de votos brancos e número de votos nulos, todos por UF, no primeiro turno
#     2. Número de Votos dos candidatos que foram ao segundo turno; número de votos brancos e número de votos nulos, todos por UF, no segundo turno
#     
#     3. Representação em % dos votos desses candidatos, dos votos brancos e dos votos nulos em cada UF (votos dos dois candidatos + votos brancos + votos nulos representam 100% dos votos de cada UF), no primeiro turno
#     4. Representação em % dos votos desses candidatos, dos votos brancos e dos votos nulos em cada UF (votos dos dois candidatos + votos brancos + votos nulos representam 100% dos votos de cada UF), no primeiro turno
#     
#     5. Diferença entre os números de votos obtidos (votos segundo turno - votos primeiro turno ) para cada um dos candidatos, para os votos nulos e para os votos brancos, por UF 
#     6. Diferença % entre os valores % de votos (% segundo turno - % primeiro turno ) para cada um dos candidatos, para os votos nulos e para os votos brancos, por UF 
#    
#    Versão anterior:
#     1. % de votos por turno por UF para os candidatos presidência no primeiro turno
#     2. diferença % entre primeiro e segundo turno de cada candidato  que foi para o segundo turno por UF
#     3. diferença % abstenção primeiro e segundo turno por cada UF
# 

# Codigos de cargos
# +-----------------+-----------------+
# |CD_CARGO_PERGUNTA|DS_CARGO_PERGUNTA|
# +-----------------+-----------------+
# |                5|          Senador|
# |                6| Deputado Federal|
# |                3|       Governador|
# |                7|Deputado Estadual|
# |                1|       Presidente|
# +-----------------+-----------------+

# ## Inicializar ambiente DEV

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w


def write_parquet(df, folder, mode):
    data = (
        df
        .write
        .mode(mode)
        .format('parquet')
        .save(folder)
    )
    return data

def read_parquet(sp, folder):
    spark = (
        sp
        .read
        .parquet(folder)
    )
    return spark


overwrite_opt = True

def get_writemode (overwrite:bool = False ):
    if overwrite == False:
        writemode = 'ignore'
    if overwrite == True:
        writemode = 'overwrite'
    return writemode


spark = ( SparkSession.\
        builder.\
        appName("pyspark-eleicoes2022").\
#        master("spark://spark-master:7077").\
#        config("spark.executor.memory", "512m").\
        getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")

# ## 1 ler os arquivos raw e formatar o dataset completo à partir da pasta raw/eleicoes2022

raw_folder_path = "s3://prepuceleicoes2022/raw/"
raw_sep = ";"
raw_header = True
raw_enconding = "latin1"

parquet_folder_path = "s3://prepuceleicoes2022/parquet/eleicoes2022/"

data = spark.read.option("encoding", raw_enconding).csv(path=raw_folder_path, sep=raw_sep, header=raw_header)

data = (
    data
        .select(["ANO_ELEICAO", "NR_TURNO", "SG_UF", "NM_MUNICIPIO", 
                     "NR_ZONA", "NR_SECAO", "NR_PARTIDO", "SG_PARTIDO", 
                     "QT_APTOS", "QT_COMPARECIMENTO", "QT_ABSTENCOES", 
                     "NM_VOTAVEL", "QT_VOTOS", "QT_ELEITORES_BIOMETRIA_NH"])
        
        .withColumn("QT_APTOS", f.col("QT_APTOS").cast("int"))
        .withColumn("QT_COMPARECIMENTO", f.col("QT_COMPARECIMENTO").cast("int"))
        .withColumn("QT_ABSTENCOES", f.col("QT_ABSTENCOES").cast("int"))
        .withColumn("QT_VOTOS", f.col("QT_VOTOS").cast("int"))
        .withColumn("QT_ELEITORES_BIOMETRIA_NH", f.col("QT_ELEITORES_BIOMETRIA_NH").cast("int"))
        .where ("CD_CARGO_PERGUNTA = 1")
)

# ## 2 transformar em parquet

write_parquet(data, parquet_folder_path, get_writemode(overwrite_opt))
# (
#     data
#     .write
#     .format('parquet')
#     .save(parquet_folder_path)
# )

votosparquet = read_parquet(spark, parquet_folder_path)
# votosparquet = (
#     spark
#     .read
#     #.parquet("s3://igti-ney-rais-prod-processing-zone-127012818163/rais/")
#     .parquet(parquet_folder_path)
# )

# ### Candidatos do Segundo turno

candidatos_2t = votosparquet.select('NM_VOTAVEL').where('NR_TURNO = 2').distinct()
parquet_folder_candidatos_2t = parquet_folder_path+'candidatos_2t/'
write_parquet(candidatos_2t, parquet_folder_candidatos_2t, get_writemode(overwrite_opt))
#read_parquet(spark, parquet_folder_candidatos_2t)



# ###     1. Número de Votos dos candidatos que foram ao segundo turno; número de votos brancos e número de votos nulos, todos por UF, no primeiro turno

votosUFCandidato1t = (
             votosparquet
                 .groupBy("NR_TURNO", "SG_UF", "NM_VOTAVEL")
                 .agg(
                    f.sum("QT_VOTOS").cast("int").alias("VotosUFCandidato-1T"),
                 )
                 .where ('NR_TURNO = 1')
            )

votosUFCandidato1t = (
    votosUFCandidato1t.join(candidatos_2t, ['NM_VOTAVEL'], "leftsemi")
    .select('SG_UF', 'NM_VOTAVEL', 'VotosUFCandidato-1T' )
)

parquet_folder_votosUFCandidato1t = parquet_folder_path+'votosUFCandidato1t/'
write_parquet(votosUFCandidato1t, parquet_folder_votosUFCandidato1t, get_writemode(overwrite_opt))
#read_parquet(spark, parquet_folder_votosUFCandidato1t)
#votosUFCandidato1t.show(n=1000)

# ###     2. Número de Votos dos candidatos que foram ao segundo turno; número de votos brancos e número de votos nulos, todos por UF, no segundo turno

votosUFCandidato2t = (
             votosparquet
                 .groupBy("NR_TURNO", "SG_UF", "NM_VOTAVEL")
                 .agg(
                    f.sum("QT_VOTOS").cast("int").alias("VotosUFCandidato-2T"),
                 )
                 .where ('NR_TURNO = 2')
                 .select('SG_UF', 'NM_VOTAVEL', 'VotosUFCandidato-2T' )
            )
parquet_folder_votosUFCandidato2t = parquet_folder_path+'votosUFCandidato2t/'
write_parquet(votosUFCandidato2t, parquet_folder_votosUFCandidato2t, get_writemode(overwrite_opt))
#read_parquet(spark, parquet_folder_votosUFCandidato2t)
#votosUFCandidato2t.show(n=1000)

# ###    3. Representação em % dos votos desses candidatos, dos votos brancos e dos votos nulos em cada UF (votos dos dois candidatos + votos brancos + votos nulos representam 100% dos votos de cada UF), no primeiro turno

totalVotosUF1t = (
             votosUFCandidato1t
                 .groupBy("SG_UF")
                 .agg(
                    f.sum("VotosUFCandidato-1T").alias("TotalVotosUF1t")
                 )
            )

percVotosCandidatoUF1t = (
    votosUFCandidato1t
        .join(totalVotosUF1t, ['SG_UF'] , "inner")
        .withColumn('% Votos UF 1T', ((f.col('VotosUFCandidato-1T') / f.col('TotalVotosUF1t') ) * 100).cast('decimal(5,2)'))
        .select('SG_UF', 'NM_VOTAVEL', '`% Votos UF 1T`')
    
)

parquet_folder_percVotosCandidatoUF1t = parquet_folder_path+'votospercVotosCandidatoUF1t/'
write_parquet(percVotosCandidatoUF1t, parquet_folder_percVotosCandidatoUF1t, get_writemode(overwrite_opt))
#read_parquet(spark, parquet_folder_percVotosCandidatoUF1t)
#percVotosCandidatoUF1t.show()

# ###    4. Representação em % dos votos desses candidatos, dos votos brancos e dos votos nulos em cada UF (votos dos dois candidatos + votos brancos + votos nulos representam 100% dos votos de cada UF), no segundo turno

totalVotosUF2t = (
             votosUFCandidato2t
                 .groupBy("SG_UF")
                 .agg(
                    f.sum("VotosUFCandidato-2T").alias("TotalVotosUF2t")
                 )
            )

percVotosCandidatoUF2t = (
    votosUFCandidato2t
        .join(totalVotosUF2t, ['SG_UF'] , "inner")
        .withColumn('% Votos UF 2T', ((f.col('VotosUFCandidato-2T') / f.col('TotalVotosUF2t') ) * 100).cast('decimal(5,2)'))
        .select('SG_UF', 'NM_VOTAVEL', '`% Votos UF 2T`')
)

parquet_folder_percVotosCandidatoUF2t = parquet_folder_path+'percVotosCandidatoUF2t/'
write_parquet(percVotosCandidatoUF2t, parquet_folder_percVotosCandidatoUF2t, get_writemode(overwrite_opt))
#read_parquet(spark, parquet_folder_percVotosCandidatoUF2t)
#percVotosCandidatoUF2t.show()

# ###    5. Diferença entre os números de votos obtidos (votos segundo turno - votos primeiro turno ) para cada um dos candidatos, para os votos nulos e para os votos brancos, por UF 

difVotosUFCandidato = (
    votosUFCandidato1t
        .join(votosUFCandidato2t, ['SG_UF','NM_VOTAVEL'], 'inner')
        .withColumn('Dif. VotosUFCandidato', f.col('VotosUFCandidato-2T') - f.col('VotosUFCandidato-1T'))
        #.select('SG_UF', 'NM_VOTAVEL', '`Dif. VotosUFCandidato`')
)

parquet_folder_difVotosUFCandidato = parquet_folder_path+'difVotosUFCandidato/'
write_parquet(difVotosUFCandidato, parquet_folder_difVotosUFCandidato, get_writemode(overwrite_opt))
#read_parquet(spark, parquet_folder_difVotosUFCandidato)
#difVotosUFCandidato.show()

# ###    6. Diferença % entre os valores % de votos (% segundo turno - % primeiro turno ) para cada um dos candidatos, para os votos nulos e para os votos brancos, por UF 

difPercVotosCandidatoUF = (
    percVotosCandidatoUF1t
        .join(percVotosCandidatoUF2t, ['SG_UF','NM_VOTAVEL'], 'inner')
        .withColumn('Dif. % Votos UF', f.col('% Votos UF 2T') - f.col('% Votos UF 1T'))
)



parquet_folder_difPercVotosCandidatoUF = parquet_folder_path+'difPercVotosCandidatoUF/'
write_parquet(difPercVotosCandidatoUF, parquet_folder_difPercVotosCandidatoUF, get_writemode(overwrite_opt))
#read_parquet(spark, parquet_folder_difPercVotosCandidatoUF)
# (difPercVotosCandidatoUF
#     .sort(f.col('`Dif. % Votos UF`').desc())
#     .show()
# )

# ###    7. Junção das tabelas

tabela_final = (
    difVotosUFCandidato.join(difPercVotosCandidatoUF, ['SG_UF','NM_VOTAVEL'])
)

tabela_final = (
    tabela_final
    .select('SG_UF','NM_VOTAVEL','VotosUFCandidato-1T','VotosUFCandidato-2T','`% Votos UF 1T`','`% Votos UF 2T`','`Dif. % Votos UF`','`Dif. VotosUFCandidato`')
)

parquet_folder_tabela_final = parquet_folder_path+'tabela_final/'
write_parquet(tabela_final, parquet_folder_tabela_final, 'ignore')
#read_parquet(spark, parquet_folder_tabela_final)