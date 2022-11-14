# # Analises Finais:

# Para candidatos que foram para o segundo turno, branco e nulos:
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

# ## Inicializar ambiente DEV

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w

spark = SparkSession.\
        builder.\
        appName("pyspark-notebook-eleicoes2022").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "512m").\
        getOrCreate()


# ## 1 ler os arquivos raw e formatar o dataset completo à partir da pasta raw/eleicoes2022

raw_folder_path = "raw/eleicoes2022"
raw_sep = ";"
raw_header = True
raw_enconding = "latin1"

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
)

# ## 2 transformar em parquet
(
    data
    .write
    .format('parquet')
    .save("parquet/eleicoes2022")
)

votosparquet = (
    spark
    .read
    #.parquet("s3://igti-ney-rais-prod-processing-zone-127012818163/rais/")
    .parquet("parquet/eleicoes2022")
)

# ### Candidatos do Segundo turno

candidatos_2t = votosparquet.select('NM_VOTAVEL').where('NR_TURNO = 2').distinct()

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

votosUFCandidato1t.show(n=1000)

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

votosUFCandidato2t.show(n=1000)

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

percVotosCandidatoUF1t.show()

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

percVotosCandidatoUF2t.show()

# ###    5. Diferença entre os números de votos obtidos (votos segundo turno - votos primeiro turno ) para cada um dos candidatos, para os votos nulos e para os votos brancos, por UF 

difVotosUFCandidato = (
    votosUFCandidato1t
        .join(votosUFCandidato2t, ['SG_UF','NM_VOTAVEL'], 'inner')
        .withColumn('Dif. VotosUFCandidato', f.col('VotosUFCandidato-2T') - f.col('VotosUFCandidato-1T'))
        #.select('SG_UF', 'NM_VOTAVEL', '`Dif. VotosUFCandidato`')
)

difVotosUFCandidato.show()

# ###    6. Diferença % entre os valores % de votos (% segundo turno - % primeiro turno ) para cada um dos candidatos, para os votos nulos e para os votos brancos, por UF 

difPercVotosCandidatoUF = (
    percVotosCandidatoUF1t
        .join(percVotosCandidatoUF2t, ['SG_UF','NM_VOTAVEL'], 'inner')
        .withColumn('Dif. % Votos UF', f.col('% Votos UF 2T') - f.col('% Votos UF 1T'))
)

(difPercVotosCandidatoUF
    .sort(f.col('`Dif. % Votos UF`').desc())
    .show()
)

# ###    7. Junção das tabelas

tabela_final = (
    difVotosUFCandidato.join(difPercVotosCandidatoUF, ['SG_UF','NM_VOTAVEL'])
)

(tabela_final
    .select('SG_UF','NM_VOTAVEL','VotosUFCandidato-1T','VotosUFCandidato-2T','`% Votos UF 1T`','`% Votos UF 2T`','`Dif. % Votos UF`','`Dif. VotosUFCandidato`')
    .show()
)