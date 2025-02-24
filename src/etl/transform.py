from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, count, lit, when, length, max, to_timestamp, regexp_replace, month, year
import os
from pyspark.sql import SparkSession
import os

class SparkSessionManager:
    def __init__(self):
        self.jdbc_driver_path = os.getenv("JDBC_DRIVER")
    
    def get_spark(self, app_name: str):
        self.spark = SparkSession.builder.appName(app_name).config("spark.jars", self.jdbc_driver_path).getOrCreate()
        return self.spark

class TransformData:
    def __init__(self):
        self.spark_manager = SparkSessionManager()

    def transform_atracacao_fato(self, year_file: str, folder:str):
        spark = self.spark_manager.get_spark("atracacao_fato")
        df_atracacao = spark.read.option("header", True).option("delimiter", ";").csv(f"{folder}/{year_file}Atracacao.txt")

        columns = ["Data Atracação", "Data Chegada", "Data Desatracação", "Data Início Operação", "Data Término Operação"]
        for column in columns:
            df_atracacao = df_atracacao.withColumn(column, to_timestamp(col(column), "dd/MM/yyyy HH:mm:ss"))

        df_atracacao = df_atracacao.withColumn("mes_inicio_operacao", month(col("Data Início Operação"))) \
                            .withColumn("ano_inicio_operacao", year(col("Data Início Operação")))
        
        df_atracacao = df_atracacao.select(col("IDAtracacao").cast(IntegerType()), 
                                        "CDTUP", 
                                        "IDBerco", 
                                        "Berço", 
                                        "Porto Atracação", 
                                        "Apelido Instalação Portuária", 
                                        "Complexo Portuário", 
                                        "Tipo da Autoridade Portuária", 
                                        "Data Atracação", 
                                        "Data Chegada", 
                                        "Data Desatracação", 
                                        "Data Início Operação", 
                                        "Data Término Operação", 
                                        "ano_inicio_operacao",
                                        "mes_inicio_operacao",
                                        "Tipo de Operação", 
                                        "Tipo de Navegação da Atracação", 
                                        col("Nacionalidade do Armador").cast(IntegerType()), 
                                        col("FlagMCOperacaoAtracacao").cast(IntegerType()), 
                                        "Terminal", 
                                        "Município", 
                                        "UF", 
                                        "SGUF", 
                                        "Região Geográfica", 
                                        "Nº da Capitania", 
                                        "Nº do IMO")

        df_tempo_atracacao = spark.read.option("header", True).option("delimiter", ";").csv(f"{folder}/{year_file}TemposAtracacao.txt")

        df_tempo_atracacao = df_tempo_atracacao.select(col("IDAtracacao").cast(IntegerType()).alias("IDAtracacao_tmp"), 
                                                    "TEsperaAtracacao", 
                                                    "TesperaInicioOp", 
                                                    "TOperacao", 
                                                    "TEsperaDesatracacao", 
                                                    "TAtracado", 
                                                    "TEstadia"
                                                    )

        columns = ["TEsperaAtracacao", "TesperaInicioOp", "TOperacao", "TEsperaDesatracacao", "TAtracado", "TEstadia"]

        for column in columns:
            df_tempo_atracacao = df_tempo_atracacao.withColumn(column, when(
                col(column) == "Valor Discrepante", lit(None))
                .otherwise(
                    col(column)
                    )
                ) \
            .withColumn(column, when(
                col(column) == "Zero", lit(0))
                .otherwise(
                    col(column)
                    )
                ) \
            .withColumn(column, regexp_replace(column, ',', '.'))

        df_atracacao_tempo = df_atracacao.join(df_tempo_atracacao, df_atracacao["IDAtracacao"] == df_tempo_atracacao["IDAtracacao_tmp"], "left").drop("IDAtracacao_tmp")

        df_atracacao_tempo = df_atracacao_tempo \
            .withColumnRenamed("IDAtracacao", "id_atracacao") \
            .withColumnRenamed("CDTUP", "cdtup") \
            .withColumnRenamed("IDBerco", "id_berco") \
            .withColumnRenamed("Berço", "berco") \
            .withColumnRenamed("Porto Atracação", "porto_atracacao") \
            .withColumnRenamed("Apelido Instalação Portuária", "apelido_instalação_portuaria") \
            .withColumnRenamed("Complexo Portuário", "complexo_portuario") \
            .withColumnRenamed("Tipo da Autoridade Portuária", "tipo_da_autoridade_portuaria") \
            .withColumnRenamed("Data Atracação", "data_atracacao") \
            .withColumnRenamed("Data Chegada", "data_chegada") \
            .withColumnRenamed("Data Desatracação", "data_desatracacao") \
            .withColumnRenamed("Data Início Operação", "data_inicio_operacao") \
            .withColumnRenamed("Data Término Operação", "data_termino_operacao") \
            .withColumnRenamed("Tipo de Operação", "tipo_de_operacao") \
            .withColumnRenamed("Tipo de Navegação da Atracação", "tipo_de_navegação_da_atracacao") \
            .withColumnRenamed("Nacionalidade do Armador", "nacionalidade_do_armador") \
            .withColumnRenamed("FlagMCOperacaoAtracacao", "flag_mc_operacao_atracacao") \
            .withColumnRenamed("Terminal", "terminal") \
            .withColumnRenamed("Município", "municipio") \
            .withColumnRenamed("UF", "uf") \
            .withColumnRenamed("SGUF", "sguf") \
            .withColumnRenamed("Região Geográfica", "regiao_geografica") \
            .withColumnRenamed("Nº da Capitania", "numero_capitania") \
            .withColumnRenamed("Nº do IMO", "numero_imo") \
            .withColumnRenamed("TEsperaAtracacao", "tempo_espera_atracacao") \
            .withColumnRenamed("TesperaInicioOp", "tempo_espera_inicio_operacao") \
            .withColumnRenamed("TOperacao", "tempo_operacao") \
            .withColumnRenamed("TEsperaDesatracacao", "tempo_espera_desatracacao") \
            .withColumnRenamed("TAtracado", "tempo_atracado") \
            .withColumnRenamed("TEstadia", "tempo_estadia")
        
        return df_atracacao_tempo
    
    def transform_carga_fato(self, year_file: str, folder: str):
        spark = self.spark_manager.get_spark("carga_fato")
        df_carga = spark.read.option("header", True).option("delimiter", ";").csv(f"{folder}/{year_file}Carga.txt")

        df_carga = df_carga.select(col("IDCarga").cast(IntegerType()), 
                                        "IDAtracacao", 
                                        "Origem", 
                                        "Destino",
                                        "CDMercadoria",
                                        "Tipo Operação da Carga",
                                        "Carga Geral Acondicionamento",
                                        "ConteinerEstado",
                                        "Tipo Navegação",
                                        "FlagAutorizacao",
                                        "FlagCabotagem",
                                        "FlagCabotagemMovimentacao",
                                        "FlagConteinerTamanho",
                                        "FlagTransporteViaInterioir",
                                        "Percurso Transporte em vias Interiores",
                                        "Percurso Transporte Interiores",
                                        "STNaturezaCarga",
                                        "STSH2",
                                        "STSH4",
                                        "Natureza da Carga",
                                        "Sentido",
                                        "TEU",
                                        "QTCarga",
                                        "VLPesoCargaBruta")

        df_carga_conteinerizada = spark.read.option("header", True).option("delimiter", ";").csv(f"{folder}/{year_file}Carga_Conteinerizada.txt")

        df_carga_conteinerizada = df_carga_conteinerizada.withColumnRenamed("IDCarga", "IDCarga_conteinerizada")

        df_carga = df_carga.join(df_carga_conteinerizada, df_carga["IDCarga"] == df_carga_conteinerizada["IDCarga_conteinerizada"], "left")

        df = df_carga.filter(col("IDCarga_conteinerizada").isNotNull())

        df_carga = df_carga.withColumn("CDMercadoria", when(
                col("IDCarga_conteinerizada").isNotNull(), col("CDMercadoriaConteinerizada"))
                .otherwise(
                    col("CDMercadoria")
                    )
                )

        df_carga = df_carga.withColumn("peso_liquido", when(
                col("IDCarga_conteinerizada").isNotNull(), col("VLPesoCargaConteinerizada"))
                .otherwise(
                    col("VLPesoCargaBruta")
                    )
                )

        df_atracacao = spark.read.option("header", True).option("delimiter", ";").csv(f"{folder}/{year_file}Atracacao.txt")
        df_atracacao = df_atracacao.withColumn("Data Início Operação", to_timestamp(col("Data Início Operação"), "dd/MM/yyyy HH:mm:ss"))
        df_atracacao = df_atracacao.withColumn("mes_inicio_operacao", month(col("Data Início Operação"))) \
                                    .withColumn("ano_inicio_operacao", year(col("Data Início Operação")))

        df_atracacao = df_atracacao.select(col("IDAtracacao").alias("IDAtracacao_atr"), "Porto Atracação", "SGUF", "Data Início Operação", "ano_inicio_operacao", "mes_inicio_operacao")


        df_carga = df_carga.join(df_atracacao, df_carga["IDCarga"] == df_atracacao["IDAtracacao_atr"], "left")

        df_carga = df_carga.select(col("IDCarga").cast(IntegerType()), 
                                        "IDAtracacao", 
                                        "Origem", 
                                        "Destino",
                                        "CDMercadoria",
                                        "Tipo Operação da Carga",
                                        "Carga Geral Acondicionamento",
                                        "ConteinerEstado",
                                        "Tipo Navegação",
                                        "FlagAutorizacao",
                                        "FlagCabotagem",
                                        "FlagCabotagemMovimentacao",
                                        "FlagConteinerTamanho",
                                        "FlagTransporteViaInterioir",
                                        "Percurso Transporte em vias Interiores",
                                        "Percurso Transporte Interiores",
                                        "STNaturezaCarga",
                                        "STSH2",
                                        "STSH4",
                                        "Natureza da Carga",
                                        "Sentido",
                                        "TEU",
                                        "QTCarga",
                                        "VLPesoCargaBruta",
                                        "ano_inicio_operacao",
                                        "mes_inicio_operacao", 
                                        "Porto Atracação",
                                        "SGUF",
                                        "peso_liquido")

        df_carga = df_carga \
            .withColumnRenamed("IDCarga", "id_carga") \
            .withColumnRenamed("IDAtracacao", "id_atracacao") \
            .withColumnRenamed("Origem", "origem") \
            .withColumnRenamed("Destino", "destino") \
            .withColumnRenamed("CDMercadoria", "cd_mercadoria") \
            .withColumnRenamed("Tipo Operação da Carga", "tipo_operacao_da_carga") \
            .withColumnRenamed("Carga Geral Acondicionamento", "carga_geral_acondicionamento") \
            .withColumnRenamed("ConteinerEstado", "conteiner_estado") \
            .withColumnRenamed("Tipo Navegação", "tipo_navegacao") \
            .withColumnRenamed("FlagAutorizacao", "flag_autorizacao") \
            .withColumnRenamed("FlagCabotagem", "flag_cabotagem") \
            .withColumnRenamed("FlagCabotagemMovimentacao", "flag_cabotagem_movimentacao") \
            .withColumnRenamed("FlagConteinerTamanho", "flag_conteiner_tamanho") \
            .withColumnRenamed("FlagTransporteViaInterioir", "flag_transporte_via_interioir") \
            .withColumnRenamed("Percurso Transporte em vias Interiores", "percurso_transporte_em_vias_interiores") \
            .withColumnRenamed("Percurso Transporte Interiores", "percurso_transporte_interiores") \
            .withColumnRenamed("STNaturezaCarga", "st_natureza_carga") \
            .withColumnRenamed("STSH2", "stsh2") \
            .withColumnRenamed("STSH4", "stsh4") \
            .withColumnRenamed("Natureza da Carga", "natureza_da_carga") \
            .withColumnRenamed("Sentido", "sentido") \
            .withColumnRenamed("TEU", "teu") \
            .withColumnRenamed("QTCarga", "qt_carga") \
            .withColumnRenamed("VLPesoCargaBruta", "vl_peso_carga_bruta") \
            .withColumnRenamed("ano_inicio_operacao", "ano_inicio_operacao") \
            .withColumnRenamed("mes_inicio_operacao", "mes_inicio_operacao") \
            .withColumnRenamed("Porto Atracação", "porto_atracacao") \
            .withColumnRenamed("SGUF", "sguf") \
            .withColumnRenamed("peso_liquido", "peso_liquido")
        
        return df_carga

 