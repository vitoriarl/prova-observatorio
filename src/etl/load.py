from pyspark.sql import DataFrame
import os


class DataFrameToSQLServer:
    def __init__(self):
        """
        Inicializa a conexão com o banco de dados.

        :param jdbc_driver_path: Caminho do driver JDBC necessário para a conexão.
        """
        self.user = os.getenv("USERNAME")
        self.password = os.getenv("PASSWORD")
        self.host = os.getenv("HOST")
        self.port = os.getenv("PORT")
        self.database = os.getenv("DATABASE")

        self.jdbc_url = f"jdbc:sqlserver://{self.host}:{self.port};databaseName={self.database};encrypt=false;trustServerCertificate=false"
        self.connection_properties = {
            "user": self.user,
            "password": self.password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

    def save_to_sql(self, df: DataFrame, table_name: str, mode: str = "append"):
        """
        Salva um DataFrame no SQL Server.

        :param df: DataFrame do Spark a ser salvo.
        :param table_name: Nome da tabela no SQL Server.
        :param mode: Modo de escrita ('append', 'overwrite', 'ignore', 'error').
        """
        if not isinstance(df, DataFrame):
            raise ValueError("O parâmetro df deve ser um DataFrame do Spark.")

        df.write \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .mode(mode) \
            .save()
        
        print(f"DataFrame salvo na tabela '{table_name}' com sucesso!")

