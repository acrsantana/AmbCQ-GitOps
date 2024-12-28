import logging

from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError


class DBConnection:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)

    def is_connection_active(self, engine):
        """
        Verifica se a conexão da engine ainda está ativa.

        :param engine: SQLAlchemy engine
        :return: True se a conexão estiver ativa, False caso contrário
        """
        try:
            with engine.connect() as connection:
                connection.execute("SELECT 1")
            return True
        except OperationalError:
            return False

    def getEngine(self, name_connection):
        """
        Estabelece uma conexão com o banco de dados especificado e retorna a engine do SQLAlchemy.
        Argumentos:
            name_connection (str): O nome da conexão a ser recuperada do gerenciamento de conexões do Airflow.
        Retorna:
            sqlalchemy.engine.Engine: Uma engine do SQLAlchemy conectada ao banco de dados especificado.
        Lança:
            ValueError: Se o tipo de conexão não for uma string.
            SQLAlchemyError: Se houver um erro ao conectar ao banco de dados.
        Logs:
            Info: Quando a conexão com o banco de dados é bem-sucedida.
            Error: Quando há um erro ao conectar ao banco de dados.
        """
        try:
            conn = BaseHook.get_connection(name_connection)

            type_connection = {
                "trino": "trino",
                "mysql": "mysql+mysqldb",
                "postgres": "postgresql",
            }

            if not isinstance(conn.conn_type, str):
                raise ValueError("Connection type must be a string")

            fractal_db_url = f"{type_connection[conn.conn_type]}://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
            engine = create_engine(fractal_db_url)

            if self.is_connection_active(engine):
                self.logger.info("Conexão bem-sucedida ao banco de dados")
                self.logger.debug(f"URL de conexão: {fractal_db_url}")
            else:
                self.logger.error("Erro ao conectar ao banco de dados")
                raise SQLAlchemyError("Erro ao conectar ao banco de dados")

            return engine
        except SQLAlchemyError as e:
            self.logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise
