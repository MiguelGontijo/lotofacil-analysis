# src/database_manager.py
import sqlite3
import pandas as pd
from pathlib import Path
import logging
from typing import List, Optional

# Importar as constantes de configuração relevantes do config.py
# DATABASE_PATH não existe; usaremos DATA_DIR e DB_FILE_NAME para construir o caminho.
from src.config import (
    DB_FILE_NAME,       # Nome do arquivo do banco de dados (ex: "lotofacil_analysis.db")
    DATA_DIR            # Diretório onde o arquivo do BD será armazenado (ex: BASE_DIR / "Data")
)

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Gerencia todas as interações com o banco de dados SQLite.
    """
    def __init__(self, db_path: Optional[str] = None):
        """
        Inicializa o DatabaseManager.

        Args:
            db_path: Caminho completo para o arquivo do banco de dados.
                     Se None, constrói o caminho a partir de DATA_DIR e DB_FILE_NAME
                     definidos em src.config.
        """
        if db_path:
            self.db_path = Path(db_path)
            logger.info(f"DatabaseManager usando db_path fornecido: {self.db_path}")
        else:
            # Constrói o caminho do banco de dados usando DATA_DIR e DB_FILE_NAME de config.py
            if DATA_DIR and DB_FILE_NAME:
                self.db_path = DATA_DIR / DB_FILE_NAME
                logger.info(f"Nenhum db_path explícito fornecido. Usando caminho construído do config: {self.db_path}")
            else:
                # Fallback crítico se as constantes não estiverem devidamente configuradas em config.py
                # Isso não deveria acontecer se config.py estiver correto.
                default_fallback_path = Path("lotofacil_default_critical_error.db")
                logger.error(
                    f"DATA_DIR ou DB_FILE_NAME não estão configurados corretamente em src.config.py "
                    f"e nenhum db_path foi fornecido ao DatabaseManager. "
                    f"Usando fallback crítico: {default_fallback_path}"
                )
                self.db_path = default_fallback_path
                # Poderia também levantar um ValueError aqui para forçar a correção da configuração:
                # raise ValueError("Caminho do banco de dados não pôde ser determinado devido à falta de DATA_DIR ou DB_FILE_NAME em config.py.")

        self._ensure_db_directory_exists()
        logger.info(f"DatabaseManager inicializado. Banco de dados em: {self.db_path}")

    def _ensure_db_directory_exists(self):
        """
        Garante que o diretório pai do arquivo de banco de dados exista.
        """
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Diretório do banco de dados '{self.db_path.parent}' verificado/criado.")
        except Exception as e:
            logger.error(f"Erro ao tentar criar o diretório do banco de dados '{self.db_path.parent}': {e}", exc_info=True)
            # Dependendo da criticidade, pode-se levantar o erro aqui.

    def get_connection(self) -> sqlite3.Connection:
        """
        Retorna uma nova conexão com o banco de dados.
        O chamador é responsável por fechar a conexão.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            logger.debug(f"Conexão com o banco de dados '{self.db_path}' estabelecida.")
            return conn
        except sqlite3.Error as e:
            logger.error(f"Erro ao conectar ao banco de dados '{self.db_path}': {e}", exc_info=True)
            raise  # Propaga o erro para que o chamador possa lidar com ele

    def save_dataframe_to_db(self, df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
        """
        Salva um DataFrame do Pandas em uma tabela no banco de dados SQLite.

        Args:
            df: DataFrame a ser salvo.
            table_name: Nome da tabela onde o DataFrame será salvo.
            if_exists: Comportamento se a tabela já existir ('replace', 'append', 'fail').
                       Padrão é 'replace'.
        """
        if df.empty and if_exists == 'replace':
            logger.warning(f"Tentando salvar um DataFrame vazio na tabela '{table_name}' com if_exists='replace'. "
                           f"Se a tabela existir, ela será substituída por uma tabela vazia (ou nenhuma tabela, dependendo do driver). "
                           f"Para evitar isso, não chame save_dataframe_to_db com DataFrames vazios para if_exists='replace'.")
            # Opcionalmente, pode-se optar por não fazer nada aqui ou apenas dropar a tabela se ela existir.
            # Por enquanto, vamos permitir que o pandas lide com isso, mas o log é importante.
        
        logger.info(f"Salvando DataFrame na tabela '{table_name}' (modo: {if_exists}). DataFrame com {len(df)} linhas.")
        try:
            with self.get_connection() as conn:
                df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            logger.info(f"DataFrame salvo com sucesso na tabela '{table_name}'.")
        except sqlite3.Error as e:
            logger.error(f"Erro SQLite ao salvar DataFrame na tabela '{table_name}': {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Erro inesperado ao salvar DataFrame na tabela '{table_name}': {e}", exc_info=True)

    def load_dataframe_from_db(self, table_name: str) -> Optional[pd.DataFrame]:
        """
        Carrega uma tabela do banco de dados SQLite para um DataFrame do Pandas.

        Args:
            table_name: Nome da tabela a ser carregada.

        Returns:
            DataFrame com os dados da tabela, ou None se a tabela não existir ou ocorrer um erro.
        """
        logger.info(f"Carregando dados da tabela '{table_name}'.")
        if not self.table_exists(table_name):
            logger.warning(f"Tabela '{table_name}' não encontrada no banco de dados '{self.db_path}'.")
            return None # Ou retornar um DataFrame vazio: pd.DataFrame()
        
        try:
            with self.get_connection() as conn:
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            logger.info(f"Dados da tabela '{table_name}' carregados com sucesso. DataFrame com {len(df)} linhas.")
            return df
        except sqlite3.Error as e:
            logger.error(f"Erro SQLite ao carregar dados da tabela '{table_name}': {e}", exc_info=True)
            return None # Ou retornar um DataFrame vazio
        except Exception as e:
            logger.error(f"Erro inesperado ao carregar dados da tabela '{table_name}': {e}", exc_info=True)
            return None # Ou retornar um DataFrame vazio

    def table_exists(self, table_name: str) -> bool:
        """
        Verifica se uma tabela existe no banco de dados.

        Args:
            table_name: Nome da tabela a ser verificada.

        Returns:
            True se a tabela existir, False caso contrário.
        """
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;"
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (table_name,))
                result = cursor.fetchone()
            return result is not None
        except sqlite3.Error as e:
            logger.error(f"Erro SQLite ao verificar a existência da tabela '{table_name}': {e}", exc_info=True)
            return False # Assumir que não existe em caso de erro de consulta
        except Exception as e:
            logger.error(f"Erro inesperado ao verificar a existência da tabela '{table_name}': {e}", exc_info=True)
            return False

    def get_table_names(self) -> List[str]:
        """
        Retorna uma lista com os nomes de todas as tabelas no banco de dados.
        """
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                tables = [row[0] for row in cursor.fetchall()]
            # Filtra tabelas internas do SQLite, se desejado (ex: 'sqlite_sequence')
            tables = [table for table in tables if not table.startswith('sqlite_')]
            logger.debug(f"Tabelas encontradas no banco de dados: {tables}")
            return tables
        except sqlite3.Error as e:
            logger.error(f"Erro SQLite ao obter nomes das tabelas: {e}", exc_info=True)
            return []
        except Exception as e:
            logger.error(f"Erro inesperado ao obter nomes das tabelas: {e}", exc_info=True)
            return []

    def execute_query(self, query: str, params: Optional[tuple] = None, fetch_one: bool = False, fetch_all: bool = False):
        """
        Executa uma consulta SQL genérica.

        Args:
            query: A string da consulta SQL.
            params: Tupla de parâmetros para a consulta (opcional).
            fetch_one: Se True, retorna o primeiro resultado.
            fetch_all: Se True, retorna todos os resultados.

        Returns:
            Resultado da consulta (dependendo de fetch_one/fetch_all) ou None.
        """
        logger.debug(f"Executando query: {query} com params: {params}")
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params or ())
                conn.commit() # Commit para DML (INSERT, UPDATE, DELETE) ou DDL
                
                if fetch_one:
                    return cursor.fetchone()
                if fetch_all:
                    return cursor.fetchall()
                # Para INSERT, UPDATE, DELETE, pode-se retornar cursor.rowcount
                if cursor.description is None: # Sem resultados para SELECT (ex: INSERT, UPDATE, DELETE)
                    return cursor.rowcount 
                return None # Por padrão, se não for fetch_one ou fetch_all

        except sqlite3.Error as e:
            logger.error(f"Erro SQLite ao executar query '{query}': {e}", exc_info=True)
            # Não propaga o erro aqui para permitir que o fluxo continue, mas loga.
            # Dependendo do caso de uso, pode ser melhor propagar (raise).
            return None 
        except Exception as e:
            logger.error(f"Erro inesperado ao executar query '{query}': {e}", exc_info=True)
            return None