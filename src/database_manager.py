# src/database_manager.py

import sqlite3
import pandas as pd
from pathlib import Path
import logging
from typing import List, Optional

# Importa configurações e o logger
from src.config import DATABASE_PATH, TABLE_NAME, logger, NEW_BALL_COLUMNS

def save_to_db(df: pd.DataFrame,
               table_name: str = TABLE_NAME,
               db_path: Path = DATABASE_PATH,
               if_exists: str = 'replace') -> bool:
    """ Salva um DataFrame em uma tabela do banco de dados SQLite. (Código anterior omitido para brevidade) """
    logger.info(f"Tentando salvar dados na tabela '{table_name}' em {db_path}...")
    logger.info(f"Modo 'if_exists' definido como: '{if_exists}'")

    if df is None or df.empty:
        logger.warning("DataFrame está vazio ou é None. Nada para salvar.")
        return False

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            df.to_sql(name=table_name,
                      con=conn,
                      if_exists=if_exists,
                      index=False)
            logger.info(f"{len(df)} registros salvos com sucesso na tabela '{table_name}'.")

            index_name = f"idx_{table_name}_concurso"
            sql_create_index = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (concurso);"
            cursor.execute(sql_create_index)
            logger.info(f"Índice '{index_name}' criado (ou já existente) na coluna 'concurso'.")
        return True

    except sqlite3.Error as e:
        logger.error(f"Erro SQLite ao salvar dados ou criar índice: {e}")
        return False
    except Exception as e:
        logger.error(f"Erro inesperado ao salvar dados no banco de dados: {e}")
        return False

def read_data_from_db(db_path: Path = DATABASE_PATH,
                      table_name: str = TABLE_NAME,
                      columns: Optional[List[str]] = None,
                      concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Lê dados da tabela de sorteios do banco de dados SQLite para um DataFrame.

    Args:
        db_path (Path): Caminho para o arquivo do banco de dados SQLite.
        table_name (str): Nome da tabela a ser lida.
        columns (Optional[List[str]]): Lista de colunas a serem selecionadas. Se None, seleciona todas (*).
        concurso_maximo (Optional[int]): Se fornecido, lê apenas os concursos <= a este valor.

    Returns:
        Optional[pd.DataFrame]: DataFrame com os dados lidos ou None em caso de erro.
    """
    logger.info(f"Tentando ler dados da tabela '{table_name}' em {db_path}")
    if concurso_maximo:
        logger.info(f"Filtrando por concursos <= {concurso_maximo}")

    try:
        with sqlite3.connect(db_path) as conn:
            # Constrói a query SQL
            select_cols = '*' if columns is None else ', '.join(f'"{col}"' for col in columns) # Aspas duplas para nomes com espaço, se houver
            sql_query = f"SELECT {select_cols} FROM {table_name}"
            params = []

            if concurso_maximo is not None:
                sql_query += " WHERE concurso <= ?"
                params.append(concurso_maximo)

            sql_query += " ORDER BY concurso ASC;" # Ordenar sempre para consistência

            # Lê os dados usando pandas
            df = pd.read_sql_query(sql_query, conn, params=params)

            # Tentar converter tipos após leitura do SQL (read_sql pode não preservar Int64/datas perfeitamente)
            if not df.empty:
                 if 'data_sorteio' in df.columns:
                      df['data_sorteio'] = pd.to_datetime(df['data_sorteio'], errors='coerce')
                 for col in df.columns:
                     # Se for coluna de concurso ou bola, tenta converter para Int64
                     if col == 'concurso' or col in NEW_BALL_COLUMNS:
                         df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

            logger.info(f"{len(df)} registros lidos com sucesso.")
            return df

    except sqlite3.OperationalError as e:
         logger.error(f"Erro SQLite ao ler dados: Tabela '{table_name}' existe? Erro: {e}")
         return None
    except Exception as e:
        logger.error(f"Erro inesperado ao ler dados do banco de dados: {e}")
        return None