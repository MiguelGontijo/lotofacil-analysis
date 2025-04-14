# src/database_manager.py

import sqlite3
import pandas as pd
from pathlib import Path
import logging
from typing import List, Optional, Set # Adicionado Set

from src.config import DATABASE_PATH, TABLE_NAME, logger, NEW_BALL_COLUMNS

# save_to_db continua igual...
def save_to_db(df: pd.DataFrame, table_name: str = TABLE_NAME, db_path: Path = DATABASE_PATH, if_exists: str = 'replace') -> bool:
    logger.info(f"Salvando dados na tabela '{table_name}' em {db_path} (if_exists='{if_exists}')...")
    if df is None or df.empty: return False
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            df.to_sql(name=table_name, con=conn, if_exists=if_exists, index=False)
            logger.info(f"{len(df)} registros salvos.")
            index_name = f"idx_{table_name}_concurso"
            sql_create_index = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} (concurso);"
            cursor.execute(sql_create_index)
            logger.info(f"Índice '{index_name}' ok.")
        return True
    except Exception as e: logger.error(f"Erro ao salvar no BD: {e}"); return False


# read_data_from_db continua igual...
def read_data_from_db(db_path: Path = DATABASE_PATH, table_name: str = TABLE_NAME, columns: Optional[List[str]] = None, concurso_minimo: Optional[int] = None, concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    log_msg = f"Lendo dados da tabela '{table_name}' em {db_path}"
    conditions = []
    params = []
    if concurso_minimo is not None: conditions.append("concurso >= ?"); params.append(concurso_minimo); log_msg += f" >= {concurso_minimo}"
    if concurso_maximo is not None: conditions.append("concurso <= ?"); params.append(concurso_maximo); log_msg += f" <= {concurso_maximo}"
    logger.info(log_msg)
    try:
        with sqlite3.connect(db_path) as conn:
            select_cols = '*' if columns is None else ', '.join(f'"{col}"' for col in columns)
            sql_query = f"SELECT {select_cols} FROM {table_name}"
            if conditions: sql_query += " WHERE " + " AND ".join(conditions)
            sql_query += " ORDER BY concurso ASC;"
            df = pd.read_sql_query(sql_query, conn, params=params)
            if not df.empty:
                 if 'data_sorteio' in df.columns: df['data_sorteio'] = pd.to_datetime(df['data_sorteio'], errors='coerce')
                 for col in df.columns:
                     if col == 'concurso' or col in NEW_BALL_COLUMNS: df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            logger.info(f"{len(df)} registros lidos.")
            return df
    except Exception as e: logger.error(f"Erro ao ler do BD: {e}"); return None


# --- NOVA FUNÇÃO ---
def get_draw_numbers(concurso: int, db_path: Path = DATABASE_PATH, table_name: str = TABLE_NAME) -> Optional[Set[int]]:
    """
    Busca as dezenas sorteadas para um concurso específico.

    Args:
        concurso (int): O número do concurso a ser buscado.
        db_path (Path): Caminho para o banco de dados.
        table_name (str): Nome da tabela de sorteios.

    Returns:
        Optional[Set[int]]: Um conjunto com as 15 dezenas sorteadas, ou None se não encontrado/erro.
    """
    logger.debug(f"Buscando dezenas do concurso {concurso}...")
    try:
        with sqlite3.connect(db_path) as conn:
            # Seleciona apenas as colunas de bolas
            ball_cols_str = ', '.join(f'"{col}"' for col in NEW_BALL_COLUMNS)
            sql_query = f'SELECT {ball_cols_str} FROM {table_name} WHERE concurso = ?'
            cursor = conn.cursor()
            cursor.execute(sql_query, (concurso,))
            result = cursor.fetchone() # Pega a primeira (e única) linha

            if result:
                # Converte o resultado (tupla) em um conjunto de inteiros, ignorando Nones/NAs
                drawn_numbers = {int(num) for num in result if num is not None and pd.notna(num)}
                if len(drawn_numbers) == 15:
                    logger.debug(f"Dezenas encontradas para concurso {concurso}: {drawn_numbers}")
                    return drawn_numbers
                else:
                    logger.warning(f"Concurso {concurso} encontrado, mas não continha 15 números válidos no BD: {result}")
                    return None
            else:
                logger.warning(f"Concurso {concurso} não encontrado no banco de dados.")
                return None
    except Exception as e:
        logger.error(f"Erro ao buscar dezenas do concurso {concurso}: {e}")
        return None