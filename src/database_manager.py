# src/database_manager.py

import sqlite3
import pandas as pd
from pathlib import Path
import logging
from typing import List, Optional, Set

from src.config import DATABASE_PATH, TABLE_NAME, logger, NEW_BALL_COLUMNS

# --- Funções de Leitura (read_data_from_db, get_draw_numbers, get_last_cycle_end) ---
# (Código idêntico ao da última versão completa enviada)
def read_data_from_db(db_path: Path = DATABASE_PATH, table_name: str = TABLE_NAME, columns: Optional[List[str]] = None, concurso_minimo: Optional[int] = None, concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    log_msg = f"Lendo dados da tabela '{table_name}' em {db_path}"
    conditions = []; params = []
    if concurso_minimo is not None: conditions.append("concurso >= ?"); params.append(concurso_minimo); log_msg += f" >= {concurso_minimo}"
    if concurso_maximo is not None: conditions.append("concurso <= ?"); params.append(concurso_maximo); log_msg += f" <= {concurso_maximo}"
    logger.info(log_msg)
    try:
        with sqlite3.connect(db_path) as conn:
            select_cols = '*' if columns is None else ', '.join(f'"{col}"' for col in columns); sql_query = f"SELECT {select_cols} FROM {table_name}"
            if conditions: sql_query += " WHERE " + " AND ".join(conditions)
            sql_query += " ORDER BY concurso ASC;"
            df = pd.read_sql_query(sql_query, conn, params=params)
            if not df.empty:
                 if 'data_sorteio' in df.columns: df['data_sorteio'] = pd.to_datetime(df['data_sorteio'], errors='coerce')
                 for col in df.columns:
                     # Usar is_integer_dtype ou similar pode ser mais robusto
                     if col == 'concurso' or col in NEW_BALL_COLUMNS: df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            logger.info(f"{len(df)} registros lidos.")
            return df
    except Exception as e: logger.error(f"Erro ao ler '{table_name}' do BD: {e}"); return None

def get_draw_numbers(concurso: int, db_path: Path = DATABASE_PATH, table_name: str = TABLE_NAME) -> Optional[Set[int]]:
    logger.debug(f"Buscando dezenas do concurso {concurso}...")
    try:
        with sqlite3.connect(db_path) as conn:
            ball_cols_str = ', '.join(f'"{col}"' for col in NEW_BALL_COLUMNS); sql_query = f'SELECT {ball_cols_str} FROM {table_name} WHERE concurso = ?'
            result = conn.execute(sql_query, (concurso,)).fetchone()
            if result:
                drawn_numbers = {int(num) for num in result if num is not None and pd.notna(num)}
                if len(drawn_numbers) == 15: return drawn_numbers
                else: logger.warning(f"Concurso {concurso} não tem 15 números válidos: {result}"); return None
            else: logger.warning(f"Concurso {concurso} não encontrado."); return None
    except Exception as e: logger.error(f"Erro ao buscar dezenas {concurso}: {e}"); return None

def create_cycles_table(db_path: Path = DATABASE_PATH):
    """ Cria a tabela 'ciclos' se ela não existir. """
    try:
        with sqlite3.connect(db_path) as conn:
            sql_create = """
            CREATE TABLE IF NOT EXISTS ciclos (
                numero_ciclo INTEGER PRIMARY KEY,
                concurso_inicio INTEGER NOT NULL,
                concurso_fim INTEGER NOT NULL UNIQUE,
                duracao INTEGER NOT NULL
            ); """
            conn.execute(sql_create)
            # Índice agora é criado na função de update após salvar
            logger.info("Tabela 'ciclos' verificada/criada.")
    except sqlite3.Error as e: logger.error(f"Erro ao criar/verificar 'ciclos': {e}")

def get_last_cycle_end(db_path: Path = DATABASE_PATH) -> Optional[int]:
    """ Busca o concurso_fim do último ciclo registrado na tabela 'ciclos'. """
    create_cycles_table(db_path)
    try:
        with sqlite3.connect(db_path) as conn:
            result = conn.execute("SELECT MAX(concurso_fim) FROM ciclos").fetchone()
            return int(result[0]) if result and result[0] is not None else None
    except sqlite3.Error as e: logger.error(f"Erro ao buscar último fim de ciclo: {e}"); return None


# --- FUNÇÃO save_to_db MODIFICADA ---
def save_to_db(df: pd.DataFrame,
               table_name: str, # Tornar nome da tabela obrigatório
               db_path: Path = DATABASE_PATH,
               if_exists: str = 'replace' # replace, append, fail
               ) -> bool:
    """
    Salva um DataFrame em uma tabela do banco de dados SQLite.
    NÃO cria mais índices automaticamente.
    """
    logger.info(f"Salvando dados na tabela '{table_name}' em {db_path} (if_exists='{if_exists}')...")
    if df is None or df.empty:
        logger.warning(f"DataFrame para '{table_name}' vazio. Nada a salvar.")
        return False
    try:
        with sqlite3.connect(db_path) as conn:
            df.to_sql(name=table_name, con=conn, if_exists=if_exists, index=False)
            logger.info(f"{len(df)} registros salvos em '{table_name}'.")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar em '{table_name}': {e}")
        return False