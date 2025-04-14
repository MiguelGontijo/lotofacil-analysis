# src/database_manager.py

import sqlite3
import pandas as pd
from pathlib import Path
import logging
from typing import List, Optional, Set, Tuple

# Importa APENAS o necessário do config
from src.config import DATABASE_PATH, TABLE_NAME, logger, NEW_BALL_COLUMNS
# Define ALL_NUMBERS localmente
ALL_NUMBERS: List[int] = list(range(1, 26))

# --- Funções save_to_db, read_data_from_db, get_draw_numbers ---
# (Código idêntico ao da última versão completa enviada - sem ALL_NUMBERS na importação)
def save_to_db(df: pd.DataFrame, table_name: str, db_path: Path = DATABASE_PATH, if_exists: str = 'replace') -> bool:
    logger.info(f"Salvando dados na tabela '{table_name}' (if_exists='{if_exists}')...")
    if df is None or df.empty: logger.warning(f"DataFrame para '{table_name}' vazio."); return False
    try:
        with sqlite3.connect(db_path) as conn: df.to_sql(name=table_name, con=conn, if_exists=if_exists, index=False)
        logger.info(f"{len(df)} registros salvos em '{table_name}'.")
        return True
    except Exception as e: logger.error(f"Erro ao salvar em '{table_name}': {e}"); return False

def read_data_from_db(db_path: Path = DATABASE_PATH, table_name: str = TABLE_NAME, columns: Optional[List[str]] = None, concurso_minimo: Optional[int] = None, concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    log_msg = f"Lendo '{table_name}'"; conditions = []; params = []
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
                     if col == 'concurso' or col in NEW_BALL_COLUMNS: df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            logger.info(f"{len(df)} registros lidos.")
            return df
    except Exception as e: logger.error(f"Erro ao ler '{table_name}': {e}"); return None

def get_draw_numbers(concurso: int, db_path: Path = DATABASE_PATH, table_name: str = TABLE_NAME) -> Optional[Set[int]]:
    logger.debug(f"Buscando dezenas concurso {concurso}...")
    try:
        with sqlite3.connect(db_path) as conn:
            ball_cols_str = ', '.join(f'"{col}"' for col in NEW_BALL_COLUMNS); sql_query = f'SELECT {ball_cols_str} FROM {table_name} WHERE concurso = ?'
            result = conn.execute(sql_query, (concurso,)).fetchone()
            if result:
                drawn_numbers = {int(num) for num in result if num is not None and pd.notna(num)}
                if len(drawn_numbers) == 15: return drawn_numbers
                else: logger.warning(f"Concurso {concurso} sem 15 números válidos: {result}"); return None
            else: logger.warning(f"Concurso {concurso} não encontrado."); return None
    except Exception as e: logger.error(f"Erro buscar dezenas {concurso}: {e}"); return None

# --- Funções de Ciclo (create_cycles_table, get_last_cycle_end) ---
# (Código idêntico ao da última versão)
def create_cycles_table(db_path: Path = DATABASE_PATH):
    try:
        with sqlite3.connect(db_path) as conn:
            sql_create = """CREATE TABLE IF NOT EXISTS ciclos (numero_ciclo INTEGER PRIMARY KEY, concurso_inicio INTEGER NOT NULL, concurso_fim INTEGER NOT NULL UNIQUE, duracao INTEGER NOT NULL);"""
            conn.execute(sql_create); logger.info("Tabela 'ciclos' verificada/criada.")
    except sqlite3.Error as e: logger.error(f"Erro criar/verificar 'ciclos': {e}")
def get_last_cycle_end(db_path: Path = DATABASE_PATH) -> Optional[int]:
    create_cycles_table(db_path);
    try:
        with sqlite3.connect(db_path) as conn: result = conn.execute("SELECT MAX(concurso_fim) FROM ciclos").fetchone(); return int(result[0]) if result and result[0] is not None else None
    except sqlite3.Error as e: logger.error(f"Erro buscar último fim de ciclo: {e}"); return None


# --- Funções para Snapshots de Frequência (Usando ALL_NUMBERS local) ---
FREQ_SNAP_TABLE_NAME = 'freq_geral_snap'

def create_freq_snap_table(db_path: Path = DATABASE_PATH):
    """ Cria a tabela 'freq_geral_snap' se ela não existir. """
    try:
        with sqlite3.connect(db_path) as conn:
            # Usa ALL_NUMBERS definido no início deste arquivo
            col_defs = ', '.join([f'd{i} INTEGER NOT NULL' for i in ALL_NUMBERS])
            sql_create = f"""
            CREATE TABLE IF NOT EXISTS {FREQ_SNAP_TABLE_NAME} (
                concurso_snap INTEGER PRIMARY KEY,
                {col_defs}
            );
            """
            conn.execute(sql_create)
            logger.info(f"Tabela '{FREQ_SNAP_TABLE_NAME}' verificada/criada.")
    except sqlite3.Error as e:
        logger.error(f"Erro ao criar/verificar tabela '{FREQ_SNAP_TABLE_NAME}': {e}")

def get_last_freq_snapshot_contest(db_path: Path = DATABASE_PATH) -> Optional[int]:
    """ Busca o concurso do último snapshot registrado. """
    create_freq_snap_table(db_path)
    try:
        with sqlite3.connect(db_path) as conn:
            result = conn.execute(f"SELECT MAX(concurso_snap) FROM {FREQ_SNAP_TABLE_NAME}").fetchone()
            return int(result[0]) if result and result[0] is not None else None
    except sqlite3.Error as e:
        logger.error(f"Erro ao buscar último snapshot de frequência: {e}")
        return None

def get_closest_freq_snapshot(concurso: int, db_path: Path = DATABASE_PATH) -> Optional[Tuple[int, pd.Series]]:
    """ Busca o snapshot de frequência mais recente <= ao concurso fornecido. """
    create_freq_snap_table(db_path)
    logger.debug(f"Buscando snapshot de frequência mais próximo <= {concurso}...")
    try:
        with sqlite3.connect(db_path) as conn:
            # Usa ALL_NUMBERS local
            col_names = ['concurso_snap'] + [f'd{i}' for i in ALL_NUMBERS]
            col_names_str = ', '.join(f'"{col}"' for col in col_names)
            sql = f"SELECT {col_names_str} FROM {FREQ_SNAP_TABLE_NAME} WHERE concurso_snap <= ? ORDER BY concurso_snap DESC LIMIT 1;"
            result = conn.execute(sql, (concurso,)).fetchone()

            if result:
                snap_concurso = int(result[0])
                # Usa ALL_NUMBERS local
                freq_data = {num: int(count) for num, count in zip(ALL_NUMBERS, result[1:])}
                freq_series = pd.Series(freq_data, name=f'FreqSnap_{snap_concurso}').sort_index()
                logger.debug(f"Snapshot encontrado: Concurso {snap_concurso}")
                return snap_concurso, freq_series
            else:
                logger.debug(f"Nenhum snapshot encontrado <= {concurso}.")
                return None
    except sqlite3.Error as e:
        logger.error(f"Erro ao buscar snapshot de frequência: {e}")
        return None

def save_freq_snapshot(concurso_snap: int, freq_series: pd.Series, db_path: Path = DATABASE_PATH):
    """ Salva (ou atualiza) um snapshot de frequência no banco. """
    create_freq_snap_table(db_path)
    # Usa ALL_NUMBERS local
    if freq_series is None or len(freq_series.index.intersection(ALL_NUMBERS)) != 25:
        logger.error(f"Série de frequência inválida para salvar snapshot {concurso_snap}. Índice: {freq_series.index if freq_series is not None else 'None'}")
        return

    logger.debug(f"Salvando snapshot de frequência para concurso {concurso_snap}...")
    try:
        with sqlite3.connect(db_path) as conn:
            # Usa ALL_NUMBERS local
            col_names = ['concurso_snap'] + [f'd{i}' for i in ALL_NUMBERS]
            placeholders = ', '.join(['?'] * (len(ALL_NUMBERS) + 1))
            # Garante ordem e valores padrão 0
            values_ordered = [freq_series.get(i, 0) for i in ALL_NUMBERS]
            sql_insert_replace = f"INSERT OR REPLACE INTO {FREQ_SNAP_TABLE_NAME} ({', '.join(col_names)}) VALUES ({placeholders});"
            values_to_insert = [concurso_snap] + values_ordered
            conn.execute(sql_insert_replace, values_to_insert)
            conn.commit()
            logger.debug(f"Snapshot para concurso {concurso_snap} salvo/atualizado.")
    except sqlite3.Error as e:
        logger.error(f"Erro ao salvar snapshot {concurso_snap}: {e}")