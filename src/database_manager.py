# src/database_manager.py

import sqlite3
import pandas as pd
from pathlib import Path
import logging
from typing import List, Optional, Set, Tuple
import sys

# Importa do config
from src.config import (
    DATABASE_PATH, TABLE_NAME, CYCLES_TABLE_NAME, FREQ_SNAP_TABLE_NAME,
    FREQ_CHUNK_DETAIL_PREFIX,
    logger, NEW_BALL_COLUMNS, ALL_NUMBERS
)

# Fallback
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))

# --- Funções save_to_db, read_data_from_db, get_draw_numbers ---
def save_to_db(df: pd.DataFrame, table_name: str, db_path: Path = DATABASE_PATH, if_exists: str = 'replace') -> bool:
    logger.info(f"Salvando dados '{table_name}' (if_exists='{if_exists}')...")
    if df is None or df.empty: logger.warning(f"DataFrame '{table_name}' vazio."); return False
    try:
        with sqlite3.connect(db_path) as conn: df.to_sql(name=table_name, con=conn, if_exists=if_exists, index=False)
        logger.info(f"{len(df)} registros salvos em '{table_name}'."); return True
    except Exception as e: logger.error(f"Erro salvar '{table_name}': {e}"); return False

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
            logger.info(f"{len(df)} registros lidos."); return df
    except Exception as e: logger.error(f"Erro ao ler '{table_name}': {e}"); return None

def get_draw_numbers(concurso: int, db_path: Path = DATABASE_PATH, table_name: str = TABLE_NAME) -> Optional[Set[int]]:
    logger.debug(f"Buscando dezenas concurso {concurso}...")
    try:
        with sqlite3.connect(db_path) as conn:
            ball_cols_str = ', '.join(f'"{col}"' for col in NEW_BALL_COLUMNS); sql_query = f'SELECT {ball_cols_str} FROM {table_name} WHERE concurso = ?'
            result = conn.execute(sql_query, (concurso,)).fetchone()
            if result: drawn_numbers = {int(num) for num in result if num is not None and pd.notna(num)}; return drawn_numbers if len(drawn_numbers) == 15 else None
            else: return None
    except Exception as e: logger.error(f"Erro buscar dezenas {concurso}: {e}"); return None

# --- Funções de Ciclo ---
# <<< FUNÇÃO CORRIGIDA >>>
def create_cycles_table(db_path: Path = DATABASE_PATH):
    """ Cria a tabela 'ciclos' com schema completo se não existir. """
    try:
        with sqlite3.connect(db_path) as conn:
            sql_create = f"""
            CREATE TABLE IF NOT EXISTS {CYCLES_TABLE_NAME} (
                numero_ciclo INTEGER PRIMARY KEY,
                concurso_inicio INTEGER NOT NULL,
                concurso_fim INTEGER NOT NULL UNIQUE,
                duracao INTEGER NOT NULL
            ); """ # Schema completo
            conn.execute(sql_create)
            logger.info(f"Tabela '{CYCLES_TABLE_NAME}' verificada/criada.")
    except sqlite3.Error as e:
        logger.error(f"Erro ao criar/verificar '{CYCLES_TABLE_NAME}': {e}")

def get_last_cycle_end(db_path: Path = DATABASE_PATH) -> Optional[int]:
    # (Código idêntico ao da última versão)
    create_cycles_table(db_path); cycles_table_name = CYCLES_TABLE_NAME
    try:
        with sqlite3.connect(db_path) as conn: result = conn.execute(f"SELECT MAX(concurso_fim) FROM {cycles_table_name}").fetchone(); return int(result[0]) if result and result[0] is not None else None
    except sqlite3.Error as e: logger.error(f"Erro buscar último fim de ciclo: {e}"); return None

# --- Funções para Snapshots de Frequência ---
# (Código idêntico ao da última versão correta)
def create_freq_snap_table(db_path: Path = DATABASE_PATH):
    try:
        with sqlite3.connect(db_path) as conn: col_defs = ', '.join([f'd{i} INTEGER NOT NULL' for i in ALL_NUMBERS]); sql_create = f"""CREATE TABLE IF NOT EXISTS {FREQ_SNAP_TABLE_NAME} (concurso_snap INTEGER PRIMARY KEY, {col_defs});"""; conn.execute(sql_create); logger.info(f"Tabela '{FREQ_SNAP_TABLE_NAME}' ok.")
    except sqlite3.Error as e: logger.error(f"Erro criar/verificar '{FREQ_SNAP_TABLE_NAME}': {e}")
def get_last_freq_snapshot_contest(db_path: Path = DATABASE_PATH) -> Optional[int]:
    create_freq_snap_table(db_path);
    try:
        with sqlite3.connect(db_path) as conn: result = conn.execute(f"SELECT MAX(concurso_snap) FROM {FREQ_SNAP_TABLE_NAME}").fetchone(); return int(result[0]) if result and result[0] is not None else None
    except sqlite3.Error as e: logger.error(f"Erro buscar último snapshot freq: {e}"); return None
def get_closest_freq_snapshot(concurso: int, db_path: Path = DATABASE_PATH) -> Optional[Tuple[int, pd.Series]]:
    create_freq_snap_table(db_path); logger.debug(f"Buscando snapshot freq <= {concurso}...")
    try:
        with sqlite3.connect(db_path) as conn:
            col_names = ['concurso_snap'] + [f'd{i}' for i in ALL_NUMBERS]; col_names_str = ', '.join(f'"{col}"' for col in col_names); sql = f"SELECT {col_names_str} FROM {FREQ_SNAP_TABLE_NAME} WHERE concurso_snap <= ? ORDER BY concurso_snap DESC LIMIT 1;"
            result = conn.execute(sql, (concurso,)).fetchone()
            if result:
                snap_concurso = int(result[0]); freq_data = {}
                for i, count_val in enumerate(result[1:]):
                    dezena = ALL_NUMBERS[i];
                    try:
                        if isinstance(count_val, int): freq_data[dezena] = count_val
                        elif isinstance(count_val, bytes): freq_data[dezena] = int.from_bytes(count_val, byteorder=sys.byteorder, signed=False);
                        else: freq_data[dezena] = 0 if count_val is None or pd.isna(count_val) else int(count_val)
                    except Exception as e: logger.error(f"Erro converter snap d{dezena}, c={snap_concurso}: {e}. Usando 0."); freq_data[dezena] = 0
                freq_series = pd.Series(freq_data).reindex(ALL_NUMBERS, fill_value=0).astype(int); logger.debug(f"Snapshot encontrado: {snap_concurso}"); return snap_concurso, freq_series
            else: logger.debug(f"Nenhum snapshot <= {concurso}."); return None
    except Exception as e: logger.error(f"Erro buscar snapshot freq: {e}"); return None
def save_freq_snapshot(concurso_snap: int, freq_series: pd.Series, db_path: Path = DATABASE_PATH):
    create_freq_snap_table(db_path);
    if freq_series is None or len(freq_series.index.intersection(ALL_NUMBERS)) != 25: logger.error(f"Série freq inválida p/ snap {concurso_snap}."); return
    logger.debug(f"Salvando snapshot freq c={concurso_snap}...")
    try:
        with sqlite3.connect(db_path) as conn:
            col_names = ['concurso_snap'] + [f'd{i}' for i in ALL_NUMBERS]; placeholders = ', '.join(['?'] * (len(ALL_NUMBERS) + 1)); values_ordered = [freq_series.get(i, 0) for i in ALL_NUMBERS]
            sql_insert_replace = f"INSERT OR REPLACE INTO {FREQ_SNAP_TABLE_NAME} ({', '.join(col_names)}) VALUES ({placeholders});"; values_to_insert = [concurso_snap] + values_ordered
            conn.execute(sql_insert_replace, values_to_insert); conn.commit(); logger.debug(f"Snapshot {concurso_snap} salvo/atualizado.")
    except Exception as e: logger.error(f"Erro salvar snapshot {concurso_snap}: {e}")

# --- Funções para Tabelas de Chunk Detalhadas ---
def get_chunk_table_name(interval_size: int) -> str:
    # (Código idêntico ao da última versão)
    return f"{FREQ_CHUNK_DETAIL_PREFIX}{interval_size}_detail"
def create_chunk_freq_table(interval_size: int, db_path: Path = DATABASE_PATH):
    # (Código idêntico ao da última versão)
    table_name = get_chunk_table_name(interval_size)
    try:
        with sqlite3.connect(db_path) as conn: col_defs = ', '.join([f'd{i} INTEGER NOT NULL' for i in ALL_NUMBERS]); sql_create = f"""CREATE TABLE IF NOT EXISTS {table_name} (concurso INTEGER PRIMARY KEY, {col_defs});"""; conn.execute(sql_create); logger.info(f"Tabela '{table_name}' verificada/criada.")
    except sqlite3.Error as e: logger.error(f"Erro criar/verificar '{table_name}': {e}")
def get_last_contest_in_chunk_table(interval_size: int, db_path: Path = DATABASE_PATH) -> Optional[int]:
    # (Código idêntico ao da última versão)
    table_name = get_chunk_table_name(interval_size); create_chunk_freq_table(interval_size, db_path)
    try:
        with sqlite3.connect(db_path) as conn: result = conn.execute(f"SELECT MAX(concurso) FROM {table_name}").fetchone(); return int(result[0]) if result and result[0] is not None else None
    except sqlite3.Error as e: logger.error(f"Erro buscar último concurso em '{table_name}': {e}"); return None
def save_chunk_freq_row(concurso: int, freq_counts_series: pd.Series, interval_size: int, db_path: Path = DATABASE_PATH):
    # (Código idêntico ao da última versão)
    table_name = get_chunk_table_name(interval_size); create_chunk_freq_table(interval_size, db_path)
    if freq_counts_series is None or len(freq_counts_series.index.intersection(ALL_NUMBERS)) != 25: logger.error(f"Série freq inválida p/ chunk {interval_size}, c={concurso}."); return
    logger.debug(f"Salvando freq chunk {interval_size} c={concurso}...")
    try:
        with sqlite3.connect(db_path) as conn:
            col_names = ['concurso'] + [f'd{i}' for i in ALL_NUMBERS]; placeholders = ', '.join(['?'] * (len(ALL_NUMBERS) + 1)); values_ordered = [freq_counts_series.get(i, 0) for i in ALL_NUMBERS]
            sql = f"INSERT OR REPLACE INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders});"; values = [concurso] + values_ordered
            conn.execute(sql, values); conn.commit()
    except Exception as e: logger.error(f"Erro salvar chunk {interval_size} c={concurso}: {e}")
def get_chunk_freq_row(concurso: int, interval_size: int, db_path: Path = DATABASE_PATH) -> Optional[pd.Series]:
    # (Código idêntico ao da última versão)
    table_name = get_chunk_table_name(interval_size); logger.debug(f"Lendo freq chunk {interval_size} c={concurso}...")
    try:
        with sqlite3.connect(db_path) as conn:
            col_names = [f'd{i}' for i in ALL_NUMBERS]; sql = f"SELECT {', '.join(col_names)} FROM {table_name} WHERE concurso = ?"
            result = conn.execute(sql, (concurso,)).fetchone()
            if result: freq_data = {num: int(count or 0) for num, count in zip(ALL_NUMBERS, result)}; return pd.Series(freq_data).astype(int)
            else: return None
    except sqlite3.Error as e: logger.error(f"Erro ler chunk {interval_size} c={concurso}: {e}"); return None