# tests/conftest.py

import pytest
import sqlite3
import pandas as pd
from pathlib import Path
from typing import List, Optional, Set

# Importa constantes do config
try:
    from src.config import (
        ALL_NUMBERS, NEW_BALL_COLUMNS, TABLE_NAME as SORTEIOS_TABLE_NAME,
        CHUNK_STATS_FINAL_PREFIX
    )
except ImportError:
    print("WARN: Falha ao importar do config em conftest.py")
    ALL_NUMBERS = list(range(1, 26))
    NEW_BALL_COLUMNS = [f'b{i}' for i in range(1, 16)]
    SORTEIOS_TABLE_NAME = 'sorteios'
    CHUNK_STATS_FINAL_PREFIX = 'chunk_stats_'

# Importa função de nome de tabela
from src.database_manager import get_chunk_final_stats_table_name

# --- Dados de Teste Estendidos (20 concursos) ---
TEST_DATA_SORTEIOS = [
    {'concurso': 1,  'data_sorteio': '2023-01-01', 'b1': 1,  'b2': 2,  'b3': 3,  'b4': 4,  'b5': 5,  'b6': 6,  'b7': 7,  'b8': 8,  'b9': 9,  'b10': 10, 'b11': 11, 'b12': 12, 'b13': 13, 'b14': 14, 'b15': 15},
    {'concurso': 2,  'data_sorteio': '2023-01-02', 'b1': 11, 'b2': 12, 'b3': 13, 'b4': 14, 'b5': 15, 'b6': 16, 'b7': 17, 'b8': 18, 'b9': 19, 'b10': 20, 'b11': 21, 'b12': 22, 'b13': 23, 'b14': 24, 'b15': 25},
    {'concurso': 3,  'data_sorteio': '2023-01-03', 'b1': 1,  'b2': 3,  'b3': 5,  'b4': 7,  'b5': 9,  'b6': 11, 'b7': 13, 'b8': 15, 'b9': 17, 'b10': 19, 'b11': 21, 'b12': 23, 'b13': 25, 'b14': 2,  'b15': 4},
    {'concurso': 4,  'data_sorteio': '2023-01-04', 'b1': 2,  'b2': 4,  'b3': 6,  'b4': 8,  'b5': 10, 'b6': 12, 'b7': 14, 'b8': 16, 'b9': 18, 'b10': 20, 'b11': 22, 'b12': 24, 'b13': 1,  'b14': 3,  'b15': 5},
    {'concurso': 5,  'data_sorteio': '2023-01-05', 'b1': 21, 'b2': 22, 'b3': 23, 'b4': 24, 'b5': 25, 'b6': 1,  'b7': 2,  'b8': 3,  'b9': 4,  'b10': 5,  'b11': 6,  'b12': 7,  'b13': 8,  'b14': 9,  'b15': 10},
    {'concurso': 6,  'data_sorteio': '2023-01-06', 'b1': 1,  'b2': 2,  'b3': 3,  'b4': 4,  'b5': 5,  'b6': 16, 'b7': 17, 'b8': 18, 'b9': 19, 'b10': 20, 'b11': 11, 'b12': 12, 'b13': 13, 'b14': 14, 'b15': 15},
    {'concurso': 7,  'data_sorteio': '2023-01-07', 'b1': 1,  'b2': 2,  'b3': 3,  'b4': 4,  'b5': 5,  'b6': 16, 'b7': 17, 'b8': 18, 'b9': 19, 'b10': 20, 'b11': 21, 'b12': 22, 'b13': 23, 'b14': 24, 'b15': 25},
    {'concurso': 8,  'data_sorteio': '2023-01-08', 'b1': 6,  'b2': 7,  'b3': 8,  'b4': 9,  'b5': 10, 'b6': 11, 'b7': 12, 'b8': 13, 'b9': 14, 'b10': 15, 'b11': 1,  'b12': 3,  'b13': 5,  'b14': 7,  'b15': 9}, # 13 bolas - inválido para repetição
    {'concurso': 9,  'data_sorteio': '2023-01-09', 'b1': 2,  'b2': 4,  'b3': 6,  'b4': 8,  'b5': 10, 'b6': 12, 'b7': 14, 'b8': 16, 'b9': 18, 'b10': 20, 'b11': 21, 'b12': 23, 'b13': 25, 'b14': 1,  'b15': 5},
    {'concurso': 10, 'data_sorteio': '2023-01-10', 'b1': 1,  'b2': 2,  'b3': 3,  'b4': 4,  'b5': 5,  'b6': 6,  'b7': 7,  'b8': 8,  'b9': 9,  'b10': 10, 'b11': 11, 'b12': 12, 'b13': 13, 'b14': 14, 'b15': 15},
    # Concursos 11-20
    {'concurso': 11, 'data_sorteio': '2023-01-11', 'b1': 1,  'b2': 2,  'b3': 3,  'b4': 4,  'b5': 5,  'b6': 6,  'b7': 7,  'b8': 8,  'b9': 9,  'b10': 10, 'b11': 11, 'b12': 12, 'b13': 13, 'b14': 14, 'b15': 15},
    {'concurso': 12, 'data_sorteio': '2023-01-12', 'b1': 11, 'b2': 12, 'b3': 13, 'b4': 14, 'b5': 15, 'b6': 16, 'b7': 17, 'b8': 18, 'b9': 19, 'b10': 20, 'b11': 21, 'b12': 22, 'b13': 23, 'b14': 24, 'b15': 25},
    {'concurso': 13, 'data_sorteio': '2023-01-13', 'b1': 1,  'b2': 3,  'b3': 5,  'b4': 7,  'b5': 9,  'b6': 11, 'b7': 13, 'b8': 15, 'b9': 17, 'b10': 19, 'b11': 21, 'b12': 23, 'b13': 25, 'b14': 2,  'b15': 4},
    {'concurso': 14, 'data_sorteio': '2023-01-14', 'b1': 2,  'b2': 4,  'b3': 6,  'b4': 8,  'b5': 10, 'b6': 12, 'b7': 14, 'b8': 16, 'b9': 18, 'b10': 20, 'b11': 22, 'b12': 24, 'b13': 1,  'b14': 3,  'b15': 5},
    {'concurso': 15, 'data_sorteio': '2023-01-15', 'b1': 1,  'b2': 2,  'b3': 3,  'b4': 4,  'b5': 5,  'b6': 6,  'b7': 7,  'b8': 8,  'b9': 9,  'b10': 10, 'b11': 11, 'b12': 12, 'b13': 13, 'b14': 14, 'b15': 15},
    {'concurso': 16, 'data_sorteio': '2023-01-16', 'b1': 11, 'b2': 12, 'b3': 13, 'b4': 14, 'b5': 15, 'b6': 16, 'b7': 17, 'b8': 18, 'b9': 19, 'b10': 20, 'b11': 21, 'b12': 22, 'b13': 23, 'b14': 24, 'b15': 25},
    {'concurso': 17, 'data_sorteio': '2023-01-17', 'b1': 1,  'b2': 3,  'b3': 5,  'b4': 7,  'b5': 9,  'b6': 11, 'b7': 13, 'b8': 15, 'b9': 17, 'b10': 19, 'b11': 21, 'b12': 23, 'b13': 25, 'b14': 2,  'b15': 4},
    {'concurso': 18, 'data_sorteio': '2023-01-18', 'b1': 2,  'b2': 4,  'b3': 6,  'b4': 8,  'b5': 10, 'b6': 12, 'b7': 14, 'b8': 16, 'b9': 18, 'b10': 20, 'b11': 22, 'b12': 24, 'b13': 1,  'b14': 3,  'b15': 5},
    {'concurso': 19, 'data_sorteio': '2023-01-19', 'b1': 1,  'b2': 2,  'b3': 3,  'b4': 4,  'b5': 5,  'b6': 11, 'b7': 12, 'b8': 13, 'b9': 14, 'b10': 15, 'b11': 21, 'b12': 22, 'b13': 23, 'b14': 24, 'b15': 25},
    {'concurso': 20, 'data_sorteio': '2023-01-20', 'b1': 6,  'b2': 7,  'b3': 8,  'b4': 9,  'b5': 10, 'b6': 16, 'b7': 17, 'b8': 18, 'b9': 19, 'b10': 20, 'b11': 1,  'b12': 2,  'b13': 3,  'b14': 4,  'b15': 5},
]
NUM_TEST_RECORDS = len(TEST_DATA_SORTEIOS) # Agora é 20

# Dados PRÉ-CALCULADOS para chunk_stats_10_final (Concurso 10)
EXPECTED_CHUNK_10_STATS_CONC_10 = {
    'concurso_fim': 10,
    'd1_freq': 9, 'd2_freq': 8, 'd3_freq': 8, 'd4_freq': 8, 'd5_freq': 9, 'd6_freq': 8, 'd7_freq': 7, 'd8_freq': 8, 'd9_freq': 7, 'd10_freq': 8, 'd11_freq': 6, 'd12_freq': 7, 'd13_freq': 6, 'd14_freq': 7, 'd15_freq': 6, 'd16_freq': 5, 'd17_freq': 5, 'd18_freq': 5, 'd19_freq': 5, 'd20_freq': 5, 'd21_freq': 5, 'd22_freq': 4, 'd23_freq': 5, 'd24_freq': 4, 'd25_freq': 5,
    'd1_rank': 1, 'd2_rank': 3, 'd3_rank': 3, 'd4_rank': 3, 'd5_rank': 1, 'd6_rank': 3, 'd7_rank': 9, 'd8_rank': 3, 'd9_rank': 9, 'd10_rank': 3, 'd11_rank': 13, 'd12_rank': 9, 'd13_rank': 13, 'd14_rank': 9, 'd15_rank': 13, 'd16_rank': 16, 'd17_rank': 16, 'd18_rank': 16, 'd19_rank': 16, 'd20_rank': 16, 'd21_rank': 16, 'd22_rank': 24, 'd23_rank': 16, 'd24_rank': 24, 'd25_rank': 16,
}
# Dados PRÉ-CALCULADOS para chunk_stats_10_final (Concurso 20)
EXPECTED_CHUNK_10_STATS_CONC_20 = {
    'concurso_fim': 20,
    'd1_freq': 6, 'd2_freq': 7, 'd3_freq': 7, 'd4_freq': 7, 'd5_freq': 7, 'd6_freq': 5, 'd7_freq': 5, 'd8_freq': 5, 'd9_freq': 5, 'd10_freq': 5, 'd11_freq': 6, 'd12_freq': 7, 'd13_freq': 7, 'd14_freq': 7, 'd15_freq': 7, 'd16_freq': 5, 'd17_freq': 5, 'd18_freq': 5, 'd19_freq': 5, 'd20_freq': 5, 'd21_freq': 5, 'd22_freq': 5, 'd23_freq': 5, 'd24_freq': 5, 'd25_freq': 6,
    'd1_rank': 9, 'd2_rank': 1, 'd3_rank': 1, 'd4_rank': 1, 'd5_rank': 1, 'd6_rank': 12, 'd7_rank': 12, 'd8_rank': 12, 'd9_rank': 12, 'd10_rank': 12, 'd11_rank': 9, 'd12_rank': 1, 'd13_rank': 1, 'd14_rank': 1, 'd15_rank': 1, 'd16_rank': 12, 'd17_rank': 12, 'd18_rank': 12, 'd19_rank': 12, 'd20_rank': 12, 'd21_rank': 12, 'd22_rank': 12, 'd23_rank': 12, 'd24_rank': 12, 'd25_rank': 9,
}

@pytest.fixture(scope="function")
def test_db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:"); yield conn; conn.close()

@pytest.fixture(scope="function")
def populated_db_conn(test_db_conn: sqlite3.Connection) -> sqlite3.Connection:
    """ Popula BD em memória com sorteios e stats finais dos chunks 10 e 20. """
    conn = test_db_conn; cursor = conn.cursor()
    # Cria e popula 'sorteios'
    ball_cols_schema = ', '.join([f'"{col}" INTEGER' for col in NEW_BALL_COLUMNS]); create_sorteios_sql = f"CREATE TABLE IF NOT EXISTS {SORTEIOS_TABLE_NAME} (concurso INTEGER PRIMARY KEY, data_sorteio TEXT, {ball_cols_schema});"; cursor.execute(create_sorteios_sql)
    cols_ordered_sorteios = ['concurso', 'data_sorteio'] + NEW_BALL_COLUMNS; placeholders_sorteios = ', '.join(['?'] * len(cols_ordered_sorteios)); sql_insert_sorteios = f"INSERT INTO {SORTEIOS_TABLE_NAME} ({', '.join(cols_ordered_sorteios)}) VALUES ({placeholders_sorteios})"
    for record in TEST_DATA_SORTEIOS: cursor.execute(sql_insert_sorteios, [record.get(col) for col in cols_ordered_sorteios])
    conn.commit(); print(f"\nFixture: Inseriu {len(TEST_DATA_SORTEIOS)} registros em '{SORTEIOS_TABLE_NAME}'.")

    # Cria e popula 'chunk_stats_10_final'
    interval_10 = 10; table_name_chunk_10 = get_chunk_final_stats_table_name(interval_10)
    freq_col_defs = ', '.join([f'd{i}_freq INTEGER NOT NULL' for i in ALL_NUMBERS]); rank_col_defs = ', '.join([f'd{i}_rank INTEGER NOT NULL' for i in ALL_NUMBERS])
    create_chunk_sql = f"CREATE TABLE IF NOT EXISTS {table_name_chunk_10} (concurso_fim INTEGER PRIMARY KEY, {freq_col_defs}, {rank_col_defs});"; cursor.execute(create_chunk_sql); conn.commit()
    chunk_stat_cols = ['concurso_fim'] + [f'd{i}_freq' for i in ALL_NUMBERS] + [f'd{i}_rank' for i in ALL_NUMBERS]; chunk_placeholders = ', '.join(['?'] * len(chunk_stat_cols)); sql_insert_chunk = f"INSERT OR REPLACE INTO {table_name_chunk_10} ({', '.join(chunk_stat_cols)}) VALUES ({chunk_placeholders})"
    # Insere dados pré-calculados para concurso 10 E 20
    values_conc_10 = [EXPECTED_CHUNK_10_STATS_CONC_10.get(col, 0) for col in chunk_stat_cols]; cursor.execute(sql_insert_chunk, values_conc_10)
    values_conc_20 = [EXPECTED_CHUNK_10_STATS_CONC_20.get(col, 0) for col in chunk_stat_cols]; cursor.execute(sql_insert_chunk, values_conc_20)
    conn.commit(); print(f"Fixture: Inseriu 2 registros em '{table_name_chunk_10}'.")

    return conn