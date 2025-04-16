# tests/conftest.py

import pytest
import sqlite3
import pandas as pd
from pathlib import Path
from typing import List, Optional, Set # Adicionado Optional, Set

# Importa constantes do config (garantir que nomes estão corretos)
try:
    from src.config import (
        ALL_NUMBERS, NEW_BALL_COLUMNS, TABLE_NAME as SORTEIOS_TABLE_NAME,
        FREQ_CHUNK_DETAIL_PREFIX
    )
except ImportError: # Fallbacks se config não estiver acessível ou completo
    print("WARN: Falha ao importar do config em conftest.py")
    ALL_NUMBERS = list(range(1, 26))
    NEW_BALL_COLUMNS = [f'b{i}' for i in range(1, 16)]
    SORTEIOS_TABLE_NAME = 'sorteios'
    FREQ_CHUNK_DETAIL_PREFIX = 'freq_chunk_'

# Importa funções de DB para criar tabela
# É melhor evitar importar funções que usam o logger global do config aqui
# Vamos redefinir a criação da tabela de chunk aqui de forma segura
from src.database_manager import get_chunk_table_name

# Dados de teste para sorteios (mantidos como antes)
TEST_DATA_SORTEIOS = [
    {'concurso': 1, 'data_sorteio': '2023-01-01', 'b1': 1,  'b2': 2,  'b3': 3,  'b4': 4,  'b5': 5,  'b6': 6,  'b7': 7,  'b8': 8,  'b9': 9,  'b10': 10, 'b11': 11, 'b12': 12, 'b13': 13, 'b14': 14, 'b15': 15},
    {'concurso': 2, 'data_sorteio': '2023-01-02', 'b1': 11, 'b2': 12, 'b3': 13, 'b4': 14, 'b5': 15, 'b6': 16, 'b7': 17, 'b8': 18, 'b9': 19, 'b10': 20, 'b11': 21, 'b12': 22, 'b13': 23, 'b14': 24, 'b15': 25},
    {'concurso': 3, 'data_sorteio': '2023-01-03', 'b1': 1,  'b2': 3,  'b3': 5,  'b4': 7,  'b5': 9,  'b6': 11, 'b7': 13, 'b8': 15, 'b9': 17, 'b10': 19, 'b11': 21, 'b12': 23, 'b13': 25, 'b14': 2,  'b15': 4},
    {'concurso': 4, 'data_sorteio': '2023-01-04', 'b1': 2,  'b2': 4,  'b3': 6,  'b4': 8,  'b5': 10, 'b6': 12, 'b7': 14, 'b8': 16, 'b9': 18, 'b10': 20, 'b11': 22, 'b12': 24, 'b13': 1,  'b14': 3,  'b15': 5},
    {'concurso': 5, 'data_sorteio': '2023-01-05', 'b1': 21, 'b2': 22, 'b3': 23, 'b4': 24, 'b5': 25, 'b6': 1,  'b7': 2,  'b8': 3,  'b9': 4,  'b10': 5,  'b11': 6,  'b12': 7,  'b13': 8,  'b14': 9,  'b15': 10},
    {'concurso': 6, 'data_sorteio': '2023-01-06', 'b1': 1, 'b2': 2, 'b3': 3, 'b4': 4, 'b5': 5, 'b6': 16, 'b7': 17, 'b8': 18, 'b9': 19, 'b10': 20, 'b11': 11, 'b12': 12, 'b13': 13, 'b14': 14, 'b15': 15},
]

@pytest.fixture(scope="function")
def test_db_conn() -> sqlite3.Connection: # Adicionado type hint
    """ Pytest fixture para criar um banco de dados SQLite em memória. """
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()

@pytest.fixture(scope="function")
def populated_db_conn(test_db_conn: sqlite3.Connection) -> sqlite3.Connection: # Adicionado type hint
    """ Popula o BD em memória com dados de sorteios e cria tabela chunk 10 vazia. """
    conn = test_db_conn
    cursor = conn.cursor()

    # --- Cria e popula tabela 'sorteios' ---
    ball_cols_schema = ', '.join([f'"{col}" INTEGER' for col in NEW_BALL_COLUMNS])
    create_sorteios_sql = f"""CREATE TABLE IF NOT EXISTS {SORTEIOS_TABLE_NAME} (concurso INTEGER PRIMARY KEY, data_sorteio TEXT, {ball_cols_schema});"""
    cursor.execute(create_sorteios_sql)
    cols_ordered_sorteios = ['concurso', 'data_sorteio'] + NEW_BALL_COLUMNS
    placeholders_sorteios = ', '.join(['?'] * len(cols_ordered_sorteios))
    sql_insert_sorteios = f"INSERT INTO {SORTEIOS_TABLE_NAME} ({', '.join(cols_ordered_sorteios)}) VALUES ({placeholders_sorteios})"
    for record in TEST_DATA_SORTEIOS:
         values = [record.get(col) for col in cols_ordered_sorteios]
         cursor.execute(sql_insert_sorteios, values)
    conn.commit()

    # --- Cria tabela 'freq_chunk_10_detail' (VAZIA) ---
    interval_10 = 10
    table_name_chunk_10 = get_chunk_table_name(interval_10) # Usa função do db_manager
    col_defs_chunk = ', '.join([f'd{i} INTEGER NOT NULL' for i in ALL_NUMBERS])
    create_chunk_sql = f"""CREATE TABLE IF NOT EXISTS {table_name_chunk_10} (concurso INTEGER PRIMARY KEY, {col_defs_chunk});"""
    cursor.execute(create_chunk_sql)
    conn.commit()
    print(f"\nFixture: Criou tabela '{table_name_chunk_10}' (vazia).")

    return conn # Retorna a conexão populada