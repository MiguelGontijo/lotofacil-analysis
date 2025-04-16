# tests/conftest.py

import pytest
import sqlite3
import pandas as pd
from pathlib import Path
from typing import List # Adicionado para type hint

# Importa constantes necessárias do config
# É melhor importar explicitamente do que depender de fallbacks nos testes
from src.config import (
    ALL_NUMBERS, NEW_BALL_COLUMNS, TABLE_NAME as SORTEIOS_TABLE_NAME,
    FREQ_CHUNK_DETAIL_PREFIX # Para construir nome da tabela de chunk
)
# Importa função para criar tabela de chunk
from src.database_manager import create_chunk_freq_table, get_chunk_table_name

# Dados de teste para sorteios (6 concursos)
TEST_DATA_SORTEIOS = [
    {'concurso': 1, 'data_sorteio': '2023-01-01', 'b1': 1,  'b2': 2,  'b3': 3,  'b4': 4,  'b5': 5,  'b6': 6,  'b7': 7,  'b8': 8,  'b9': 9,  'b10': 10, 'b11': 11, 'b12': 12, 'b13': 13, 'b14': 14, 'b15': 15},
    {'concurso': 2, 'data_sorteio': '2023-01-02', 'b1': 11, 'b2': 12, 'b3': 13, 'b4': 14, 'b5': 15, 'b6': 16, 'b7': 17, 'b8': 18, 'b9': 19, 'b10': 20, 'b11': 21, 'b12': 22, 'b13': 23, 'b14': 24, 'b15': 25},
    {'concurso': 3, 'data_sorteio': '2023-01-03', 'b1': 1,  'b2': 3,  'b3': 5,  'b4': 7,  'b5': 9,  'b6': 11, 'b7': 13, 'b8': 15, 'b9': 17, 'b10': 19, 'b11': 21, 'b12': 23, 'b13': 25, 'b14': 2,  'b15': 4},
    {'concurso': 4, 'data_sorteio': '2023-01-04', 'b1': 2,  'b2': 4,  'b3': 6,  'b4': 8,  'b5': 10, 'b6': 12, 'b7': 14, 'b8': 16, 'b9': 18, 'b10': 20, 'b11': 22, 'b12': 24, 'b13': 1,  'b14': 3,  'b15': 5},
    {'concurso': 5, 'data_sorteio': '2023-01-05', 'b1': 21, 'b2': 22, 'b3': 23, 'b4': 24, 'b5': 25, 'b6': 1,  'b7': 2,  'b8': 3,  'b9': 4,  'b10': 5,  'b11': 6,  'b12': 7,  'b13': 8,  'b14': 9,  'b15': 10},
    {'concurso': 6, 'data_sorteio': '2023-01-06', 'b1': 1,  'b2': 2,  'b3': 3,  'b4': 4,  'b5': 5,  'b6': 16, 'b7': 17, 'b8': 18, 'b9': 19, 'b10': 20, 'b11': 11, 'b12': 12, 'b13': 13, 'b14': 14, 'b15': 15},
    # Adicionar mais dados se precisar testar mais chunks
]

# --- Dados PRÉ-CALCULADOS para freq_chunk_10_detail (baseado nos TEST_DATA_SORTEIOS) ---
# Bloco 1-10: Usaremos os dados até o concurso 5 como exemplo (concurso 10 não existe nos dados)
# Contagem manual para concursos 1 a 5:
# 1: {1:1, 2:1, 3:1, 4:1, 5:1, 6:1, 7:1, 8:1, 9:1, 10:1, 11:1, 12:1, 13:1, 14:1, 15:1}
# 2: {11:1, 12:1, 13:1, 14:1, 15:1, 16:1, 17:1, 18:1, 19:1, 20:1, 21:1, 22:1, 23:1, 24:1, 25:1}
# 3: {1:1, 2:1, 3:1, 4:1, 5:1, 7:1, 9:1, 11:1, 13:1, 15:1, 17:1, 19:1, 21:1, 23:1, 25:1}
# 4: {1:1, 2:1, 3:1, 4:1, 5:1, 6:1, 8:1, 10:1, 12:1, 14:1, 16:1, 18:1, 20:1, 22:1, 24:1}
# 5: {1:1, 2:1, 3:1, 4:1, 5:1, 6:1, 7:1, 8:1, 9:1, 10:1, 21:1, 22:1, 23:1, 24:1, 25:1}
# Contagem acumulada para Concurso 5 (dentro do chunk 1-10):
# d1: 1+0+1+1+1=4, d2: 1+0+1+1+1=4, d3: 1+0+1+1+1=4, d4: 1+0+1+1+1=4, d5: 1+0+1+1+1=4,
# d6: 1+0+0+1+1=3, d7: 1+0+1+0+1=3, d8: 1+0+0+1+1=3, d9: 1+0+1+0+1=3, d10: 1+0+0+1+1=3,
# d11: 1+1+1+0+0=3, d12: 1+1+0+1+0=3, d13: 1+1+1+0+0=3, d14: 1+1+0+1+0=3, d15: 1+1+1+0+0=3,
# d16: 0+1+0+1+0=2, d17: 0+1+1+0+0=2, d18: 0+1+0+1+0=2, d19: 0+1+1+0+0=2, d20: 0+1+0+1+0=2,
# d21: 0+1+1+0+1=3, d22: 0+1+0+1+1=3, d23: 0+1+1+0+1=3, d24: 0+1+0+1+1=3, d25: 0+1+1+0+1=3
TEST_DATA_CHUNK_10_DETAIL = [
    # Apenas a linha do concurso 5 para teste
    {'concurso': 5, 'd1': 4, 'd2': 4, 'd3': 4, 'd4': 4, 'd5': 4, 'd6': 3, 'd7': 3, 'd8': 3, 'd9': 3, 'd10': 3, 'd11': 3, 'd12': 3, 'd13': 3, 'd14': 3, 'd15': 3, 'd16': 2, 'd17': 2, 'd18': 2, 'd19': 2, 'd20': 2, 'd21': 3, 'd22': 3, 'd23': 3, 'd24': 3, 'd25': 3}
]

@pytest.fixture(scope="function")
def test_db_conn():
    """ Pytest fixture para criar um banco de dados SQLite em memória. """
    conn = sqlite3.connect(":memory:")
    # Opcional: Habilitar foreign keys se usarmos no futuro
    # conn.execute("PRAGMA foreign_keys = ON")
    yield conn
    conn.close()

@pytest.fixture(scope="function")
def populated_db_conn(test_db_conn: sqlite3.Connection):
    """ Popula o BD em memória com dados de sorteios e chunk 10. """
    conn = test_db_conn
    cursor = conn.cursor()

    # --- Cria e popula tabela 'sorteios' ---
    ball_cols_schema = ', '.join([f'"{col}" INTEGER' for col in NEW_BALL_COLUMNS])
    create_sorteios_sql = f"""
    CREATE TABLE IF NOT EXISTS {SORTEIOS_TABLE_NAME} (
        concurso INTEGER PRIMARY KEY, data_sorteio TEXT, {ball_cols_schema}
    );"""
    cursor.execute(create_sorteios_sql)
    cols_ordered_sorteios = ['concurso', 'data_sorteio'] + NEW_BALL_COLUMNS
    placeholders_sorteios = ', '.join(['?'] * len(cols_ordered_sorteios))
    sql_insert_sorteios = f"INSERT INTO {SORTEIOS_TABLE_NAME} ({', '.join(cols_ordered_sorteios)}) VALUES ({placeholders_sorteios})"
    for record in TEST_DATA_SORTEIOS:
         values = [record.get(col) for col in cols_ordered_sorteios]
         cursor.execute(sql_insert_sorteios, values)
    conn.commit()
    print(f"\nFixture: Inseriu {len(TEST_DATA_SORTEIOS)} registros em '{SORTEIOS_TABLE_NAME}'.")


    # --- Cria e popula tabela 'freq_chunk_10_detail' ---
    interval_10 = 10
    table_name_chunk_10 = get_chunk_table_name(interval_10)
    # Usa a função do database_manager para criar a tabela
    create_chunk_freq_table(interval_size=interval_10, db_path=":memory:") # Passa path ou usa default? Precisa ajustar create_table

    # Re-define create_chunk_freq_table para aceitar conexão
    # OU simplesmente executa o SQL aqui na fixture
    col_defs_chunk = ', '.join([f'd{i} INTEGER NOT NULL' for i in ALL_NUMBERS])
    create_chunk_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name_chunk_10} (
        concurso INTEGER PRIMARY KEY, {col_defs_chunk}
    );"""
    cursor.execute(create_chunk_sql)
    conn.commit()

    # Insere dados pré-calculados manualmente
    chunk_cols_ordered = ['concurso'] + [f'd{i}' for i in ALL_NUMBERS]
    chunk_placeholders = ', '.join(['?'] * len(chunk_cols_ordered))
    sql_insert_chunk = f"INSERT OR REPLACE INTO {table_name_chunk_10} ({', '.join(chunk_cols_ordered)}) VALUES ({chunk_placeholders})"
    for record in TEST_DATA_CHUNK_10_DETAIL:
         values = [record.get(col, 0) for col in chunk_cols_ordered] # Usa get com default 0
         cursor.execute(sql_insert_chunk, values)
    conn.commit()
    print(f"Fixture: Inseriu {len(TEST_DATA_CHUNK_10_DETAIL)} registros em '{table_name_chunk_10}'.")


    return conn # Retorna a conexão populada com ambas as tabelas