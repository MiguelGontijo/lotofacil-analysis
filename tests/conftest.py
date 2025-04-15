# tests/conftest.py

import pytest
import sqlite3
import pandas as pd
from pathlib import Path

# Importa funções e constantes necessárias do seu projeto src
# Cuidado com imports circulares ou dependências pesadas aqui
from src.config import ALL_NUMBERS, NEW_BALL_COLUMNS, TABLE_NAME as SORTEIOS_TABLE_NAME
# Precisamos da função de criar tabela principal, idealmente movida/importável do db_manager
# ou redefinida aqui para o teste. Vamos redefinir por simplicidade agora.

# Dados de teste para inserir no banco em memória
TEST_DATA = [
    {'concurso': 1, 'data_sorteio': '2023-01-01', 'b1': 1,  'b2': 2,  'b3': 3,  'b4': 4,  'b5': 5,  'b6': 6,  'b7': 7,  'b8': 8,  'b9': 9,  'b10': 10, 'b11': 11, 'b12': 12, 'b13': 13, 'b14': 14, 'b15': 15},
    {'concurso': 2, 'data_sorteio': '2023-01-02', 'b1': 11, 'b2': 12, 'b3': 13, 'b4': 14, 'b5': 15, 'b6': 16, 'b7': 17, 'b8': 18, 'b9': 19, 'b10': 20, 'b11': 21, 'b12': 22, 'b13': 23, 'b14': 24, 'b15': 25},
    {'concurso': 3, 'data_sorteio': '2023-01-03', 'b1': 1,  'b2': 3,  'b3': 5,  'b4': 7,  'b5': 9,  'b6': 11, 'b7': 13, 'b8': 15, 'b9': 17, 'b10': 19, 'b11': 21, 'b12': 23, 'b13': 25, 'b14': 2,  'b15': 4},
    {'concurso': 4, 'data_sorteio': '2023-01-04', 'b1': 2,  'b2': 4,  'b3': 6,  'b4': 8,  'b5': 10, 'b6': 12, 'b7': 14, 'b8': 16, 'b9': 18, 'b10': 20, 'b11': 22, 'b12': 24, 'b13': 1,  'b14': 3,  'b15': 5},
    {'concurso': 5, 'data_sorteio': '2023-01-05', 'b1': 21, 'b2': 22, 'b3': 23, 'b4': 24, 'b5': 25, 'b6': 1,  'b7': 2,  'b8': 3,  'b9': 4,  'b10': 5,  'b11': 6,  'b12': 7,  'b13': 8,  'b14': 9,  'b15': 10}, # Ciclo fecha aqui
    {'concurso': 6, 'data_sorteio': '2023-01-06', 'b1': 1, 'b2': 2, 'b3': 3, 'b4': 4, 'b5': 5, 'b6': 16, 'b7': 17, 'b8': 18, 'b9': 19, 'b10': 20, 'b11': 11, 'b12': 12, 'b13': 13, 'b14': 14, 'b15': 15}, # Novo ciclo
]

@pytest.fixture(scope="function") # Cria um novo BD em memória para cada função de teste
def test_db_conn():
    """ Pytest fixture para criar um banco de dados SQLite em memória. """
    conn = sqlite3.connect(":memory:")
    yield conn # Fornece a conexão para o teste
    conn.close() # Fecha a conexão após o teste

@pytest.fixture(scope="function")
def populated_db_conn(test_db_conn):
    """ Pytest fixture que popula o BD em memória com dados de teste. """
    conn = test_db_conn
    cursor = conn.cursor()

    # Cria a tabela sorteios (simplificado, idealmente usa schema real)
    ball_cols_schema = ', '.join([f'"{col}" INTEGER' for col in NEW_BALL_COLUMNS])
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {SORTEIOS_TABLE_NAME} (
        concurso INTEGER PRIMARY KEY,
        data_sorteio TEXT,
        {ball_cols_schema}
    );
    """)
    conn.commit()

    # Insere dados de teste
    test_df = pd.DataFrame(TEST_DATA)
    # Usa a função save_to_db do seu projeto se ela puder receber uma conexão
    # Ou insere manualmente:
    cols_ordered = ['concurso', 'data_sorteio'] + NEW_BALL_COLUMNS
    placeholders = ', '.join(['?'] * len(cols_ordered))
    sql_insert = f"INSERT INTO {SORTEIOS_TABLE_NAME} ({', '.join(cols_ordered)}) VALUES ({placeholders})"

    for record in TEST_DATA:
         values = [record.get(col) for col in cols_ordered]
         cursor.execute(sql_insert, values)
    conn.commit()

    return conn # Retorna a conexão populada

# Fixture para o caminho do banco de dados (útil se precisar passar o path)
# @pytest.fixture
# def temp_db_path(tmp_path):
#     """ Cria um arquivo de BD temporário """
#     return tmp_path / "test_loto.db"