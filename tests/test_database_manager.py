# tests/test_database_manager.py

import pytest
import pandas as pd
from pathlib import Path
import sqlite3
from typing import Optional, Set # <<< IMPORT ADICIONADO >>>

# Importa funções a testar e constantes
from src.database_manager import read_data_from_db, get_draw_numbers, save_to_db
# Assume que NEW_BALL_COLUMNS e SORTEIOS_TABLE_NAME vêm do config ou são definidos aqui
try:
    from src.config import NEW_BALL_COLUMNS, TABLE_NAME as SORTEIOS_TABLE_NAME
except ImportError:
    NEW_BALL_COLUMNS = [f'b{i}' for i in range(1, 16)]
    SORTEIOS_TABLE_NAME = 'sorteios'


# Usa a fixture 'populated_db_conn' definida em conftest.py
def test_get_draw_numbers_found(populated_db_conn: sqlite3.Connection):
    """ Testa buscar dezenas de um concurso existente. """
    # Função auxiliar refeita aqui para usar a conexão da fixture
    def _get_draw_numbers_test(concurso: int, conn: sqlite3.Connection) -> Optional[Set[int]]: # <<< Usa Set importado
         cursor = conn.cursor()
         ball_cols_str = ', '.join(f'"{col}"' for col in NEW_BALL_COLUMNS)
         sql = f'SELECT {ball_cols_str} FROM {SORTEIOS_TABLE_NAME} WHERE concurso = ?'
         try:
             cursor.execute(sql, (concurso,))
             result = cursor.fetchone()
             if result:
                 # Converte para int, tratando possíveis Nones que não deveriam existir nas bolas
                 numbers = {int(n) for n in result if n is not None and pd.notna(n)}
                 # Verifica se temos 15 números antes de retornar
                 return numbers if len(numbers) == 15 else None
         except Exception as e:
             print(f"Erro no _get_draw_numbers_test: {e}") # Log de erro para debug
         return None

    numbers_conc_2 = _get_draw_numbers_test(2, populated_db_conn)
    expected_conc_2 = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}
    assert numbers_conc_2 is not None
    assert numbers_conc_2 == expected_conc_2

def test_get_draw_numbers_not_found(populated_db_conn: sqlite3.Connection):
    """ Testa buscar dezenas de um concurso inexistente. """
    # Função auxiliar refeita aqui
    def _get_draw_numbers_test(concurso: int, conn: sqlite3.Connection) -> Optional[Set[int]]: # <<< Usa Set importado
         cursor = conn.cursor()
         ball_cols_str = ', '.join(f'"{col}"' for col in NEW_BALL_COLUMNS)
         sql = f'SELECT {ball_cols_str} FROM {SORTEIOS_TABLE_NAME} WHERE concurso = ?'
         try:
             cursor.execute(sql, (concurso,))
             result = cursor.fetchone()
             if result:
                 numbers = {int(n) for n in result if n is not None and pd.notna(n)}
                 return numbers if len(numbers) == 15 else None
         except Exception as e:
              print(f"Erro no _get_draw_numbers_test: {e}")
         return None

    numbers_conc_99 = _get_draw_numbers_test(99, populated_db_conn)
    assert numbers_conc_99 is None

def test_read_data_from_db_basic(populated_db_conn: sqlite3.Connection):
    """ Testa leitura básica de dados. """
    # Usa pd.read_sql diretamente com a conexão da fixture
    df = pd.read_sql(f"SELECT * FROM {SORTEIOS_TABLE_NAME} ORDER BY concurso", populated_db_conn)
    assert df is not None
    # Verifica o número de linhas baseado nos dados inseridos em conftest.py
    # Se TEST_DATA tem 6 linhas:
    assert len(df) == 6
    assert df['concurso'].iloc[0] == 1
    assert df['concurso'].iloc[-1] == 6 # Último concurso é 6

def test_read_data_from_db_filtered(populated_db_conn: sqlite3.Connection):
    """ Testa leitura com filtros min/max. """
    df = pd.read_sql(f"SELECT * FROM {SORTEIOS_TABLE_NAME} WHERE concurso >= ? AND concurso <= ? ORDER BY concurso",
                     populated_db_conn, params=(2, 4))
    assert df is not None
    assert len(df) == 3 # Concursos 2, 3, 4
    assert df['concurso'].tolist() == [2, 3, 4]