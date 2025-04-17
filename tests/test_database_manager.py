# tests/test_database_manager.py

import pytest
import pandas as pd
from pathlib import Path
import sqlite3
from typing import Optional, Set

# Importa funções a testar
from src.database_manager import read_data_from_db, get_draw_numbers, save_to_db
# Importa constantes
try:
    from src.config import NEW_BALL_COLUMNS, TABLE_NAME as SORTEIOS_TABLE_NAME
    from tests.conftest import NUM_TEST_RECORDS # <<< Importa do conftest
except ImportError:
    NEW_BALL_COLUMNS = [f'b{i}' for i in range(1, 16)]
    SORTEIOS_TABLE_NAME = 'sorteios'
    NUM_TEST_RECORDS = 10 # Fallback se import falhar

# Usa a fixture 'populated_db_conn' definida em conftest.py
def test_get_draw_numbers_found(populated_db_conn: sqlite3.Connection):
    """ Testa buscar dezenas de um concurso existente. """
    # Função auxiliar (mantida como antes)
    def _get_draw_numbers_test(concurso: int, conn: sqlite3.Connection) -> Optional[Set[int]]:
         cursor = conn.cursor(); ball_cols_str = ', '.join(f'"{col}"' for col in NEW_BALL_COLUMNS); sql = f'SELECT {ball_cols_str} FROM {SORTEIOS_TABLE_NAME} WHERE concurso = ?'
         try: cursor.execute(sql, (concurso,)); result = cursor.fetchone(); return {int(n) for n in result if n is not None and pd.notna(n)} if result else None
         except Exception as e: print(f"Erro: {e}"); return None

    numbers_conc_2 = _get_draw_numbers_test(2, populated_db_conn)
    expected_conc_2 = {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}
    assert numbers_conc_2 == expected_conc_2

def test_get_draw_numbers_not_found(populated_db_conn: sqlite3.Connection):
    """ Testa buscar dezenas de um concurso inexistente. """
    def _get_draw_numbers_test(concurso: int, conn: sqlite3.Connection) -> Optional[Set[int]]:
         cursor = conn.cursor(); ball_cols_str = ', '.join(f'"{col}"' for col in NEW_BALL_COLUMNS); sql = f'SELECT {ball_cols_str} FROM {SORTEIOS_TABLE_NAME} WHERE concurso = ?'
         try: cursor.execute(sql, (concurso,)); result = cursor.fetchone(); return {int(n) for n in result if n is not None and pd.notna(n)} if result else None
         except Exception as e: print(f"Erro: {e}"); return None

    numbers_conc_99 = _get_draw_numbers_test(99, populated_db_conn)
    assert numbers_conc_99 is None

def test_read_data_from_db_basic(populated_db_conn: sqlite3.Connection):
    """ Testa leitura básica de dados. """
    df = pd.read_sql(f"SELECT * FROM {SORTEIOS_TABLE_NAME} ORDER BY concurso", populated_db_conn)
    assert df is not None
    # *** CORREÇÃO AQUI: Usa a constante importada ***
    assert len(df) == NUM_TEST_RECORDS, f"Esperado {NUM_TEST_RECORDS} linhas, obteve {len(df)}"
    assert df['concurso'].iloc[0] == 1
    assert df['concurso'].iloc[-1] == NUM_TEST_RECORDS # Último concurso é o número de registros

def test_read_data_from_db_filtered(populated_db_conn: sqlite3.Connection):
    """ Testa leitura com filtros min/max. """
    df = pd.read_sql(f"SELECT * FROM {SORTEIOS_TABLE_NAME} WHERE concurso >= ? AND concurso <= ? ORDER BY concurso",
                     populated_db_conn, params=(2, 4))
    assert df is not None
    assert len(df) == 3
    assert df['concurso'].tolist() == [2, 3, 4]