# tests/test_chunk_analysis.py

import pytest
import pandas as pd
import sqlite3
from typing import Optional

# Importa as funções a serem testadas
from src.analysis.chunk_analysis import get_chunk_final_stats, calculate_historical_rank_stats
# Importa DB manager e config para mocks/constantes
from src.database_manager import DATABASE_PATH
from src.config import ALL_NUMBERS
from unittest.mock import patch

# Importa dados esperados do conftest
from .conftest import EXPECTED_CHUNK_10_STATS_CONC_10

# Garante fallback
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS_TEST = list(range(1, 26))
else: ALL_NUMBERS_TEST = ALL_NUMBERS


# Usa a fixture 'populated_db_conn'
@patch('src.analysis.chunk_analysis.DATABASE_PATH', ':memory:')
@patch('src.analysis.chunk_analysis.sqlite3.connect')
def test_get_chunk_final_stats_with_data(mock_connect, populated_db_conn: sqlite3.Connection):
    """ Testa se busca e retorna corretamente os dados do chunk 10 finalizado. """
    mock_connect.return_value = populated_db_conn
    interval = 10
    result_df = get_chunk_final_stats(interval_size=interval, concurso_maximo=None) # Pega todos disponíveis na fixture

    assert result_df is not None; assert isinstance(result_df, pd.DataFrame); assert not result_df.empty
    assert result_df.index.tolist() == [10]; assert len(result_df) == 1
    row_10 = result_df.loc[10]
    assert row_10['d1_freq'] == EXPECTED_CHUNK_10_STATS_CONC_10['d1_freq']
    assert row_10['d1_rank'] == EXPECTED_CHUNK_10_STATS_CONC_10['d1_rank']
    assert row_10['d25_freq'] == EXPECTED_CHUNK_10_STATS_CONC_10['d25_freq']
    assert row_10['d25_rank'] == EXPECTED_CHUNK_10_STATS_CONC_10['d25_rank']


@patch('src.analysis.chunk_analysis.DATABASE_PATH', ':memory:')
@patch('src.analysis.chunk_analysis.sqlite3.connect')
def test_get_chunk_final_stats_limit(mock_connect, populated_db_conn: sqlite3.Connection):
    """ Testa se o filtro concurso_maximo funciona. """
    mock_connect.return_value = populated_db_conn
    result_df = get_chunk_final_stats(interval_size=10, concurso_maximo=9)
    assert result_df is not None; assert isinstance(result_df, pd.DataFrame); assert result_df.empty


@patch('src.analysis.chunk_analysis.DATABASE_PATH', ':memory:')
@patch('src.analysis.chunk_analysis.sqlite3.connect')
def test_get_chunk_final_stats_missing_table(mock_connect, test_db_conn: sqlite3.Connection):
    """ Testa o comportamento se a tabela chunk não existir (deve retornar None). """
    mock_connect.return_value = test_db_conn # Usa DB sem a tabela chunk criada
    result_df = get_chunk_final_stats(interval_size=999, concurso_maximo=10)
    assert result_df is None, "Esperado None quando tabela chunk não existe"


# --- NOVO TESTE (Pulado por enquanto) ---
@pytest.mark.skip(reason="Requer fixture com MÚLTIPLOS chunks finais pré-calculados na tabela chunk_stats")
def test_calculate_historical_rank_stats():
    """ Testa o cálculo de média e std dev dos ranks históricos. """
    # 1. Modificar fixture `populated_db_conn` para inserir dados de MÚLTIPLOS chunk ends
    #    (ex: concursos 10, 20, 30) na tabela `chunk_stats_10_final`.
    # 2. Chamar `calculate_historical_rank_stats(interval_size=10)`
    # 3. Calcular manualmente a média e std dev esperados para alguns ranks de dezenas
    #    baseado nos dados inseridos na fixture.
    # 4. Assertar os valores calculados pela função contra os valores esperados.
    pass