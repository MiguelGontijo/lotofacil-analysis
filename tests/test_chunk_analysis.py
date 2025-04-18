# tests/test_chunk_analysis.py

import pytest
import pandas as pd
import sqlite3
from typing import Optional
import numpy as np # Import numpy

# Importa as funções a serem testadas
from src.analysis.chunk_analysis import get_chunk_final_stats, calculate_historical_rank_stats
# Importa DB manager e config para mocks/constantes
from src.database_manager import DATABASE_PATH
from unittest.mock import patch

# Importa dados esperados do conftest
from .conftest import (
    EXPECTED_CHUNK_10_STATS_CONC_10, EXPECTED_CHUNK_10_STATS_CONC_20,
    ALL_NUMBERS, NUM_TEST_RECORDS # Usa NUM_TEST_RECORDS
)

# Garante fallback
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS_TEST = list(range(1, 26))
else: ALL_NUMBERS_TEST = ALL_NUMBERS


# --- Testes para get_chunk_final_stats ---
@patch('src.analysis.chunk_analysis.DATABASE_PATH', ':memory:')
@patch('src.analysis.chunk_analysis.sqlite3.connect')
def test_get_chunk_final_stats_with_data(mock_connect, populated_db_conn: sqlite3.Connection):
    """ Testa se busca e retorna corretamente os dados dos chunks 10 e 20. """
    mock_connect.return_value = populated_db_conn
    interval = 10
    result_df = get_chunk_final_stats(interval_size=interval, concurso_maximo=None)

    assert result_df is not None; assert isinstance(result_df, pd.DataFrame); assert not result_df.empty
    assert result_df.index.tolist() == [10, 20]; assert len(result_df) == 2 # Espera 2 linhas

    # Verifica concurso 10
    row_10 = result_df.loc[10]
    assert row_10['d1_freq'] == EXPECTED_CHUNK_10_STATS_CONC_10['d1_freq']
    assert row_10['d1_rank'] == EXPECTED_CHUNK_10_STATS_CONC_10['d1_rank']
    assert row_10['d25_freq'] == EXPECTED_CHUNK_10_STATS_CONC_10['d25_freq']
    assert row_10['d25_rank'] == EXPECTED_CHUNK_10_STATS_CONC_10['d25_rank']

    # Verifica concurso 20
    row_20 = result_df.loc[20]
    assert row_20['d1_freq'] == EXPECTED_CHUNK_10_STATS_CONC_20['d1_freq']
    assert row_20['d1_rank'] == EXPECTED_CHUNK_10_STATS_CONC_20['d1_rank']
    assert row_20['d25_freq'] == EXPECTED_CHUNK_10_STATS_CONC_20['d25_freq']
    assert row_20['d25_rank'] == EXPECTED_CHUNK_10_STATS_CONC_20['d25_rank']

@patch('src.analysis.chunk_analysis.DATABASE_PATH', ':memory:')
@patch('src.analysis.chunk_analysis.sqlite3.connect')
def test_get_chunk_final_stats_limit(mock_connect, populated_db_conn: sqlite3.Connection):
    """ Testa se o filtro concurso_maximo funciona. """
    mock_connect.return_value = populated_db_conn
    # Pede stats até o concurso 15 (só deve retornar o chunk 10)
    result_df = get_chunk_final_stats(interval_size=10, concurso_maximo=15)
    assert result_df is not None; assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty; assert result_df.index.tolist() == [10]

    # Pede stats até o concurso 9 (não deve retornar nada)
    result_df_9 = get_chunk_final_stats(interval_size=10, concurso_maximo=9)
    assert result_df_9 is not None; assert isinstance(result_df_9, pd.DataFrame); assert result_df_9.empty


@patch('src.analysis.chunk_analysis.DATABASE_PATH', ':memory:')
@patch('src.analysis.chunk_analysis.sqlite3.connect')
def test_get_chunk_final_stats_missing_table(mock_connect, test_db_conn: sqlite3.Connection):
    """ Testa o comportamento se a tabela chunk não existir (deve retornar None). """
    mock_connect.return_value = test_db_conn # Usa DB *sem* a tabela chunk criada
    result_df = get_chunk_final_stats(interval_size=999, concurso_maximo=10)
    assert result_df is None


# --- NOVO TESTE IMPLEMENTADO ---
@patch('src.analysis.chunk_analysis.DATABASE_PATH', ':memory:')
@patch('src.analysis.chunk_analysis.sqlite3.connect')
def test_calculate_historical_rank_stats(mock_connect, populated_db_conn: sqlite3.Connection):
    """ Testa cálculo de média e std dev dos ranks históricos dos chunks 10 e 20. """
    mock_connect.return_value = populated_db_conn
    interval = 10

    # Calcula os stats históricos usando os dados da fixture (chunks 10 e 20)
    hist_stats_df = calculate_historical_rank_stats(interval_size=interval, concurso_maximo=None)

    assert hist_stats_df is not None
    assert isinstance(hist_stats_df, pd.DataFrame)
    assert not hist_stats_df.empty
    assert len(hist_stats_df) == 25
    assert hist_stats_df.index.tolist() == ALL_NUMBERS_TEST
    col_avg = f'avg_rank_chunk{interval}'
    col_std = f'std_rank_chunk{interval}'
    assert col_avg in hist_stats_df.columns
    assert col_std in hist_stats_df.columns

    # Verifica valores calculados manualmente para algumas dezenas
    # Dezena 1: Ranks [1, 9]. Avg=5.0. Std=sqrt(32)=5.657
    assert hist_stats_df.loc[1, col_avg] == pytest.approx(5.0)
    assert hist_stats_df.loc[1, col_std] == pytest.approx(np.sqrt(32))

    # Dezena 2: Ranks [3, 1]. Avg=2.0. Std=sqrt(2)=1.414
    assert hist_stats_df.loc[2, col_avg] == pytest.approx(2.0)
    assert hist_stats_df.loc[2, col_std] == pytest.approx(np.sqrt(2))

    # Dezena 25: Ranks [16, 9]. Avg=12.5. Std=sqrt(24.5)=4.950
    assert hist_stats_df.loc[25, col_avg] == pytest.approx(12.5)
    assert hist_stats_df.loc[25, col_std] == pytest.approx(np.sqrt(24.5))

    # Verifica se não há NaNs (pois temos 2 pontos para todos)
    assert not hist_stats_df.isnull().any().any()


@patch('src.analysis.chunk_analysis.get_chunk_final_stats') # Mocka a função que lê dados
def test_calculate_historical_rank_stats_insufficient_data(mock_get_stats):
    """ Testa o caso com menos de 2 chunks (std dev deve ser NaN). """
    # Mock get_chunk_final_stats para retornar apenas 1 linha
    # Cria um DF com as colunas de rank esperadas
    rank_cols = [f'd{i}_rank' for i in ALL_NUMBERS_TEST]
    mock_data = {col: [EXPECTED_CHUNK_10_STATS_CONC_10[col]] for col in rank_cols}
    mock_df = pd.DataFrame(mock_data, index=[10])
    mock_df.index.name = 'concurso_fim' # Define nome do índice como esperado

    mock_get_stats.return_value = mock_df
    hist_stats_df = calculate_historical_rank_stats(interval_size=10)

    assert hist_stats_df is not None
    assert isinstance(hist_stats_df, pd.DataFrame)
    assert len(hist_stats_df) == 25
    # Média deve ser igual ao rank do único chunk
    assert hist_stats_df.loc[1, 'avg_rank_chunk10'] == EXPECTED_CHUNK_10_STATS_CONC_10['d1_rank']
    # Std dev deve ser NaN
    assert pd.isna(hist_stats_df.loc[1, 'std_rank_chunk10'])
    assert hist_stats_df['std_rank_chunk10'].isnull().all()