# tests/test_chunk_analysis.py

import pytest
import pandas as pd
import sqlite3
from typing import Optional

# Importa a função a ser testada
from src.analysis.chunk_analysis import get_chunk_final_stats
# Importa DB manager apenas para mockar a conexão (se necessário)
from src.database_manager import DATABASE_PATH
from unittest.mock import patch

# Importa dados esperados do conftest
from .conftest import EXPECTED_CHUNK_10_STATS_CONC_10, ALL_NUMBERS


# Usa a fixture 'populated_db_conn'
# Remove patch de read_data_from_db, pois não é mais necessário mocká-lo aqui
@patch('src.analysis.chunk_analysis.DATABASE_PATH', ':memory:')
@patch('src.analysis.chunk_analysis.sqlite3.connect')
def test_get_chunk_final_stats_with_data(mock_connect, populated_db_conn: sqlite3.Connection):
    """ Testa se busca e retorna corretamente os dados do chunk 10 finalizado. """
    # Configura mocks
    mock_connect.return_value = populated_db_conn

    interval = 10
    # Pede stats até o concurso 10 (deve retornar apenas a linha do concurso 10 da fixture)
    # Passamos None para concurso_maximo para garantir que leia tudo da fixture
    result_df = get_chunk_final_stats(interval_size=interval, concurso_maximo=None)

    assert result_df is not None, "Função retornou None"
    assert isinstance(result_df, pd.DataFrame), "Não retornou DataFrame"
    assert not result_df.empty, "DataFrame retornado está vazio"
    assert result_df.index.tolist() == [10], "Índice do DataFrame não é [10]"
    assert len(result_df) == 1, "Esperado apenas 1 linha (chunk final 10)"

    # Verifica frequências e ranks específicos da linha 10 com base nos dados pré-calculados
    row_10 = result_df.loc[10]
    assert row_10['d1_freq'] == EXPECTED_CHUNK_10_STATS_CONC_10['d1_freq'], "Freq D1"
    assert row_10['d1_rank'] == EXPECTED_CHUNK_10_STATS_CONC_10['d1_rank'], "Rank D1"
    assert row_10['d5_freq'] == EXPECTED_CHUNK_10_STATS_CONC_10['d5_freq'], "Freq D5"
    assert row_10['d5_rank'] == EXPECTED_CHUNK_10_STATS_CONC_10['d5_rank'], "Rank D5"
    assert row_10['d22_freq'] == EXPECTED_CHUNK_10_STATS_CONC_10['d22_freq'], "Freq D22"
    assert row_10['d22_rank'] == EXPECTED_CHUNK_10_STATS_CONC_10['d22_rank'], "Rank D22"
    assert row_10['d25_freq'] == EXPECTED_CHUNK_10_STATS_CONC_10['d25_freq'], "Freq D25"
    assert row_10['d25_rank'] == EXPECTED_CHUNK_10_STATS_CONC_10['d25_rank'], "Rank D25"


@patch('src.analysis.chunk_analysis.DATABASE_PATH', ':memory:')
@patch('src.analysis.chunk_analysis.sqlite3.connect')
def test_get_chunk_final_stats_limit(mock_connect, populated_db_conn: sqlite3.Connection):
    """ Testa se o filtro concurso_maximo funciona (não deve achar chunk 10). """
    mock_connect.return_value = populated_db_conn

    # Pede stats até o concurso 9 (não deve retornar o chunk 10)
    result_df = get_chunk_final_stats(interval_size=10, concurso_maximo=9)

    assert result_df is not None
    assert isinstance(result_df, pd.DataFrame)
    assert result_df.empty, "Esperado DataFrame vazio para max_concurso=9"


@patch('src.analysis.chunk_analysis.DATABASE_PATH', ':memory:')
@patch('src.analysis.chunk_analysis.sqlite3.connect')
def test_get_chunk_final_stats_missing_table(mock_connect, test_db_conn: sqlite3.Connection):
    """ Testa o comportamento se a tabela chunk não existir. """
    mock_connect.return_value = test_db_conn # Usa DB *sem* a tabela chunk criada

    # Chama a função que agora deve criar a tabela E retornar um DF vazio
    result_df = get_chunk_final_stats(interval_size=999, concurso_maximo=10)

    # *** ASSERT CORRIGIDO: Espera DataFrame vazio, não None ***
    assert result_df is not None, "Esperado DataFrame vazio, não None"
    assert isinstance(result_df, pd.DataFrame), "Não retornou DataFrame"
    assert result_df.empty, "Esperado DataFrame vazio quando a tabela é criada vazia"