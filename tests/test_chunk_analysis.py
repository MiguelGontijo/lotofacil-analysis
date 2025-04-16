# tests/test_chunk_analysis.py

import pytest
import pandas as pd
import sqlite3
from typing import Optional

# Importa a função a ser testada
from src.analysis.chunk_analysis import get_chunk_final_stats
# Importa DB manager para poder mockar a conexão ou usar fixture
from src.database_manager import DATABASE_PATH # Importa o path real se precisar mockar connect
from unittest.mock import patch

# Usa a fixture 'populated_db_conn' definida em conftest.py
# Mockamos DATABASE_PATH dentro da função de teste para que ela use o BD em memória
@patch('src.analysis.chunk_analysis.DATABASE_PATH', ':memory:') # Força usar BD em memória
@patch('src.analysis.chunk_analysis.sqlite3.connect') # Mocka a conexão
@patch('src.analysis.chunk_analysis.read_data_from_db') # Mocka leitura da tabela principal
def test_get_chunk_final_stats_no_chunk_data(mock_read_sorteios, mock_connect, populated_db_conn: sqlite3.Connection):
    """
    Testa o comportamento de get_chunk_final_stats quando a tabela de chunk está VAZIA,
    mas a tabela de sorteios tem dados. Deve retornar um DataFrame vazio.
    """
    # Configura mocks
    mock_connect.return_value = populated_db_conn # Usa a conexão da fixture (já com tabela chunk vazia)
    # Simula a leitura do último concurso da tabela sorteios (necessário para determinar limites)
    mock_read_sorteios.return_value = pd.DataFrame({'concurso': [6]}) # Último concurso é 6

    interval = 10
    # Pede stats até o concurso 10. A função deve tentar ler a linha 10 da tabela chunk.
    result_df = get_chunk_final_stats(interval_size=interval, concurso_maximo=10)

    assert result_df is not None, "Função retornou None inesperadamente"
    assert isinstance(result_df, pd.DataFrame), "Não retornou DataFrame"
    # Como a tabela chunk na fixture está vazia, a query não retornará linhas
    assert result_df.empty, f"Esperado DataFrame vazio quando tabela chunk está vazia, mas obteve {len(result_df)} linhas"


# Teste pulado que precisa de dados mockados na tabela chunk (implementação futura)
@pytest.mark.skip(reason="Requer fixture com dados pré-calculados na tabela chunk_detail")
def test_get_chunk_final_stats_with_data():
    """
    Testa se busca e rankeia corretamente os dados de um chunk existente.
    (Este teste precisa que a fixture popule a tabela chunk com dados conhecidos)
    """
    # 1. Modificar a fixture populated_db_conn para inserir linhas conhecidas
    #    em freq_chunk_10_detail (ex: linha para concurso 10)
    # 2. Chamar get_chunk_final_stats(10, 10)
    # 3. Assertar as frequências e ranks esperados para o concurso 10
    pass