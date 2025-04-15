# tests/test_analysis_frequency.py

import pytest
import pandas as pd
from pathlib import Path
import sqlite3

# Importa função a testar
from src.analysis.frequency_analysis import calculate_frequency, get_cumulative_frequency
# Importa DB manager para poder *chamar* a função passando a conexão mockada
# Ou mockamos read_data_from_db para retornar um DF fixo? Mais fácil.
from unittest.mock import patch

# Usa a fixture 'populated_db_conn' definida em conftest.py
# Mas vamos mockar a leitura do BD para isolar o teste da lógica de frequência

@pytest.fixture
def sample_dataframe():
    """ Retorna um DataFrame simples para testes de frequência. """
    # Dados similares aos da fixture de BD, mas direto aqui
    test_data = [
        {'concurso': 1, 'b1': 1, 'b2': 2, 'b3': 3, 'b4': 4, 'b5': 5, 'b6': 6, 'b7': 7, 'b8': 8, 'b9': 9, 'b10': 10, 'b11': 11, 'b12': 12, 'b13': 13, 'b14': 14, 'b15': 15},
        {'concurso': 2, 'b1': 11,'b2': 12,'b3': 13,'b4': 14,'b5': 15,'b6': 16,'b7': 17,'b8': 18,'b9': 19,'b10': 20,'b11': 21,'b12': 22,'b13': 23,'b14': 24,'b15': 25},
        {'concurso': 3, 'b1': 1, 'b2': 3, 'b3': 5, 'b4': 7, 'b5': 9, 'b6': 11,'b7': 13,'b8': 15,'b9': 17,'b10': 19,'b11': 21,'b12': 23,'b13': 25,'b14': 2, 'b15': 4},
    ]
    df = pd.DataFrame(test_data)
    # Garante tipos corretos como no data_loader
    for col in df.columns:
        if col != 'data_sorteio': # Ignora data se houver
             df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    return df

# Mocka a função que lê do banco de dados para estes testes
@patch('src.analysis.frequency_analysis.read_data_from_db')
def test_calculate_frequency_all(mock_read_db, sample_dataframe):
    """ Testa o cálculo de frequência para todos os dados mockados. """
    mock_read_db.return_value = sample_dataframe # Configura o mock

    freq = calculate_frequency(concurso_minimo=1, concurso_maximo=3)

    mock_read_db.assert_called_once() # Verifica se read_data_from_db foi chamado
    assert freq is not None
    assert freq[1] == 2 # Número 1 aparece nos concursos 1 e 3
    assert freq[2] == 2 # Número 2 aparece nos concursos 1 e 3 (bola 14)
    assert freq[16] == 1 # Número 16 aparece só no concurso 2
    assert freq[6] == 1  # Número 6 aparece só no concurso 1
    assert freq.sum() == 15 * 3 # Total de bolas sorteadas = 15 * num_concursos

@patch('src.analysis.frequency_analysis.read_data_from_db')
def test_calculate_frequency_subset(mock_read_db, sample_dataframe):
    """ Testa o cálculo de frequência para um subconjunto de concursos. """
    # Filtra o DF para simular a leitura apenas do concurso 2
    mock_read_db.return_value = sample_dataframe[sample_dataframe['concurso'] == 2]

    freq = calculate_frequency(concurso_minimo=2, concurso_maximo=2)

    mock_read_db.assert_called_once()
    assert freq is not None
    assert freq[1] == 0 # Não aparece no concurso 2
    assert freq[11] == 1
    assert freq[25] == 1
    assert freq.sum() == 15 # Apenas 15 bolas no concurso 2

@patch('src.analysis.frequency_analysis.read_data_from_db')
@patch('src.analysis.frequency_analysis.get_closest_freq_snapshot')
def test_get_cumulative_frequency_no_snapshot(mock_get_snapshot, mock_read_db, sample_dataframe):
    """ Testa get_cumulative_frequency quando não há snapshot. """
    mock_get_snapshot.return_value = None # Simula nenhum snapshot encontrado
    mock_read_db.return_value = sample_dataframe # Simula leitura de 1 a 3

    freq = get_cumulative_frequency(concurso_maximo=3)

    mock_get_snapshot.assert_called_once_with(3)
    # Espera chamar calculate_frequency (via read_data_from_db) para 1-3
    mock_read_db.assert_called_once()
    assert freq is not None
    assert freq[1] == 2 # Igual ao teste geral
    assert freq[16] == 1

@patch('src.analysis.frequency_analysis.read_data_from_db')
@patch('src.analysis.frequency_analysis.get_closest_freq_snapshot')
def test_get_cumulative_frequency_with_snapshot(mock_get_snapshot, mock_read_db, sample_dataframe):
    """ Testa get_cumulative_frequency usando um snapshot mockado. """
    # Snapshot no concurso 1
    snap_concurso = 1
    snap_data = {num: 1 for num in range(1, 16)} # Frequência 1 para 1-15
    snap_series = pd.Series(snap_data).reindex(range(1, 26), fill_value=0)
    mock_get_snapshot.return_value = (snap_concurso, snap_series)

    # Mocka a leitura do delta (concursos 2 e 3)
    delta_df = sample_dataframe[sample_dataframe['concurso'] > snap_concurso]
    mock_read_db.return_value = delta_df

    freq = get_cumulative_frequency(concurso_maximo=3)

    mock_get_snapshot.assert_called_once_with(3)
    # Verifica se chamou calculate_frequency (via read_db) para o delta correto (2 a 3)
    # (A forma como mockamos read_db aqui não permite verificar os args min/max facilmente)
    # Mas podemos verificar se foi chamado.
    mock_read_db.assert_called_once()
    assert freq is not None
    # Frequência final deve ser a soma do snapshot + delta
    # No snapshot: freq[1]=1, freq[11]=1, freq[16]=0
    # No delta (conc 2 e 3): freq[1]=1, freq[11]=1+1=2, freq[16]=1
    # Total esperado: freq[1]=1+1=2, freq[11]=1+2=3, freq[16]=0+1=1
    assert freq[1] == 2
    assert freq[11] == 3 # 1 do snap + 1 do conc 2 + 1 do conc 3
    assert freq[16] == 1 # 0 do snap + 1 do conc 2 + 0 do conc 3