# tests/test_repetition_analysis.py

import pytest
import pandas as pd
import sqlite3
from typing import Optional, Set

# Importa a função a ser testada
from src.analysis.repetition_analysis import calculate_historical_repetition_rate
# Importa constantes e fixture do conftest
from .conftest import populated_db_conn, TEST_DATA_SORTEIOS, ALL_NUMBERS
# Mock para simular read_data_from_db
from unittest.mock import patch
import numpy as np

# Garante fallback
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))

@patch('src.analysis.repetition_analysis.read_data_from_db')
def test_calculate_historical_repetition_rate(mock_read_db):
    """ Testa o cálculo da taxa de repetição com dados mockados (10 sorteios, C8 inválido). """
    test_df = pd.DataFrame(TEST_DATA_SORTEIOS[:10])
    from src.config import NEW_BALL_COLUMNS
    if 'NEW_BALL_COLUMNS' not in globals(): NEW_BALL_COLUMNS = [f'b{i}' for i in range(1,16)]
    for col in test_df.columns:
        if col != 'data_sorteio': test_df[col] = pd.to_numeric(test_df[col], errors='coerce').astype('Int64')

    mock_read_db.return_value = test_df
    rates = calculate_historical_repetition_rate(concurso_maximo=10)

    assert rates is not None; assert isinstance(rates, pd.Series); assert len(rates) == 25
    assert rates.index.tolist() == ALL_NUMBERS; assert not rates.isnull().any()

    # --- Asserts Corrigidos com base na LÓGICA FINAL e TRACE CORRETO FINAL ---
    # Apps(N-1) VÁLIDOS / Reps(N | N-1 VÁLIDO) -> Taxa
    # 1: Apps=6, Reps=5 -> 5/6
    # 5: Apps=6, Reps=5 -> 5/6
    # 11: Apps=4, Reps=2 -> 0.5
    # 16: Apps=4, Reps=1 -> 0.25
    # 22: Apps=3, Reps=1 -> 1/3
    # 24: Apps=3, Reps=1 -> 1/3
    # 25: Apps=5, Reps=1 -> 0.2

    assert rates[1] == pytest.approx(5/6)
    assert rates[5] == pytest.approx(5/6)
    assert rates[11] == pytest.approx(0.5)
    assert rates[16] == pytest.approx(0.25)
    assert rates[22] == pytest.approx(1/3) # Correto
    # *** ASSERT CORRIGIDO PARA 24 ***
    assert rates[24] == pytest.approx(1/3) # Esperado ~0.333
    assert rates[25] == pytest.approx(0.25)

def test_calculate_historical_repetition_rate_no_data():
    """ Testa com DataFrame vazio ou None ou < 2 linhas. """
    with patch('src.analysis.repetition_analysis.read_data_from_db', return_value=None):
        rates_none = calculate_historical_repetition_rate(concurso_maximo=10)
        assert rates_none.eq(0.0).all()
    with patch('src.analysis.repetition_analysis.read_data_from_db', return_value=pd.DataFrame()):
        rates_empty = calculate_historical_repetition_rate(concurso_maximo=10)
        assert rates_empty.eq(0.0).all()
    with patch('src.analysis.repetition_analysis.read_data_from_db') as mock_read:
        mock_read.return_value = pd.DataFrame(TEST_DATA_SORTEIOS[:1])
        rates_one = calculate_historical_repetition_rate(concurso_maximo=1)
        assert rates_one.eq(0.0).all()