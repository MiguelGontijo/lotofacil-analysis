# tests/test_scorer.py

import pytest
import pandas as pd
import numpy as np
from typing import List, Optional, Set

# Importa a função e a config a ser testada
from src.scorer import calculate_scores, DEFAULT_SCORING_CONFIG_V4, MISSING_CYCLE_BONUS
# Importa ALL_NUMBERS do config (ou usa fallback)
try:
    from src.config import ALL_NUMBERS
except ImportError:
    ALL_NUMBERS = list(range(1, 26))

# Garante fallback se ALL_NUMBERS não vier do config
if 'ALL_NUMBERS' not in globals() or not ALL_NUMBERS:
     ALL_NUMBERS_TEST: List[int] = list(range(1, 26))
else: ALL_NUMBERS_TEST: List[int] = ALL_NUMBERS


# --- Fixture com Dados Mockados (igual antes, com correção do np.arange) ---
@pytest.fixture
def mock_analysis_results():
    """ Cria um dicionário de resultados de análise mockado. """
    results = {}
    idx = ALL_NUMBERS_TEST
    results['overall_freq'] = pd.Series(range(25, 0, -1), index=idx)
    results['recent_freq_10'] = pd.Series(range(25, 0, -1), index=idx)
    results['recent_freq_25'] = pd.Series(range(25, 0, -1), index=idx)
    results['recent_freq_50'] = pd.Series(range(25, 0, -1), index=idx)
    results['recent_freq_100'] = pd.Series(range(25, 0, -1), index=idx)
    results['recent_freq_200'] = pd.Series(range(25, 0, -1), index=idx)
    results['current_delay'] = pd.Series(range(25, 0, -1), index=idx)
    results['delay_std_dev'] = pd.Series(range(1, 26), index=idx) # 1 menor std dev (melhor rank)
    results['last_cycle_freq'] = pd.Series(5, index=idx)
    results['current_cycle_freq'] = pd.Series(range(25, 0, -1), index=idx)
    results['missing_current_cycle'] = set()
    trend_values = np.arange(5.0, 5.0 - (25 * 0.2), -0.2)[:25]
    results['freq_trend'] = pd.Series(trend_values, index=idx) # 1 maior tendência
    results['last_cycle_freq'].loc[10] = pd.NA
    results['delay_std_dev'].loc[15] = pd.NA
    return results

# --- Testes Refatorados ---

def test_calculate_scores_returns_valid_series(mock_analysis_results):
    """ Testa se a função retorna uma Series válida (formato, NAs, índice). """
    scores = calculate_scores(mock_analysis_results, config=DEFAULT_SCORING_CONFIG_V4)

    assert scores is not None, "Função retornou None"
    assert isinstance(scores, pd.Series), "Resultado não é uma Series Pandas"
    assert len(scores) == 25, f"Esperado 25 scores, obteve {len(scores)}"
    # Verifica se o índice contém todos os números de 1 a 25
    assert set(scores.index) == set(ALL_NUMBERS_TEST), "Índice da Series não contém todas as dezenas 1-25"
    # Verifica se não há NaNs no score final
    assert not scores.isnull().any(), f"Encontrados valores NaN nos scores finais: {scores[scores.isnull()]}"

def test_calculate_scores_ranking_extremos(mock_analysis_results):
    """ Testa se os extremos do ranking (melhor e pior score) estão corretos para os dados mockados. """
    scores = calculate_scores(mock_analysis_results, config=DEFAULT_SCORING_CONFIG_V4)

    assert scores is not None
    # Com a config V4 e os dados mockados, onde 1 é melhor em quase tudo e 25 é pior:
    assert scores.idxmax() == 1, f"Esperado Dezena 1 com maior score, mas foi {scores.idxmax()}"
    assert scores.idxmin() == 25, f"Esperado Dezena 25 com menor score, mas foi {scores.idxmin()}"

    print("\nScores (Teste Extremos):")
    print(scores.head(3))
    print(scores.tail(3))

def test_calculate_scores_cycle_bonus(mock_analysis_results):
    """ Testa a aplicação correta do bônus de ciclo. """
    missing_set: Set[int] = {23, 24, 25} # Dezenas com score naturalmente baixo no mock
    mock_copy_sem_bonus = mock_analysis_results.copy()
    mock_copy_sem_bonus['missing_current_cycle'] = set()

    mock_copy_com_bonus = mock_analysis_results.copy()
    mock_copy_com_bonus['missing_current_cycle'] = missing_set

    scores_sem_bonus = calculate_scores(mock_copy_sem_bonus, config=DEFAULT_SCORING_CONFIG_V4)
    scores_com_bonus = calculate_scores(mock_copy_com_bonus, config=DEFAULT_SCORING_CONFIG_V4)

    assert scores_com_bonus is not None
    assert scores_sem_bonus is not None

    # Verifica bônus
    for dezena in missing_set:
         msg = f"Falha bônus dezena {dezena}: Esperado {scores_sem_bonus[dezena] + MISSING_CYCLE_BONUS}, Obteve {scores_com_bonus[dezena]}"
         # Usar approx para comparações de float
         assert scores_com_bonus[dezena] == pytest.approx(scores_sem_bonus[dezena] + MISSING_CYCLE_BONUS), msg

    # Verifica sem bônus
    for dezena in set(ALL_NUMBERS_TEST) - missing_set:
        msg = f"Falha não-bônus dezena {dezena}: Score mudou de {scores_sem_bonus[dezena]} para {scores_com_bonus[dezena]}"
        assert scores_com_bonus[dezena] == pytest.approx(scores_sem_bonus[dezena]), msg

def test_calculate_scores_missing_metric(mock_analysis_results):
     """ Testa se lida bem com uma métrica faltando nos resultados da análise. """
     import copy
     results_copy = copy.deepcopy(mock_analysis_results)
     # Remove métrica com peso != 0 na config V4
     metric_to_remove = 'recent_freq_25'
     assert metric_to_remove in DEFAULT_SCORING_CONFIG_V4
     assert DEFAULT_SCORING_CONFIG_V4[metric_to_remove]['weight'] != 0
     del results_copy[metric_to_remove]

     scores = calculate_scores(results_copy, config=DEFAULT_SCORING_CONFIG_V4)
     # O teste principal é verificar se não deu erro e retornou um resultado válido
     assert scores is not None, "Retornou None ao remover métrica"
     assert len(scores) == 25, "Não retornou 25 scores ao remover métrica"
     assert not scores.isnull().any(), "Retornou NaN ao remover métrica"
     print("\nScores (Métrica Faltante): OK - Calculado sem erro.")