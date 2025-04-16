# tests/test_scorer.py

import pytest
import pandas as pd
import numpy as np
from typing import List, Optional, Set

# Importa a função e a config V6
from src.scorer import calculate_scores, DEFAULT_SCORING_CONFIG_V6, MISSING_CYCLE_BONUS, REPEAT_PENALTY
# Importa ALL_NUMBERS
try: from src.config import ALL_NUMBERS
except ImportError: ALL_NUMBERS = list(range(1, 26))

if 'ALL_NUMBERS' not in globals() or not ALL_NUMBERS: ALL_NUMBERS_TEST: List[int] = list(range(1, 26))
else: ALL_NUMBERS_TEST: List[int] = ALL_NUMBERS


# --- Fixture com Dados Mockados (V6) ---
@pytest.fixture
def mock_analysis_results():
    """ Cria um dicionário de resultados de análise mockado para V6. """
    results = {}
    idx = ALL_NUMBERS_TEST
    results['overall_freq'] = pd.Series(range(25, 0, -1), index=idx)
    results['recent_freq_10'] = pd.Series(range(25, 0, -1), index=idx)
    results['recent_freq_25'] = pd.Series(range(25, 0, -1), index=idx) * 1.1
    results['recent_freq_50'] = pd.Series(range(25, 0, -1), index=idx) * 1.2
    results['recent_freq_100'] = pd.Series(range(25, 0, -1), index=idx) * 1.3
    results['recent_freq_200'] = pd.Series(range(25, 0, -1), index=idx) * 1.4
    results['recent_freq_300'] = pd.Series(range(25, 0, -1), index=idx) * 1.5
    results['recent_freq_400'] = pd.Series(range(25, 0, -1), index=idx) * 1.6
    results['recent_freq_500'] = pd.Series(range(25, 0, -1), index=idx) * 1.7
    results['current_delay'] = pd.Series(range(25, 0, -1), index=idx)
    results['delay_std_dev'] = pd.Series(range(1, 26), index=idx)
    results['avg_hist_intra_delay'] = pd.Series(range(1, 26), index=idx)
    results['max_hist_intra_delay'] = pd.Series(range(1, 26), index=idx)
    results['last_cycle_freq'] = pd.Series(5, index=idx)
    results['current_cycle_freq'] = pd.Series(range(25, 0, -1), index=idx)
    results['missing_current_cycle'] = set()
    results['current_intra_cycle_delay'] = pd.Series(range(25, 0, -1), index=idx)
    trend_values = np.arange(5.0, 5.0 - (25 * 0.2), -0.2)[:25]; results['freq_trend'] = pd.Series(trend_values, index=idx)
    results['numbers_in_last_draw'] = {1, 3, 5} # Penaliza 1, 3, 5

    results['last_cycle_freq'].loc[10] = pd.NA
    results['delay_std_dev'].loc[15] = pd.NA
    results['avg_hist_intra_delay'].loc[20] = pd.NA

    return results

# --- Testes ---

def test_calculate_scores_returns_valid_series(mock_analysis_results):
    """ Testa se retorna Series válida (formato, NAs, índice) com config V6. """
    scores = calculate_scores(mock_analysis_results, config=DEFAULT_SCORING_CONFIG_V6)
    assert scores is not None, "Função retornou None"
    assert isinstance(scores, pd.Series), "Resultado não é Series"
    assert len(scores) == 25, f"Esperado 25 scores, obteve {len(scores)}"
    assert set(scores.index) == set(ALL_NUMBERS_TEST), "Índice não contém 1-25"
    assert not scores.isnull().any(), f"NaNs encontrados: {scores[scores.isnull()]}"

# --- TESTE COM ASSERT CORRIGIDO ---
def test_calculate_scores_ranking_extremos(mock_analysis_results):
    """ Testa se os extremos do ranking V6 estão corretos para mock data COM penalidade. """
    scores = calculate_scores(mock_analysis_results, config=DEFAULT_SCORING_CONFIG_V6)
    assert scores is not None

    # *** ASSERT CORRIGIDO: Esperamos 2 como melhor devido à penalidade em 1 ***
    expected_max_scorer = 2
    actual_max_scorer = scores.idxmax()
    assert actual_max_scorer == expected_max_scorer, f"Esperado Dezena {expected_max_scorer} com maior score V6 (devido à penalidade em 1), mas foi {actual_max_scorer}"

    # 25 ainda deve ser o pior
    expected_min_scorer = 25
    actual_min_scorer = scores.idxmin()
    assert actual_min_scorer == expected_min_scorer, f"Esperado Dezena {expected_min_scorer} com menor score V6, mas foi {actual_min_scorer}"

    print("\nScores (Teste Extremos V6 - com penalidade):"); print(scores.head(3)); print(scores.tail(3))
# --- FIM TESTE CORRIGIDO ---


def test_calculate_scores_cycle_bonus(mock_analysis_results):
    """ Testa a aplicação correta do bônus de ciclo com config V6. """
    missing_set: Set[int] = {23, 24, 25}
    mock_copy_sem_bonus = mock_analysis_results.copy(); mock_copy_sem_bonus['missing_current_cycle'] = set()
    mock_copy_com_bonus = mock_analysis_results.copy(); mock_copy_com_bonus['missing_current_cycle'] = missing_set

    scores_sem_bonus = calculate_scores(mock_copy_sem_bonus, config=DEFAULT_SCORING_CONFIG_V6)
    scores_com_bonus = calculate_scores(mock_copy_com_bonus, config=DEFAULT_SCORING_CONFIG_V6)

    assert scores_com_bonus is not None; assert scores_sem_bonus is not None
    for dezena in missing_set: assert scores_com_bonus[dezena] == pytest.approx(scores_sem_bonus[dezena] + MISSING_CYCLE_BONUS)
    for dezena in set(ALL_NUMBERS_TEST) - missing_set: assert scores_com_bonus[dezena] == pytest.approx(scores_sem_bonus[dezena])

def test_calculate_scores_repeat_penalty(mock_analysis_results):
     """ Testa a aplicação da penalidade de repetição com V6. """
     repeated_set: Set[int] = {1, 3, 5} # Do mock_analysis_results
     mock_copy_sem_penalty = mock_analysis_results.copy(); mock_copy_sem_penalty['numbers_in_last_draw'] = set()
     mock_copy_com_penalty = mock_analysis_results.copy(); # Já tem o set {1, 3, 5}

     scores_sem_penalty = calculate_scores(mock_copy_sem_penalty, config=DEFAULT_SCORING_CONFIG_V6)
     scores_com_penalty = calculate_scores(mock_copy_com_penalty, config=DEFAULT_SCORING_CONFIG_V6)

     assert scores_com_penalty is not None; assert scores_sem_penalty is not None
     for dezena in repeated_set: assert scores_com_penalty[dezena] == pytest.approx(scores_sem_penalty[dezena] + REPEAT_PENALTY)
     for dezena in set(ALL_NUMBERS_TEST) - repeated_set: assert scores_com_penalty[dezena] == pytest.approx(scores_sem_penalty[dezena])


def test_calculate_scores_missing_metric(mock_analysis_results):
     """ Testa se lida bem com métrica faltando com V6. """
     import copy
     results_copy = copy.deepcopy(mock_analysis_results)
     metric_to_remove = 'recent_freq_100'
     assert metric_to_remove in DEFAULT_SCORING_CONFIG_V6
     del results_copy[metric_to_remove]

     scores = calculate_scores(results_copy, config=DEFAULT_SCORING_CONFIG_V6)
     assert scores is not None; assert len(scores) == 25; assert not scores.isnull().any()
     print("\nScores (Métrica Faltante V6): OK")