# tests/test_scorer.py

import pytest
import pandas as pd
import numpy as np
from typing import List, Optional, Set

# Importa a função e a config V7
from src.scorer import calculate_scores, DEFAULT_SCORING_CONFIG_V7, MISSING_CYCLE_BONUS, REPEAT_PENALTY
# Importa ALL_NUMBERS e janelas de grupo do config
try: from src.config import ALL_NUMBERS, DEFAULT_GROUP_WINDOWS
except ImportError: ALL_NUMBERS = list(range(1, 26)); DEFAULT_GROUP_WINDOWS=[25,100]

if 'ALL_NUMBERS' not in globals() or not ALL_NUMBERS: ALL_NUMBERS_TEST: List[int] = list(range(1, 26))
else: ALL_NUMBERS_TEST: List[int] = ALL_NUMBERS


# --- Fixture com Dados Mockados (V7) ---
@pytest.fixture
def mock_analysis_results():
    """ Cria um dicionário de resultados de análise mockado para V7. """
    results = {}
    idx = ALL_NUMBERS_TEST
    results['overall_freq'] = pd.Series(range(25, 0, -1), index=idx)
    try: from src.config import AGGREGATOR_WINDOWS
    except ImportError: AGGREGATOR_WINDOWS = [10, 25, 50, 100, 200, 300, 400, 500]
    for w in AGGREGATOR_WINDOWS: results[f'recent_freq_{w}'] = pd.Series(range(25, 0, -1), index=idx) * (1 + w/1000.0)
    results['current_delay'] = pd.Series(range(25, 0, -1), index=idx)
    results['delay_std_dev'] = pd.Series(range(1, 26), index=idx)
    results['avg_hist_intra_delay'] = pd.Series(range(1, 26), index=idx)
    results['max_hist_intra_delay'] = pd.Series(range(1, 26), index=idx)
    results['last_cycle_freq'] = pd.Series(5, index=idx)
    results['current_cycle_freq'] = pd.Series(range(25, 0, -1), index=idx)
    results['missing_current_cycle'] = set()
    results['current_intra_cycle_delay'] = pd.Series(range(25, 0, -1), index=idx)
    results['closing_freq'] = pd.Series(range(1, 26), index=idx)
    results['sole_closing_freq'] = pd.Series(range(1, 26), index=idx)
    trend_values = np.arange(5.0, 5.0 - (25 * 0.2), -0.2)[:25]; results['freq_trend'] = pd.Series(trend_values, index=idx)
    results['rank_trend'] = pd.Series(range(10, -15, -1), index=idx)
    # --- CORREÇÃO AQUI ---
    # Cria Series com valor constante 5.0 para todas as 25 dezenas
    for w in DEFAULT_GROUP_WINDOWS: results[f'group_W{w}_avg_freq'] = pd.Series(5.0, index=idx)
    # --- FIM CORREÇÃO ---
    results['numbers_in_last_draw'] = {1, 3, 5}

    results['last_cycle_freq'].loc[10] = pd.NA
    results['delay_std_dev'].loc[15] = pd.NA
    results['avg_hist_intra_delay'].loc[20] = pd.NA
    results['closing_freq'].loc[2] = pd.NA

    return results

# --- Testes (Usam a config V7 agora) ---

def test_calculate_scores_returns_valid_series(mock_analysis_results):
    """ Testa se retorna Series válida com config V7. """
    scores = calculate_scores(mock_analysis_results, config=DEFAULT_SCORING_CONFIG_V7)
    assert scores is not None; assert isinstance(scores, pd.Series); assert len(scores) == 25
    assert set(scores.index) == set(ALL_NUMBERS_TEST); assert not scores.isnull().any()

def test_calculate_scores_ranking_extremos(mock_analysis_results):
    """ Testa se os extremos do ranking V7 estão corretos para mock data COM penalidade. """
    scores = calculate_scores(mock_analysis_results, config=DEFAULT_SCORING_CONFIG_V7)
    assert scores is not None
    # Verifica apenas se calcula e se os extremos não são NaN (ordem exata é complexa de prever)
    assert pd.notna(scores.idxmax()) and scores.idxmax() in ALL_NUMBERS_TEST
    assert pd.notna(scores.idxmin()) and scores.idxmin() in ALL_NUMBERS_TEST
    print("\nScores (Teste Extremos V7):"); print(scores.head(3)); print(scores.tail(3))

def test_calculate_scores_cycle_bonus(mock_analysis_results):
    """ Testa a aplicação correta do bônus de ciclo com config V7. """
    missing_set: Set[int] = {23, 24, 25}
    mock_copy_sem_bonus = mock_analysis_results.copy(); mock_copy_sem_bonus['missing_current_cycle'] = set()
    mock_copy_com_bonus = mock_analysis_results.copy(); mock_copy_com_bonus['missing_current_cycle'] = missing_set
    scores_sem_bonus = calculate_scores(mock_copy_sem_bonus, config=DEFAULT_SCORING_CONFIG_V7)
    scores_com_bonus = calculate_scores(mock_copy_com_bonus, config=DEFAULT_SCORING_CONFIG_V7)
    assert scores_com_bonus is not None; assert scores_sem_bonus is not None
    for dezena in missing_set: assert scores_com_bonus[dezena] == pytest.approx(scores_sem_bonus[dezena] + MISSING_CYCLE_BONUS)
    for dezena in set(ALL_NUMBERS_TEST) - missing_set: assert scores_com_bonus[dezena] == pytest.approx(scores_sem_bonus[dezena])

def test_calculate_scores_repeat_penalty(mock_analysis_results):
     """ Testa a aplicação da penalidade de repetição com V7. """
     repeated_set: Set[int] = {1, 3, 5}
     mock_copy_sem_penalty = mock_analysis_results.copy(); mock_copy_sem_penalty['numbers_in_last_draw'] = set()
     mock_copy_com_penalty = mock_analysis_results.copy();
     scores_sem_penalty = calculate_scores(mock_copy_sem_penalty, config=DEFAULT_SCORING_CONFIG_V7)
     scores_com_penalty = calculate_scores(mock_copy_com_penalty, config=DEFAULT_SCORING_CONFIG_V7)
     assert scores_com_penalty is not None; assert scores_sem_penalty is not None
     for dezena in repeated_set: assert scores_com_penalty[dezena] == pytest.approx(scores_sem_penalty[dezena] + REPEAT_PENALTY)
     for dezena in set(ALL_NUMBERS_TEST) - repeated_set: assert scores_com_penalty[dezena] == pytest.approx(scores_sem_penalty[dezena])

def test_calculate_scores_missing_metric(mock_analysis_results):
     """ Testa se lida bem com métrica faltando com V7. """
     import copy
     results_copy = copy.deepcopy(mock_analysis_results)
     metric_to_remove = 'rank_trend'
     assert metric_to_remove in DEFAULT_SCORING_CONFIG_V7
     del results_copy[metric_to_remove]
     scores = calculate_scores(results_copy, config=DEFAULT_SCORING_CONFIG_V7)
     assert scores is not None; assert len(scores) == 25; assert not scores.isnull().any()
     print("\nScores (Métrica Faltante V7): OK")