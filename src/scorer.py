# src/scorer.py

import pandas as pd
from typing import Dict, Any, Optional, List, Set
# Usa ALL_NUMBERS do config
from src.config import logger, ALL_NUMBERS, AGGREGATOR_WINDOWS

if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
# Garante que AGGREGATOR_WINDOWS exista
if 'AGGREGATOR_WINDOWS' not in globals(): AGGREGATOR_WINDOWS = [10, 25, 50, 100, 200]

# --- CONFIGURAÇÃO DE SCORE V6 ---
# Inclui mais janelas, stats hist intra-ciclo, penalidade por repetição
DEFAULT_SCORING_CONFIG_V6: Dict[str, Dict] = {
    # Metrica: {peso: float, rank_higher_is_better: bool}
    'overall_freq':      {'weight': 0.5, 'rank_higher_is_better': True},
    # Frequências Recentes com pesos decrescentes
    **{f'recent_freq_{w}': {'weight': max(0.1, 1.5 - (w/100)*0.5), 'rank_higher_is_better': True} for w in AGGREGATOR_WINDOWS if w >= 50}, # Pesos de 1.25 a 0.5
    'recent_freq_25':    {'weight': 1.3, 'rank_higher_is_better': True},
    'recent_freq_10':    {'weight': 1.5, 'rank_higher_is_better': True},
    'freq_trend':        {'weight': 1.0, 'rank_higher_is_better': True},
    'last_cycle_freq':   {'weight': 0.7, 'rank_higher_is_better': True},
    'current_cycle_freq':{'weight': 1.0, 'rank_higher_is_better': True}, # Peso reduzido
    'current_delay':     {'weight': 1.3, 'rank_higher_is_better': True}, # Peso reduzido
    'delay_std_dev':     {'weight': 0.8, 'rank_higher_is_better': False},# Menor std dev = melhor
    'current_intra_cycle_delay': {'weight': 1.5, 'rank_higher_is_better': True}, # Atraso no ciclo atual forte
    # Novas Métricas Intra-Ciclo Históricas
    'avg_hist_intra_delay': {'weight': 0.5, 'rank_higher_is_better': False}, # Média baixa = melhor
    'max_hist_intra_delay': {'weight': -0.3, 'rank_higher_is_better': True}, # Máximo alto = pior (peso negativo)
}

# Bônus para números faltantes no ciclo atual
MISSING_CYCLE_BONUS = 5.0
# Penalidade para números que saíram no último sorteio
REPEAT_PENALTY = -15.0 # Penalidade significativa

def calculate_scores(analysis_results: Dict[str, Any],
                     config: Optional[Dict[str, Dict]] = None) -> Optional[pd.Series]:
    """ Calcula pontuação V6: mais janelas, stats intra-ciclo hist, penalidade de repetição. """
    if config is None: config = DEFAULT_SCORING_CONFIG_V6 # <<< USA V6
    if not analysis_results: logger.error("Resultados da análise vazios."); return None

    logger.info("Calculando pontuação das dezenas (v6)...")
    final_scores = pd.Series(0.0, index=ALL_NUMBERS); final_scores.index.name = 'Dezena'

    # Calcula scores baseados nas métricas e pesos
    for metric, params in config.items():
        weight = params.get('weight', 1.0); higher_is_better = params.get('rank_higher_is_better', True)
        if weight == 0: continue
        logger.debug(f"Proc: {metric} (W:{weight}, HighBest:{higher_is_better})")
        metric_series = analysis_results.get(metric)
        if metric_series is None: logger.warning(f"Métrica '{metric}' Nula."); continue
        if not isinstance(metric_series, pd.Series): logger.warning(f"'{metric}' não é Series."); continue
        try: numeric_series = pd.to_numeric(metric_series, errors='coerce')
        except Exception as e: logger.warning(f"Erro converter '{metric}': {e}."); continue
        numeric_series = numeric_series.reindex(ALL_NUMBERS)
        if numeric_series.isnull().all(): logger.warning(f"'{metric}' só contém nulos."); continue
        ranks = numeric_series.rank(method='min', ascending=(not higher_is_better), na_option='bottom')
        points = 26 - ranks
        weighted_points = points * weight
        final_scores = final_scores.add(weighted_points, fill_value=0)

    # Aplica Bônus de Ciclo
    missing_in_cycle: Optional[Set[int]] = analysis_results.get('missing_current_cycle')
    if missing_in_cycle is not None and len(missing_in_cycle) > 0 and len(missing_in_cycle) < 25:
        logger.info(f"Aplicando bônus {MISSING_CYCLE_BONUS} pts para {len(missing_in_cycle)} dezenas faltantes.")
        for num in missing_in_cycle:
            if num in ALL_NUMBERS and num in final_scores.index: final_scores[num] += MISSING_CYCLE_BONUS

    # *** NOVA: Aplica Penalidade de Repetição ***
    numbers_last_draw: Optional[Set[int]] = analysis_results.get('numbers_in_last_draw')
    if numbers_last_draw is not None and REPEAT_PENALTY != 0:
         logger.info(f"Aplicando penalidade de {REPEAT_PENALTY} pts para {len(numbers_last_draw)} dezenas do último sorteio.")
         for num in numbers_last_draw:
              if num in final_scores.index: final_scores[num] += REPEAT_PENALTY # Adiciona valor negativo
         # Garante que score não fique muito negativo? Ou deixa negativo? Deixa por enquanto.
         # final_scores = final_scores.clip(lower=0) # Opcional: não deixar score ser < 0

    final_scores.sort_values(ascending=False, inplace=True)
    final_scores.fillna(0, inplace=True)

    logger.info("Cálculo de pontuação final (v6) concluído.")
    return final_scores