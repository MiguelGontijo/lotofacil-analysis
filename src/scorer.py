# src/scorer.py

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Set

# Importa do config
from src.config import logger, ALL_NUMBERS, AGGREGATOR_WINDOWS, DEFAULT_GROUP_WINDOWS

# Fallbacks
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'AGGREGATOR_WINDOWS' not in globals(): AGGREGATOR_WINDOWS = [10, 25, 50, 100, 200, 300, 400, 500]
if 'DEFAULT_GROUP_WINDOWS' not in globals(): DEFAULT_GROUP_WINDOWS = [25, 100] # Usa apenas W25 agora


# --- CONFIGURAÇÃO DE SCORE V7 - Refinada ---
DEFAULT_SCORING_CONFIG_V7: Dict[str, Dict] = {
    # Metrica: {peso: float, rank_higher_is_better: bool}
    'overall_freq':      {'weight': 0.3, 'rank_higher_is_better': True}, # Reduzido
    # Frequências Recentes - Mantendo ênfase nas curtas
    **{f'recent_freq_{w}': {'weight': max(0.1, 1.0 - (w/100)*0.3), 'rank_higher_is_better': True} for w in AGGREGATOR_WINDOWS if w >= 100}, # Pesos 0.7 a 0.1
    'recent_freq_50':    {'weight': 1.0, 'rank_higher_is_better': True}, # Base
    'recent_freq_25':    {'weight': 1.3, 'rank_higher_is_better': True}, # Importante
    'recent_freq_10':    {'weight': 1.5, 'rank_higher_is_better': True}, # Mais importante
    # Tendências
    'freq_trend':        {'weight': 0.8, 'rank_higher_is_better': True},
    'rank_trend':        {'weight': 1.2, 'rank_higher_is_better': True}, # Valorizado
    # Atrasos
    'current_delay':     {'weight': 1.6, 'rank_higher_is_better': True}, # Muito importante
    'delay_std_dev':     {'weight': 0.3, 'rank_higher_is_better': False},# Peso bem reduzido (pouca variância)
    # Ciclos
    'last_cycle_freq':   {'weight': 0.4, 'rank_higher_is_better': True}, # Menos peso
    'current_cycle_freq':{'weight': 1.0, 'rank_higher_is_better': True},
    'current_intra_cycle_delay': {'weight': 1.4, 'rank_higher_is_better': True}, # Importante
    'closing_freq':      {'weight': 0.9, 'rank_higher_is_better': True}, # Mais peso (alta variância)
    'sole_closing_freq': {'weight': 0.4, 'rank_higher_is_better': True}, # Mais peso
    # Grupos (Usando SÓ a janela W25, que mostrou mais dinâmica)
    'group_W25_avg_freq':{'weight': 0.5, 'rank_higher_is_better': True}, # Peso moderado
    # Repetição (Taxa histórica)
    'repetition_rate':   {'weight': -0.7, 'rank_higher_is_better': True}, # Taxa alta = Ruim (peso negativo maior)

    # --- Métricas com peso 0 (não usadas no score V7 refinado) ---
    **{f'group_W{w}_avg_freq': {'weight': 0.0, 'rank_higher_is_better': True} for w in DEFAULT_GROUP_WINDOWS if w != 25}, # Zera outras janelas de grupo
    # 'avg_hist_intra_delay': {'weight': 0.0, 'rank_higher_is_better': False},
    # 'max_hist_intra_delay': {'weight': 0.0, 'rank_higher_is_better': True},
    # 'delay_mean': {'weight': 0.0, 'rank_higher_is_better': True},
    # 'max_delay': {'weight': 0.0, 'rank_higher_is_better': False},
}

MISSING_CYCLE_BONUS = 5.0
REPEAT_PENALTY = -15.0 # Penalidade fixa (poderia ser dinâmica?)

def calculate_scores(analysis_results: Dict[str, Any],
                     config: Optional[Dict[str, Dict]] = None) -> Optional[pd.Series]:
    """ Calcula pontuação V7 (Refinada). """
    if config is None: config = DEFAULT_SCORING_CONFIG_V7 # <<< USA V7 Refinada
    if not analysis_results: logger.error("Resultados da análise vazios."); return None

    logger.info("Calculando pontuação das dezenas (v7 Refinada)...")
    final_scores = pd.Series(0.0, index=ALL_NUMBERS); final_scores.index.name = 'Dezena'

    # Calcula scores baseados nas métricas e pesos
    for metric, params in config.items():
        weight = params.get('weight', 1.0); higher_is_better = params.get('rank_higher_is_better', True)
        if weight == 0: continue # Pula métricas com peso zero
        if metric not in analysis_results or analysis_results[metric] is None: logger.warning(f"Métrica '{metric}' Nula/Ausente."); continue

        metric_series = analysis_results[metric]; logger.debug(f"Proc: {metric} (W:{weight}, HighBest:{higher_is_better})")
        if not isinstance(metric_series, pd.Series): logger.warning(f"'{metric}' não é Series."); continue

        fill_val = -np.inf if higher_is_better else np.inf
        try: numeric_series = pd.to_numeric(metric_series, errors='coerce').fillna(fill_val)
        except Exception as e: logger.warning(f"Erro converter/fillna '{metric}': {e}."); continue

        numeric_series = numeric_series.reindex(ALL_NUMBERS, fill_value=fill_val)
        if numeric_series.isin([np.inf, -np.inf, np.nan]).all(): logger.warning(f"'{metric}' só contém inf/nan."); continue

        ranks = numeric_series.rank(method='min', ascending=(not higher_is_better), na_option='bottom')
        points = 26 - ranks
        weighted_points = points * weight
        weighted_points.replace([np.inf, -np.inf], 0, inplace=True)
        final_scores = final_scores.add(weighted_points.fillna(0), fill_value=0)

    # Aplica Bônus de Ciclo
    missing_in_cycle: Optional[Set[int]] = analysis_results.get('missing_current_cycle')
    if missing_in_cycle is not None and len(missing_in_cycle) > 0 and len(missing_in_cycle) < 25:
        logger.info(f"Aplicando bônus {MISSING_CYCLE_BONUS} pts p/ {len(missing_in_cycle)} faltantes.")
        final_scores.loc[list(missing_in_cycle.intersection(final_scores.index))] += MISSING_CYCLE_BONUS

    # Aplica Penalidade de Repetição
    numbers_last_draw: Optional[Set[int]] = analysis_results.get('numbers_in_last_draw')
    if numbers_last_draw is not None and REPEAT_PENALTY != 0:
         logger.info(f"Aplicando penalidade {REPEAT_PENALTY} pts p/ {len(numbers_last_draw)} repetidas.")
         final_scores.loc[list(numbers_last_draw.intersection(final_scores.index))] += REPEAT_PENALTY

    final_scores.sort_values(ascending=False, inplace=True)
    final_scores.fillna(0, inplace=True)
    logger.info("Cálculo de pontuação final (v7 Refinada) concluído.")
    return final_scores