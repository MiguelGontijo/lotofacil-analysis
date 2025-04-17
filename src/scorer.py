# src/scorer.py

import pandas as pd
from typing import Dict, Any, Optional, List, Set

# Importa do config
from src.config import logger, ALL_NUMBERS, AGGREGATOR_WINDOWS, DEFAULT_GROUP_WINDOWS

# Fallbacks
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'AGGREGATOR_WINDOWS' not in globals(): AGGREGATOR_WINDOWS = [10, 25, 50, 100, 200, 300, 400, 500]
if 'DEFAULT_GROUP_WINDOWS' not in globals(): DEFAULT_GROUP_WINDOWS = [25, 100] # Adicionado fallback


# --- CONFIGURAÇÃO DE SCORE V7 (Reflete adição de closing_stats) ---
DEFAULT_SCORING_CONFIG_V7: Dict[str, Dict] = { # <<< RENOMEADO AQUI
    # Metrica: {peso: float, rank_higher_is_better: bool}
    'overall_freq':      {'weight': 0.5, 'rank_higher_is_better': True},
    # Usa as janelas definidas em AGGREGATOR_WINDOWS
    **{f'recent_freq_{w}': {'weight': max(0.1, 1.5 - (w/100)*0.4), 'rank_higher_is_better': True} for w in AGGREGATOR_WINDOWS if w >= 100},
    'recent_freq_50':    {'weight': 1.0, 'rank_higher_is_better': True},
    'recent_freq_25':    {'weight': 1.2, 'rank_higher_is_better': True},
    'recent_freq_10':    {'weight': 1.4, 'rank_higher_is_better': True},
    'freq_trend':        {'weight': 0.8, 'rank_higher_is_better': True},
    'rank_trend':        {'weight': 1.0, 'rank_higher_is_better': True}, # Adicionado Rank Trend
    'last_cycle_freq':   {'weight': 0.6, 'rank_higher_is_better': True},
    'current_cycle_freq':{'weight': 1.0, 'rank_higher_is_better': True},
    'current_delay':     {'weight': 1.3, 'rank_higher_is_better': True},
    'delay_std_dev':     {'weight': 0.8, 'rank_higher_is_better': False},
    'current_intra_cycle_delay': {'weight': 1.5, 'rank_higher_is_better': True},
    # 'avg_hist_intra_delay': {'weight': 0.0, 'rank_higher_is_better': False}, # Ainda não usado
    # 'max_hist_intra_delay': {'weight': 0.0, 'rank_higher_is_better': True}, # Ainda não usado
    'closing_freq':      {'weight': 0.7, 'rank_higher_is_better': True},
    'sole_closing_freq': {'weight': 0.3, 'rank_higher_is_better': True},
    # Adiciona Group Stats (usando as janelas padrão definidas no config)
    **{f'group_W{w}_avg_freq': {'weight': 0.3, 'rank_higher_is_better': True} for w in DEFAULT_GROUP_WINDOWS}, # Peso baixo para média do grupo
}

MISSING_CYCLE_BONUS = 5.0
REPEAT_PENALTY = -15.0

def calculate_scores(analysis_results: Dict[str, Any],
                     config: Optional[Dict[str, Dict]] = None) -> Optional[pd.Series]:
    """ Calcula pontuação V7. """
    # <<< USA V7 COMO PADRÃO >>>
    if config is None: config = DEFAULT_SCORING_CONFIG_V7
    if not analysis_results: logger.error("Resultados da análise vazios."); return None

    logger.info("Calculando pontuação das dezenas (v7)...")
    final_scores = pd.Series(0.0, index=ALL_NUMBERS); final_scores.index.name = 'Dezena'

    # Calcula scores baseados nas métricas e pesos
    for metric, params in config.items():
        # Pula métricas de grupo que serão tratadas depois ou que não são rankeáveis diretamente
        # if metric.startswith('group_'): continue # Vamos tentar rankear a Series do grupo diretamente

        weight = params.get('weight', 1.0); higher_is_better = params.get('rank_higher_is_better', True)
        if weight == 0: continue
        if metric not in analysis_results or analysis_results[metric] is None: logger.warning(f"Métrica '{metric}' Nula/Ausente."); continue

        metric_series = analysis_results[metric]; logger.debug(f"Proc: {metric} (W:{weight}, HighBest:{higher_is_better})")
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
        final_scores.loc[list(missing_in_cycle.intersection(final_scores.index))] += MISSING_CYCLE_BONUS

    # Aplica Penalidade de Repetição
    numbers_last_draw: Optional[Set[int]] = analysis_results.get('numbers_in_last_draw')
    if numbers_last_draw is not None and REPEAT_PENALTY != 0:
         logger.info(f"Aplicando penalidade {REPEAT_PENALTY} pts para {len(numbers_last_draw)} dezenas repetidas.")
         final_scores.loc[list(numbers_last_draw.intersection(final_scores.index))] += REPEAT_PENALTY

    final_scores.sort_values(ascending=False, inplace=True)
    final_scores.fillna(0, inplace=True)
    logger.info("Cálculo de pontuação final (v7) concluído.")
    return final_scores