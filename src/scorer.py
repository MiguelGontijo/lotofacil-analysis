# src/scorer.py

import pandas as pd
from typing import Dict, Any, Optional, List, Set
from src.config import logger, ALL_NUMBERS, AGGREGATOR_WINDOWS # Usa novas janelas do config

if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
# Garante que AGGREGATOR_WINDOWS exista se não vier do config
if 'AGGREGATOR_WINDOWS' not in globals(): AGGREGATOR_WINDOWS = [10, 25, 50, 100, 200]


# --- CONFIGURAÇÃO DE SCORE V5 ---
# Inclui mais janelas e atraso intra-ciclo
DEFAULT_SCORING_CONFIG_V5: Dict[str, Dict] = {
    'overall_freq':      {'weight': 0.5, 'rank_higher_is_better': True},
    'recent_freq_500':   {'weight': 0.3, 'rank_higher_is_better': True}, # Janelas mais longas
    'recent_freq_400':   {'weight': 0.4, 'rank_higher_is_better': True},
    'recent_freq_300':   {'weight': 0.5, 'rank_higher_is_better': True},
    'recent_freq_200':   {'weight': 0.6, 'rank_higher_is_better': True},
    'recent_freq_100':   {'weight': 0.8, 'rank_higher_is_better': True},
    'recent_freq_50':    {'weight': 1.0, 'rank_higher_is_better': True},
    'recent_freq_25':    {'weight': 1.3, 'rank_higher_is_better': True},
    'recent_freq_10':    {'weight': 1.5, 'rank_higher_is_better': True},
    'freq_trend':        {'weight': 1.0, 'rank_higher_is_better': True}, # Mantém tendência
    'last_cycle_freq':   {'weight': 0.7, 'rank_higher_is_better': True},
    'current_cycle_freq':{'weight': 1.0, 'rank_higher_is_better': True}, # Peso ligeiramente menor?
    'current_delay':     {'weight': 1.2, 'rank_higher_is_better': True}, # Peso ligeiramente menor?
    'delay_std_dev':     {'weight': 1.0, 'rank_higher_is_better': False},# Menor std dev = melhor
    'current_intra_cycle_delay': {'weight': 1.5, 'rank_higher_is_better': True}, # <<< NOVO: Atraso no ciclo atual (peso alto)
}

MISSING_CYCLE_BONUS = 5.0 # Mantém bônus

def calculate_scores(analysis_results: Dict[str, Any],
                     config: Optional[Dict[str, Dict]] = None) -> Optional[pd.Series]:
    """ Calcula pontuação V5: com mais janelas e atraso intra-ciclo. """
    if config is None: config = DEFAULT_SCORING_CONFIG_V5 # <<< USA V5
    if not analysis_results: logger.error("Resultados da análise vazios."); return None

    logger.info("Calculando pontuação das dezenas (v5)...")
    final_scores = pd.Series(0.0, index=ALL_NUMBERS); final_scores.index.name = 'Dezena'

    for metric, params in config.items():
        weight = params.get('weight', 1.0)
        higher_is_better = params.get('rank_higher_is_better', True)
        if weight == 0: continue

        # Verifica se a métrica existe nos resultados antes de processar
        if metric not in analysis_results or analysis_results[metric] is None:
             logger.warning(f"Métrica '{metric}' não encontrada ou Nula nos resultados. Pulando.")
             continue

        metric_series = analysis_results[metric]
        logger.debug(f"Proc: {metric} (W:{weight}, HighBest:{higher_is_better})")

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

    final_scores.sort_values(ascending=False, inplace=True)
    final_scores.fillna(0, inplace=True)

    logger.info("Cálculo de pontuação final (v5) concluído.")
    return final_scores