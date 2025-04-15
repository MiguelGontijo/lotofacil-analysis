# src/scorer.py

import pandas as pd
from typing import Dict, Any, Optional, List, Set # Adicionado Set

# Importa APENAS o logger e ALL_NUMBERS do config
from src.config import logger, ALL_NUMBERS

# Fallback (embora ALL_NUMBERS deva vir do config agora)
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))

# --- CONFIGURAÇÃO DE SCORE V4 ---
DEFAULT_SCORING_CONFIG_V4: Dict[str, Dict] = {
    # Metrica: {peso: float, rank_higher_is_better: bool}
    'overall_freq':      {'weight': 0.5, 'rank_higher_is_better': True},
    'recent_freq_200':   {'weight': 0.5, 'rank_higher_is_better': True},
    'recent_freq_100':   {'weight': 0.7, 'rank_higher_is_better': True},
    'recent_freq_50':    {'weight': 1.0, 'rank_higher_is_better': True},
    'recent_freq_25':    {'weight': 1.3, 'rank_higher_is_better': True},
    'recent_freq_10':    {'weight': 1.5, 'rank_higher_is_better': True},
    'freq_trend':        {'weight': 1.0, 'rank_higher_is_better': True},
    'last_cycle_freq':   {'weight': 0.7, 'rank_higher_is_better': True},
    'current_cycle_freq':{'weight': 1.3, 'rank_higher_is_better': True},
    'current_delay':     {'weight': 1.5, 'rank_higher_is_better': True},
    'delay_std_dev':     {'weight': 1.0, 'rank_higher_is_better': False},
}
MISSING_CYCLE_BONUS = 5.0

# <<< LINHA REMOVIDA: from src.scorer import ... >>>

def calculate_scores(analysis_results: Dict[str, Any],
                     config: Optional[Dict[str, Dict]] = None) -> Optional[pd.Series]:
    """ Calcula pontuação V4: mais janelas, tendência, bônus ciclo, std dev atraso. """
    if config is None: config = DEFAULT_SCORING_CONFIG_V4
    if not analysis_results: logger.error("Resultados da análise vazios."); return None

    logger.info("Calculando pontuação das dezenas (v4)...")
    final_scores = pd.Series(0.0, index=ALL_NUMBERS); final_scores.index.name = 'Dezena'

    for metric, params in config.items():
        weight = params.get('weight', 1.0)
        higher_is_better = params.get('rank_higher_is_better', True)
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

    missing_in_cycle: Optional[Set[int]] = analysis_results.get('missing_current_cycle')
    if missing_in_cycle is not None and len(missing_in_cycle) > 0 and len(missing_in_cycle) < 25:
        logger.info(f"Aplicando bônus de {MISSING_CYCLE_BONUS} pts para {len(missing_in_cycle)} dezenas faltantes.")
        for num in missing_in_cycle:
            if num in ALL_NUMBERS and num in final_scores.index: final_scores[num] += MISSING_CYCLE_BONUS

    final_scores.sort_values(ascending=False, inplace=True)
    final_scores.fillna(0, inplace=True)

    logger.info("Cálculo de pontuação final (v4) concluído.")
    return final_scores