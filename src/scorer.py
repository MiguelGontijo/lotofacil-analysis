# src/scorer.py

import pandas as pd
import numpy as np # Importa numpy
from typing import Dict, Any, Optional, List, Set

# Importa do config
from src.config import logger, ALL_NUMBERS, AGGREGATOR_WINDOWS, DEFAULT_GROUP_WINDOWS

# Fallbacks
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'AGGREGATOR_WINDOWS' not in globals(): AGGREGATOR_WINDOWS = [10, 25, 50, 100, 200, 300, 400, 500]
if 'DEFAULT_GROUP_WINDOWS' not in globals(): DEFAULT_GROUP_WINDOWS = [25, 100] # Precisa do fallback

# --- CONFIGURAÇÃO DE SCORE V8 ---
# Integra Rank Trend e Group Stats
DEFAULT_SCORING_CONFIG_V8: Dict[str, Dict] = {
    # Metrica: {peso: float, rank_higher_is_better: bool}

    # Frequências
    'overall_freq':      {'weight': 0.4, 'rank_higher_is_better': True},
    **{f'recent_freq_{w}': {'weight': max(0.1, 1.5 - (w/100)*0.4), 'rank_higher_is_better': True} for w in AGGREGATOR_WINDOWS if w >= 100},
    'recent_freq_50':    {'weight': 1.0, 'rank_higher_is_better': True},
    'recent_freq_25':    {'weight': 1.2, 'rank_higher_is_better': True},
    'recent_freq_10':    {'weight': 1.4, 'rank_higher_is_better': True},

    # Tendências
    'freq_trend':        {'weight': 0.8, 'rank_higher_is_better': True}, # Tendência de Freq. (W10/W50)
    'rank_trend':        {'weight': 1.2, 'rank_higher_is_better': True}, # <<< NOVO: Rank melhorando = bom (peso forte)

    # Atrasos
    'current_delay':     {'weight': 1.5, 'rank_higher_is_better': True}, # Atraso atual continua forte
    'delay_std_dev':     {'weight': 0.7, 'rank_higher_is_better': False},# Baixa variabilidade = bom (peso ajustado)

    # Ciclos
    'last_cycle_freq':   {'weight': 0.5, 'rank_higher_is_better': True}, # Peso menor
    'current_cycle_freq':{'weight': 0.8, 'rank_higher_is_better': True}, # Peso menor
    'current_intra_cycle_delay': {'weight': 1.3, 'rank_higher_is_better': True},
    'closing_freq':      {'weight': 0.6, 'rank_higher_is_better': True}, # Ser fechador frequente = bom?
    'sole_closing_freq': {'weight': 0.2, 'rank_higher_is_better': True}, # Ser fechador único = levemente bom?

    # <<< NOVOS: Stats de Grupo e Repetição >>>
    # Usa as janelas padrão definidas em DEFAULT_GROUP_WINDOWS
    **{f'group_W{w}_avg_freq': {'weight': 0.4, 'rank_higher_is_better': True} for w in DEFAULT_GROUP_WINDOWS}, # Grupo "quente" = bom (peso moderado)
    'repetition_rate':   {'weight': -0.5, 'rank_higher_is_better': True}, # <<< NOVO: Taxa alta = ruim (peso negativo)

    # Métricas ainda não usadas (pesos zero)
    # 'avg_hist_intra_delay': {'weight': 0.0, 'rank_higher_is_better': False},
    # 'max_hist_intra_delay': {'weight': 0.0, 'rank_higher_is_better': True},
    # 'delay_mean': {'weight': 0.0, 'rank_higher_is_better': True},
    # 'max_delay': {'weight': 0.0, 'rank_higher_is_better': False},
}

MISSING_CYCLE_BONUS = 5.0
REPEAT_PENALTY = -15.0 # Penalidade fixa por estar no último sorteio

def calculate_scores(analysis_results: Dict[str, Any],
                     config: Optional[Dict[str, Dict]] = None) -> Optional[pd.Series]:
    """ Calcula pontuação V8: com rank trend, group stats e repetition rate. """
    if config is None: config = DEFAULT_SCORING_CONFIG_V8 # <<< USA V8
    if not analysis_results: logger.error("Resultados da análise vazios."); return None

    logger.info("Calculando pontuação das dezenas (v8)...")
    final_scores = pd.Series(0.0, index=ALL_NUMBERS); final_scores.index.name = 'Dezena'

    # Calcula scores baseados nas métricas rankeáveis
    for metric, params in config.items():
        weight = params.get('weight', 1.0); higher_is_better = params.get('rank_higher_is_better', True)
        if weight == 0: continue
        if metric not in analysis_results or analysis_results[metric] is None: logger.warning(f"Métrica '{metric}' Nula/Ausente."); continue

        metric_series = analysis_results[metric]; logger.debug(f"Proc: {metric} (W:{weight}, HighBest:{higher_is_better})")
        if not isinstance(metric_series, pd.Series): logger.warning(f"'{metric}' não é Series."); continue

        # Trata NaNs antes de rankear para evitar problemas, preenchendo com valor neutro/ruim
        # Para "higher is better", NaN vira -infinito. Para "lower is better", NaN vira +infinito.
        fill_val = -np.inf if higher_is_better else np.inf
        try: numeric_series = pd.to_numeric(metric_series, errors='coerce').fillna(fill_val)
        except Exception as e: logger.warning(f"Erro converter/fillna '{metric}': {e}."); continue

        # Recalcula ranks caso haja infinitos (fillna pode introduzir)
        # Usar rank 'dense' pode ser uma alternativa se 'min' der problema com inf
        numeric_series = numeric_series.reindex(ALL_NUMBERS, fill_value=fill_val) # Garante índice
        if numeric_series.isin([np.inf, -np.inf]).all(): logger.warning(f"'{metric}' só contém inf/nan."); continue # Pula se só tem inf

        ranks = numeric_series.rank(method='min', ascending=(not higher_is_better), na_option='bottom')
        points = 26 - ranks
        weighted_points = points * weight
        # Corrige pontos infinitos resultantes de ranks infinitos (se fill_value foi inf)
        weighted_points.replace([np.inf, -np.inf], 0, inplace=True)
        final_scores = final_scores.add(weighted_points.fillna(0), fill_value=0) # fillna extra

    # Aplica Bônus de Ciclo
    missing_in_cycle: Optional[Set[int]] = analysis_results.get('missing_current_cycle')
    if missing_in_cycle is not None and len(missing_in_cycle) > 0 and len(missing_in_cycle) < 25:
        logger.info(f"Aplicando bônus {MISSING_CYCLE_BONUS} pts para {len(missing_in_cycle)} dezenas faltantes.")
        final_scores.loc[list(missing_in_cycle.intersection(final_scores.index))] += MISSING_CYCLE_BONUS

    # Aplica Penalidade de Repetição (estar no último sorteio)
    numbers_last_draw: Optional[Set[int]] = analysis_results.get('numbers_in_last_draw')
    if numbers_last_draw is not None and REPEAT_PENALTY != 0:
         logger.info(f"Aplicando penalidade {REPEAT_PENALTY} pts para {len(numbers_last_draw)} dezenas repetidas (último sorteio).")
         final_scores.loc[list(numbers_last_draw.intersection(final_scores.index))] += REPEAT_PENALTY

    # Ordena e finaliza
    final_scores.sort_values(ascending=False, inplace=True)
    final_scores.fillna(0, inplace=True) # Garante que não haja NaNs
    logger.info("Cálculo de pontuação final (v8) concluído.")
    return final_scores