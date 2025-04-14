# src/scorer.py

import pandas as pd
from typing import Dict, Any, Optional, List
from src.config import logger

ALL_NUMBERS: List[int] = list(range(1, 26))

# --- CONFIGURAÇÃO DE SCORE REFINADA ---
# 'weight': Peso da métrica.
# 'rank_higher_is_better': True se valor ALTO é melhor (ex: Frequência), False se BAIXO é melhor (ex: Atraso Máximo).
# Pontos = 26 - Rank (calculado com base em rank_higher_is_better)
DEFAULT_SCORING_CONFIG_V2: Dict[str, Dict] = {
    'overall_freq':      {'weight': 0.7, 'rank_higher_is_better': True},  # Peso ligeiramente menor
    'recent_freq_50':    {'weight': 1.0, 'rank_higher_is_better': True},  # Freq. média recente
    'recent_freq_25':    {'weight': 1.3, 'rank_higher_is_better': True},  # Freq. recente
    'recent_freq_10':    {'weight': 1.5, 'rank_higher_is_better': True},  # Freq. curtíssimo prazo (peso maior)
    'last_cycle_freq':   {'weight': 0.8, 'rank_higher_is_better': True},  # Freq. ciclo anterior
    'current_cycle_freq':{'weight': 1.2, 'rank_higher_is_better': True},  # Freq. ciclo atual
    'current_delay':     {'weight': 1.5, 'rank_higher_is_better': True},  # Atraso atual com peso maior
    # 'max_delay':         {'weight': 0.0, 'rank_higher_is_better': False}, # Removido/Zerado por enquanto
}

def calculate_scores(analysis_results: Dict[str, Any],
                     config: Optional[Dict[str, Dict]] = None) -> Optional[pd.Series]:
    """
    Calcula uma pontuação para cada dezena (1-25) com base nas análises
    consolidadas e uma configuração de pontuação/pesos.
    """
    # Usa V2 como padrão agora
    if config is None:
        config = DEFAULT_SCORING_CONFIG_V2
    if not analysis_results:
        logger.error("Resultados da análise vazios para calcular scores.")
        return None

    logger.info("Calculando pontuação das dezenas (v2)...")
    final_scores = pd.Series(0.0, index=ALL_NUMBERS)
    final_scores.index.name = 'Dezena'

    for metric, params in config.items():
        weight = params.get('weight', 1.0)
        higher_is_better = params.get('rank_higher_is_better', True)
        if weight == 0: continue # Pula métricas zeradas

        logger.debug(f"Processando: {metric} (Peso:{weight}, MaiorMelhor:{higher_is_better})")
        metric_series = analysis_results.get(metric)

        if metric_series is None: logger.warning(f"Métrica '{metric}' Nula. Pulando."); continue
        if not isinstance(metric_series, pd.Series): logger.warning(f"'{metric}' não é Series. Pulando."); continue

        try: numeric_series = pd.to_numeric(metric_series, errors='coerce')
        except Exception as e: logger.warning(f"Erro converter '{metric}' p/ num: {e}. Pulando."); continue

        numeric_series = numeric_series.reindex(ALL_NUMBERS) # Garante índice 1-25, NAs viram NaN
        if numeric_series.isnull().all(): logger.warning(f"'{metric}' só contém nulos. Pulando."); continue

        # Rank: ascending=False se valor alto for Rank 1, ascending=True se valor baixo for Rank 1
        ranks = numeric_series.rank(method='min', ascending=(not higher_is_better), na_option='bottom')

        # Pontos: Rank 1 = 25 pts, Rank 25 = 1 pt. NaN no rank -> NaN nos pontos
        points = 26 - ranks

        # Aplica peso (NaN * peso = NaN)
        weighted_points = points * weight

        # Adiciona ao score total (NaNs são ignorados na soma por padrão ou tratados com fill_value)
        final_scores = final_scores.add(weighted_points, fill_value=0) # fill_value=0 trata NaN inicial ou NaN nos pontos ponderados
        # logger.debug(f"Scores após {metric}: \n{final_scores.head()}") # Log verboso

    final_scores.sort_values(ascending=False, inplace=True)
    final_scores.fillna(0, inplace=True) # Garante que não haja NaN no resultado final

    logger.info("Cálculo de pontuação final (v2) concluído.")
    return final_scores