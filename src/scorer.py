# src/scorer.py

import pandas as pd
from typing import Dict, Any, Optional, List # Adicionado List
from src.config import logger

# Define ALL_NUMBERS aqui também para garantir o índice completo
ALL_NUMBERS: List[int] = list(range(1, 26))

# Configuração inicial de pontuação e pesos
# 'weight': Peso da métrica na pontuação final.
# 'rank_higher_is_better': True se valor ALTO da métrica for melhor (recebe Rank 1), False se valor BAIXO for melhor.
DEFAULT_SCORING_CONFIG: Dict[str, Dict] = {
    # Metrica: {peso: float, rank_maior_melhor: bool}
    'overall_freq':     {'weight': 1.0, 'rank_higher_is_better': True},  # Mais frequente = melhor
    'recent_freq_25':   {'weight': 1.5, 'rank_higher_is_better': True},  # Mais frequente recente = melhor (peso maior)
    'last_cycle_freq':  {'weight': 0.8, 'rank_higher_is_better': True},  # Mais frequente no último ciclo = melhor
    'current_cycle_freq':{'weight': 1.2, 'rank_higher_is_better': True},  # Mais frequente no ciclo atual = melhor
    'current_delay':    {'weight': 1.0, 'rank_higher_is_better': True},  # Mais atrasado = melhor
    'max_delay':        {'weight': 0.5, 'rank_higher_is_better': False}, # Menor atraso máximo = melhor (mais 'confiável'?)
}
# Fórmula de Pontos: Pontos = 26 - Rank (Rank 1 sempre recebe 25 pontos)

def calculate_scores(analysis_results: Dict[str, Any],
                     config: Optional[Dict[str, Dict]] = None) -> Optional[pd.Series]:
    """
    Calcula uma pontuação para cada dezena (1-25) com base nos resultados
    consolidados das análises e uma configuração de pontuação/pesos.

    Args:
        analysis_results (Dict[str, Any]): Dicionário retornado por get_consolidated_analysis.
        config (Optional[Dict[str, Dict]]): Configuração de pontuação. Usa DEFAULT se None.

    Returns:
        Optional[pd.Series]: Series com a pontuação final de cada dezena (1-25),
                             ordenada descendentemente pela pontuação, ou None se erro.
    """
    if config is None:
        config = DEFAULT_SCORING_CONFIG
    if not analysis_results:
        logger.error("Resultados da análise estão vazios. Não é possível calcular scores.")
        return None

    logger.info("Calculando pontuação das dezenas com base nas análises...")
    # Inicializa scores com zero para todas as dezenas
    final_scores = pd.Series(0.0, index=ALL_NUMBERS)
    final_scores.index.name = 'Dezena' # Nomeia o índice

    for metric, params in config.items():
        weight = params.get('weight', 1.0)
        higher_is_better = params.get('rank_higher_is_better', True) # Padrão: valor alto é melhor

        if weight == 0:
            logger.debug(f"Métrica '{metric}' com peso 0. Pulando.")
            continue

        logger.debug(f"Processando métrica: {metric} (Peso: {weight}, MaiorMelhor: {higher_is_better})")

        metric_series = analysis_results.get(metric)

        # --- Validação da Série da Métrica ---
        if metric_series is None:
            logger.warning(f"Métrica '{metric}' não encontrada ou nula. Pulando.")
            continue
        if not isinstance(metric_series, pd.Series):
             logger.warning(f"Dado para '{metric}' não é Series. Pulando. Tipo: {type(metric_series)}")
             continue
        # Garante que temos dados numéricos para rankear
        try:
            # Tenta converter para numérico, NAs viram NaN
            numeric_series = pd.to_numeric(metric_series, errors='coerce')
        except Exception as e:
             logger.warning(f"Erro ao converter '{metric}' para numérico: {e}. Pulando.")
             continue

        # Garante índice completo 1-25 e trata NaNs antes de rankear
        # NaNs no rank serão colocados no final (na_option='bottom' é default)
        numeric_series = numeric_series.reindex(ALL_NUMBERS) # fill_value=NaN padrão

        if numeric_series.isnull().all():
            logger.warning(f"Série para métrica '{metric}' só contém nulos após reindex/conversão. Pulando.")
            continue
        # ---------------------------------------

        # Calcula o rank: ascending=False se maior for melhor (rank 1 para o maior)
        #                 ascending=True se menor for melhor (rank 1 para o menor)
        ranks = numeric_series.rank(method='min', ascending=(not higher_is_better), na_option='bottom')

        # Calcula os pontos: 26 - rank (Rank 1 = 25 pts, Rank 25 = 1 pt)
        # Se o rank for NaN (valor original era NA), os pontos serão NaN
        points = 26 - ranks

        # Aplica o peso. Se pontos for NaN, continua NaN.
        weighted_points = points * weight

        # Adiciona ao score final, tratando NaNs (NaNs não somam, ficam NaN ou são ignorados dependendo da versão do Pandas/Numpy)
        # Usar add com fill_value=0 garante que NaNs na pontuação ponderada não anulem o score existente
        final_scores = final_scores.add(weighted_points, fill_value=0)
        logger.debug(f"Scores atualizados após {metric}: \n{final_scores.head()}")


    # Ordena pela pontuação final (descendente)
    final_scores.sort_values(ascending=False, inplace=True)

    # Trata possíveis NaNs restantes no score final (se todas as métricas foram NaN para um número)
    final_scores.fillna(0, inplace=True) # Atribui 0 se score final for NaN

    logger.info("Cálculo de pontuação final concluído.")
    return final_scores