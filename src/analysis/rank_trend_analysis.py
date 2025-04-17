# src/analysis/rank_trend_analysis.py

import pandas as pd
from typing import Optional

# Importa do config e frequency_analysis
from src.config import logger, ALL_NUMBERS
from src.analysis.frequency_analysis import get_cumulative_frequency

# Fallback
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))

DEFAULT_RANK_TREND_LOOKBACK = 50 # Período padrão para olhar para trás

def calculate_overall_rank_trend(concurso_maximo: int,
                                 lookback: int = DEFAULT_RANK_TREND_LOOKBACK
                                 ) -> Optional[pd.Series]:
    """
    Calcula a variação no rank da frequência geral acumulada entre
    o concurso_maximo e (concurso_maximo - lookback).

    Rank 1 = Mais frequente.
    Trend = Rank Anterior - Rank Atual. (Positivo = Melhorou o Rank)

    Args:
        concurso_maximo (int): O concurso final para análise.
        lookback (int): Quantos concursos olhar para trás para comparar o rank.

    Returns:
        Optional[pd.Series]: Series com a variação de rank para cada dezena,
                             ou None se não houver dados suficientes.
    """
    logger.info(f"Calculando tendência de rank geral (lookback={lookback}) até {concurso_maximo}...")

    concurso_anterior = concurso_maximo - lookback

    # Verifica se temos dados suficientes para os dois pontos
    # Considera que precisamos de pelo menos 1 concurso para ter frequência
    if concurso_maximo < 1 or concurso_anterior < 1:
        logger.warning(f"Não há dados suficientes para calcular tendência de rank com lookback {lookback} até {concurso_maximo}.")
        # Retorna uma série de zeros ou NaNs? Zeros indica sem mudança.
        return pd.Series(0, index=ALL_NUMBERS, name=f'rank_trend_{lookback}').astype(int)

    # Busca as frequências acumuladas nos dois pontos (usando snapshots otimizados)
    freq_atual = get_cumulative_frequency(concurso_maximo)
    freq_anterior = get_cumulative_frequency(concurso_anterior)

    # Verifica se conseguiu buscar as frequências
    if freq_atual is None:
        logger.error(f"Falha ao obter frequência acumulada para {concurso_maximo}.")
        return None
    if freq_anterior is None:
        logger.warning(f"Falha ao obter frequência acumulada para {concurso_anterior}. Não é possível calcular tendência de rank.")
        # Retorna zeros indicando sem dados para calcular a tendência
        return pd.Series(0, index=ALL_NUMBERS, name=f'rank_trend_{lookback}').astype(int)

    # Calcula os ranks (maior frequência = rank 1)
    # method='min' para lidar com empates (menor rank no empate)
    rank_atual = freq_atual.rank(method='min', ascending=False).astype(int)
    rank_anterior = freq_anterior.rank(method='min', ascending=False).astype(int)

    # Calcula a diferença (Rank Anterior - Rank Atual)
    # Se positivo, o rank melhorou (diminuiu). Se negativo, piorou (aumentou).
    rank_trend = (rank_anterior - rank_atual).fillna(0).astype(int) # Preenche NaN com 0
    rank_trend.name = f'rank_trend_{lookback}'

    logger.info(f"Cálculo de tendência de rank geral (lookback={lookback}) concluído.")
    return rank_trend