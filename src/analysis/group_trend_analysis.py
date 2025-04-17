# src/analysis/group_trend_analysis.py

import pandas as pd
from typing import Dict, List, Optional

# Importa funções e constantes necessárias
from src.config import logger, ALL_NUMBERS
from src.analysis.frequency_analysis import calculate_windowed_frequency

# Define os grupos
NUMBER_GROUPS: Dict[str, List[int]] = {
    '1-5':   list(range(1, 6)),
    '6-10':  list(range(6, 11)),
    '11-15': list(range(11, 16)),
    '16-20': list(range(16, 21)),
    '21-25': list(range(21, 26)),
}

# Janelas padrão para analisar a tendência de grupo
DEFAULT_GROUP_WINDOWS = [25, 100] # Usaremos W25 e W100 para a média

def calculate_group_freq_stats(concurso_maximo: Optional[int] = None,
                               windows: List[int] = DEFAULT_GROUP_WINDOWS
                               ) -> Optional[Dict[str, Dict[str, float]]]:
    """
    Calcula a frequência MÉDIA das dezenas dentro de cada grupo definido
    para diferentes janelas de tempo recentes.

    Args:
        concurso_maximo (Optional[int]): Último concurso a considerar.
        windows (List[int]): Lista de tamanhos de janela para calcular a frequência média.

    Returns:
        Optional[Dict[str, Dict[str, float]]]: Dicionário onde as chaves são os nomes
                                               dos grupos (ex: '1-5') e os valores são
                                               outros dicionários com {'W{size}_avg_freq': media},
                                               ou None se erro.
                                               Retorna dict vazio se não houver dados.
    """
    if not windows:
        logger.warning("Nenhuma janela especificada para análise de grupo.")
        return {}

    logger.info(f"Calculando estatísticas de frequência média por grupo (Janelas: {windows}) até {concurso_maximo or 'último'}...")

    group_stats: Dict[str, Dict[str, float]] = {name: {} for name in NUMBER_GROUPS}
    window_freq_cache: Dict[int, Optional[pd.Series]] = {} # Cache para não recalcular janelas

    for window_size in windows:
        # Busca ou calcula a frequência da janela
        if window_size not in window_freq_cache:
             logger.debug(f"Calculando janela {window_size} para análise de grupo...")
             window_freq_cache[window_size] = calculate_windowed_frequency(window_size, concurso_maximo)

        freq_series = window_freq_cache[window_size]

        if freq_series is None:
            logger.warning(f"Não foi possível calcular frequência para janela {window_size}. Stats de grupo para esta janela serão NaN.")
            # Preenche com NaN para esta janela em todos os grupos
            for group_name in NUMBER_GROUPS:
                group_stats[group_name][f'W{window_size}_avg_freq'] = float('nan')
            continue # Pula para a próxima janela

        # Calcula a média da frequência para os números de cada grupo
        for group_name, numbers_in_group in NUMBER_GROUPS.items():
            # Seleciona as frequências apenas dos números neste grupo
            group_frequencies = freq_series.reindex(numbers_in_group) # Garante que temos todos os números do grupo
            # Calcula a média (ignora NaNs se houver, embora reindex deva evitar)
            avg_freq = group_frequencies.mean(skipna=True)
            group_stats[group_name][f'W{window_size}_avg_freq'] = avg_freq if pd.notna(avg_freq) else float('nan')
            logger.debug(f"Grupo {group_name} - W{window_size}: Média Freq = {avg_freq:.4f}")

    logger.info("Cálculo de estatísticas de frequência média por grupo concluído.")
    # Retorna dicionário aninhado. Ex: {'1-5': {'W25_avg_freq': 6.2, 'W100_avg_freq': 24.5}, ...}
    return group_stats