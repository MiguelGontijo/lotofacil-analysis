# src/analysis/group_trend_analysis.py

import pandas as pd
import numpy as np
from typing import Dict, List, Optional

# Importa funções e constantes necessárias
from src.config import logger, ALL_NUMBERS
from src.analysis.frequency_analysis import calculate_windowed_frequency

# Define os grupos
NUMBER_GROUPS: Dict[str, List[int]] = {
    'G1 (1-5)':   list(range(1, 6)),
    'G2 (6-10)':  list(range(6, 11)),
    'G3 (11-15)': list(range(11, 16)),
    'G4 (16-20)': list(range(16, 21)),
    'G5 (21-25)': list(range(21, 26)),
}
# Fallback (caso config falhe)
try: from src.config import DEFAULT_GROUP_WINDOWS
except ImportError: DEFAULT_GROUP_WINDOWS = [25, 100]
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))


def calculate_group_freq_stats(concurso_maximo: Optional[int] = None,
                               windows: List[int] = DEFAULT_GROUP_WINDOWS
                               ) -> Optional[pd.DataFrame]: # <<< Retorna DataFrame
    """
    Calcula a frequência MÉDIA das dezenas dentro de cada grupo definido
    para diferentes janelas recentes.

    Returns:
        Optional[pd.DataFrame]: DataFrame indexado pelo NOME do grupo, com colunas
                                 como 'W25_avg_freq', 'W100_avg_freq'.
    """
    if not windows: return pd.DataFrame(index=list(NUMBER_GROUPS.keys()))
    logger.info(f"Calculando stats de freq. média por grupo (W: {windows}) até {concurso_maximo or 'último'}...")

    # Cache para não recalcular janelas inteiras
    window_freq_cache: Dict[int, Optional[pd.Series]] = {}
    for w in windows: window_freq_cache[w] = calculate_windowed_frequency(w, concurso_maximo)

    group_avg_data = {} # Dict para construir o DataFrame final

    # Calcula a média da frequência para os números de cada grupo
    for group_name, numbers_in_group in NUMBER_GROUPS.items():
        group_avg_data[group_name] = {} # Inicia dict para este grupo
        for window_size in windows:
            col_name = f'W{window_size}_avg_freq'
            freq_series = window_freq_cache.get(window_size)

            if freq_series is None:
                group_avg_data[group_name][col_name] = np.nan # Usa NaN se janela falhou
                logger.warning(f"Freq W{window_size} indisponível p/ grupo {group_name}.")
            else:
                group_frequencies = freq_series.reindex(numbers_in_group)
                avg_freq = group_frequencies.mean(skipna=True)
                # Usa NaN se média falhar (ex: grupo vazio - não deve acontecer)
                group_avg_data[group_name][col_name] = avg_freq if pd.notna(avg_freq) else np.nan
                logger.debug(f"Grupo {group_name} - W{window_size}: Média Freq = {avg_freq:.4f}")

    logger.info("Cálculo de stats de freq. média por grupo concluído.")
    results_df = pd.DataFrame.from_dict(group_avg_data, orient='index')
    results_df.index.name = 'grupo'
    # Preenche NaNs restantes com 0 (ex: se janela inteira falhou)
    results_df.fillna(0, inplace=True)
    return results_df