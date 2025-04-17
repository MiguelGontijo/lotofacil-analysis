# src/analysis/group_trend_analysis.py

import pandas as pd
import numpy as np # <<< IMPORT ADICIONADO >>>
from typing import Dict, List, Optional

# Importa funções e constantes necessárias
from src.config import logger, ALL_NUMBERS, DEFAULT_GROUP_WINDOWS # Usa default do config
from src.analysis.frequency_analysis import calculate_windowed_frequency

# Define os grupos
NUMBER_GROUPS: Dict[str, List[int]] = {
    '1-5':   list(range(1, 6)),
    '6-10':  list(range(6, 11)),
    '11-15': list(range(11, 16)),
    '16-20': list(range(16, 21)),
    '21-25': list(range(21, 26)),
}
# Fallback se não vier do config
if 'DEFAULT_GROUP_WINDOWS' not in globals(): DEFAULT_GROUP_WINDOWS = [25, 100]
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))


def calculate_group_freq_stats(concurso_maximo: Optional[int] = None,
                               windows: List[int] = DEFAULT_GROUP_WINDOWS
                               ) -> Optional[pd.DataFrame]:
    """
    Calcula a frequência MÉDIA das dezenas dentro de cada grupo definido
    para diferentes janelas recentes. Retorna DataFrame indexado por dezena.
    """
    if not windows: return pd.DataFrame(index=pd.Index(ALL_NUMBERS, name='dezena'))
    logger.info(f"Calculando stats de freq. média por grupo (W: {windows}) até {concurso_maximo or 'último'}...")

    group_stats_data = {}
    window_freq_cache: Dict[int, Optional[pd.Series]] = {}

    for window_size in windows:
        col_name = f'group_W{window_size}_avg_freq'
        if window_size not in window_freq_cache:
             logger.debug(f"Calculando janela {window_size} p/ análise de grupo...")
             window_freq_cache[window_size] = calculate_windowed_frequency(window_size, concurso_maximo)

        freq_series = window_freq_cache[window_size]
        group_avg_series = pd.Series(np.nan, index=ALL_NUMBERS) # Usa np.nan

        if freq_series is None:
            logger.warning(f"Não calculou freq W{window_size}. Stats de grupo NaN.")
            # Mantém a Series como NaN
        else:
            for group_name, numbers_in_group in NUMBER_GROUPS.items():
                group_frequencies = freq_series.reindex(numbers_in_group)
                # Usa np.mean se precisar, mas .mean() do Pandas já ignora NaN por padrão
                avg_freq = group_frequencies.mean(skipna=True)
                # Usa np.nan se a média falhar (embora .mean() já retorne NaN)
                avg_freq_final = avg_freq if pd.notna(avg_freq) else np.nan
                group_avg_series.loc[numbers_in_group] = avg_freq_final
                # logger.debug(f"Grupo {group_name} - W{window_size}: Média Freq = {avg_freq_final:.4f}")

        group_stats_data[col_name] = group_avg_series

    logger.info("Cálculo de stats de freq. média por grupo concluído.")
    if not group_stats_data: return pd.DataFrame(index=pd.Index(ALL_NUMBERS, name='dezena'))
    results_df = pd.DataFrame(group_stats_data)
    results_df.index.name = 'dezena'
    # Preenche NaNs restantes com 0 (se uma janela inteira falhou)
    results_df.fillna(0, inplace=True)
    return results_df