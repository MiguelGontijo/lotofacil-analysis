# src/strategies/frequency_strategies.py

import pandas as pd
from typing import Optional, Set

from src.config import logger
# Importa as funções de análise de frequência necessárias
from src.analysis.frequency_analysis import calculate_frequency, calculate_windowed_frequency

NUM_DEZENAS_LOTOFACIL = 15

def select_most_frequent_overall(concurso_maximo: int,
                                 num_to_select: int = NUM_DEZENAS_LOTOFACIL
                                 ) -> Optional[Set[int]]:
    """ Seleciona as N dezenas mais frequentes no histórico geral. """
    logger.debug(f"Estratégia 'Mais Frequentes': Calculando até {concurso_maximo}...")
    freq_series = calculate_frequency(concurso_maximo=concurso_maximo)
    if freq_series is None: return None
    most_frequent = freq_series.nlargest(num_to_select)
    selected_numbers = set(most_frequent.index)
    logger.debug(f"Estratégia 'Mais Frequentes': Selecionadas {len(selected_numbers)} dezenas: {sorted(list(selected_numbers))}")
    return selected_numbers


def select_least_frequent_overall(concurso_maximo: int,
                                  num_to_select: int = NUM_DEZENAS_LOTOFACIL
                                  ) -> Optional[Set[int]]:
    """ Seleciona as N dezenas menos frequentes no histórico geral. """
    logger.debug(f"Estratégia 'Menos Frequentes': Calculando até {concurso_maximo}...")
    freq_series = calculate_frequency(concurso_maximo=concurso_maximo)
    if freq_series is None: return None
    least_frequent = freq_series.nsmallest(num_to_select)
    selected_numbers = set(least_frequent.index)
    logger.debug(f"Estratégia 'Menos Frequentes': Selecionadas {len(selected_numbers)} dezenas: {sorted(list(selected_numbers))}")
    return selected_numbers


# --- NOVA FUNÇÃO ---
def select_most_frequent_recent(concurso_maximo: int,
                                window: int = 25, # Janela padrão de 25 concursos
                                num_to_select: int = NUM_DEZENAS_LOTOFACIL
                                ) -> Optional[Set[int]]:
    """
    Seleciona as N dezenas mais frequentes na última janela de 'window' concursos.
    """
    logger.debug(f"Estratégia 'Mais Frequentes Recentes ({window})': Calculando até {concurso_maximo}...")
    # Chama a análise de frequência por janela
    freq_series = calculate_windowed_frequency(window_size=window, concurso_maximo=concurso_maximo)

    if freq_series is None:
        logger.error(f"Falha ao calcular frequência na janela de {window} para estratégia.")
        return None

    # Seleciona as N mais frequentes na janela
    most_frequent = freq_series.nlargest(num_to_select)

    if len(most_frequent) < num_to_select:
        logger.warning(f"Menos de {num_to_select} dezenas com frequência > 0 na janela. Selecionando disponíveis.")

    selected_numbers = set(most_frequent.index)
    logger.debug(f"Estratégia 'Mais Frequentes Recentes ({window})': Selecionadas {len(selected_numbers)} dezenas: {sorted(list(selected_numbers))}")
    return selected_numbers