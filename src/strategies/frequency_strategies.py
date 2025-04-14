# src/strategies/frequency_strategies.py

import pandas as pd
from typing import Optional, Set

from src.config import logger
# Importa diretamente a função de análise necessária
from src.analysis.frequency_analysis import calculate_frequency

NUM_DEZENAS_LOTOFACIL = 15

def select_most_frequent_overall(concurso_maximo: int,
                                 num_to_select: int = NUM_DEZENAS_LOTOFACIL
                                 ) -> Optional[Set[int]]:
    """
    Seleciona as N dezenas mais frequentes no histórico geral até o concurso_maximo.

    Args:
        concurso_maximo (int): O último concurso a considerar para a análise de frequência.
        num_to_select (int): Quantas dezenas selecionar (normalmente 15).

    Returns:
        Optional[Set[int]]: Conjunto com as N dezenas selecionadas, ou None se falhar.
    """
    logger.debug(f"Estratégia 'Mais Frequentes': Calculando frequência até concurso {concurso_maximo}...")
    # Chama a função de análise para obter a frequência geral
    freq_series = calculate_frequency(concurso_maximo=concurso_maximo)

    if freq_series is None:
        logger.error("Falha ao calcular frequência para a estratégia.")
        return None

    # Seleciona as N dezenas com maior frequência (maior valor)
    # nlargest retorna a Series ordenada, pegamos o índice (as dezenas)
    most_frequent = freq_series.nlargest(num_to_select)

    if len(most_frequent) < num_to_select:
        logger.warning(f"Menos de {num_to_select} dezenas encontradas com frequência > 0. Selecionando as disponíveis.")
        # Pode acontecer em períodos muito iniciais ou se houver erro nos dados

    selected_numbers = set(most_frequent.index)
    logger.debug(f"Estratégia 'Mais Frequentes': Selecionadas {len(selected_numbers)} dezenas: {sorted(list(selected_numbers))}")
    return selected_numbers

# --- Outras estratégias baseadas em frequência poderiam ser adicionadas aqui ---
# Exemplo: Menos Frequentes
def select_least_frequent_overall(concurso_maximo: int,
                                  num_to_select: int = NUM_DEZENAS_LOTOFACIL
                                  ) -> Optional[Set[int]]:
    logger.debug(f"Estratégia 'Menos Frequentes': Calculando frequência até {concurso_maximo}...")
    freq_series = calculate_frequency(concurso_maximo=concurso_maximo)
    if freq_series is None: return None
    # Seleciona as N dezenas com MENOR frequência (menor valor)
    least_frequent = freq_series.nsmallest(num_to_select)
    selected_numbers = set(least_frequent.index)
    logger.debug(f"Estratégia 'Menos Frequentes': Selecionadas {len(selected_numbers)} dezenas: {sorted(list(selected_numbers))}")
    return selected_numbers

# Exemplo: Mais Frequentes Recentes (usando a função de janela)
# from src.analysis.frequency_analysis import calculate_windowed_frequency
# def select_most_frequent_recent(concurso_maximo: int, window: int = 25, num_to_select: int = 15) -> Optional[Set[int]]:
#     logger.debug(f"Estratégia 'Mais Frequentes Recentes ({window})': Calculando até {concurso_maximo}...")
#     freq_series = calculate_windowed_frequency(window_size=window, concurso_maximo=concurso_maximo)
#     if freq_series is None: return None
#     most_frequent = freq_series.nlargest(num_to_select)
#     selected_numbers = set(most_frequent.index)
#     logger.debug(f"Estratégia 'Mais Frequentes Recentes ({window})': Selecionadas {len(selected_numbers)} dezenas: {sorted(list(selected_numbers))}")
#     return selected_numbers