# src/strategies/frequency_strategies.py

import pandas as pd
from typing import Optional, Set, Dict, Any

from src.config import logger, ALL_NUMBERS # Usa ALL_NUMBERS
# Não importa mais funções de analysis daqui, recebe via dicionário

NUM_DEZENAS_LOTOFACIL = 15
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))

# --- Assinaturas das funções alteradas ---

def select_most_frequent_overall(current_analysis: Dict[str, Any],
                                 num_to_select: int = NUM_DEZENAS_LOTOFACIL
                                 ) -> Optional[Set[int]]:
    """ Seleciona N dezenas MAIS frequentes GERAL (usa dados do dict). """
    logger.debug(f"Estratégia 'Mais Frequentes': Usando dados pré-calculados...")
    freq_series = current_analysis.get('overall_freq')

    if freq_series is None or not isinstance(freq_series, pd.Series):
        logger.error("Série 'overall_freq' não encontrada ou inválida no dicionário de análise.")
        return None

    most_frequent = freq_series.nlargest(num_to_select)
    selected_numbers = set(most_frequent.index)
    logger.debug(f"Estratégia 'Mais Frequentes': Selecionadas {len(selected_numbers)}")
    return selected_numbers


def select_least_frequent_overall(current_analysis: Dict[str, Any],
                                  num_to_select: int = NUM_DEZENAS_LOTOFACIL
                                  ) -> Optional[Set[int]]:
    """ Seleciona N dezenas MENOS frequentes GERAL (usa dados do dict). """
    logger.debug(f"Estratégia 'Menos Frequentes': Usando dados pré-calculados...")
    freq_series = current_analysis.get('overall_freq')

    if freq_series is None or not isinstance(freq_series, pd.Series):
        logger.error("Série 'overall_freq' não encontrada ou inválida.")
        return None

    least_frequent = freq_series.nsmallest(num_to_select)
    selected_numbers = set(least_frequent.index)
    logger.debug(f"Estratégia 'Menos Frequentes': Selecionadas {len(selected_numbers)}")
    return selected_numbers


def select_most_frequent_recent(current_analysis: Dict[str, Any],
                                window: int = 25, # Janela precisa ser passada ou configurada
                                num_to_select: int = NUM_DEZENAS_LOTOFACIL
                                ) -> Optional[Set[int]]:
    """ Seleciona N dezenas MAIS frequentes RECENTES (usa dados do dict). """
    logger.debug(f"Estratégia 'Mais Frequentes Recentes ({window})': Usando dados pré-calculados...")
    freq_key = f'recent_freq_{window}'
    freq_series = current_analysis.get(freq_key)

    if freq_series is None or not isinstance(freq_series, pd.Series):
        logger.error(f"Série '{freq_key}' não encontrada ou inválida.")
        # Poderia tentar calcular na hora como fallback? Não, o objetivo é usar o estado.
        return None

    most_frequent = freq_series.nlargest(num_to_select)
    selected_numbers = set(most_frequent.index)
    logger.debug(f"Estratégia 'Mais Frequentes Recentes ({window})': Selecionadas {len(selected_numbers)}")
    return selected_numbers