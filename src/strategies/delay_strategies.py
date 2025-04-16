# src/strategies/delay_strategies.py

import pandas as pd
from typing import Optional, Set, Dict, Any

from src.config import logger, ALL_NUMBERS
# Não importa mais funções de analysis

NUM_DEZENAS_LOTOFACIL = 15
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))

# --- Assinatura da função alterada ---
def select_most_delayed(current_analysis: Dict[str, Any],
                        num_to_select: int = NUM_DEZENAS_LOTOFACIL
                        ) -> Optional[Set[int]]:
    """ Seleciona N dezenas MAIS ATRASADAS (usa dados do dict). """
    logger.debug(f"Estratégia 'Mais Atrasadas': Usando dados pré-calculados...")
    delay_series = current_analysis.get('current_delay')

    if delay_series is None or not isinstance(delay_series, pd.Series):
        logger.error("Série 'current_delay' não encontrada ou inválida.")
        return None

    # Trata NAs (que podem vir como pd.NA ou float('nan'))
    # Atribuímos -1 para NAs para que fiquem no fim de nlargest
    delay_series_filled = delay_series.fillna(-1)

    most_delayed = delay_series_filled.nlargest(num_to_select)
    selected_numbers = set(most_delayed.index)

    # Opcional: Verificar se selecionou algum com -1?
    if -1 in most_delayed.values:
         logger.warning("Estratégia 'Mais Atrasadas' considerou dezenas com atraso NA.")

    logger.debug(f"Estratégia 'Mais Atrasadas': Selecionadas {len(selected_numbers)}")
    return selected_numbers