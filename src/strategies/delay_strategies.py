# src/strategies/delay_strategies.py

import pandas as pd
from typing import Optional, Set

from src.config import logger
# Importa a função de análise de atraso necessária
from src.analysis.delay_analysis import calculate_current_delay

NUM_DEZENAS_LOTOFACIL = 15

def select_most_delayed(concurso_maximo: int,
                        num_to_select: int = NUM_DEZENAS_LOTOFACIL
                        ) -> Optional[Set[int]]:
    """
    Seleciona as N dezenas com o maior atraso atual até o concurso_maximo.

    Args:
        concurso_maximo (int): O concurso de referência para calcular o atraso.
        num_to_select (int): Quantas dezenas selecionar.

    Returns:
        Optional[Set[int]]: Conjunto com as N dezenas selecionadas, ou None se falhar.
    """
    logger.debug(f"Estratégia 'Mais Atrasadas': Calculando atraso até {concurso_maximo}...")
    # Chama a função de análise de atraso atual
    delay_series = calculate_current_delay(concurso_maximo=concurso_maximo)

    if delay_series is None:
        logger.error("Falha ao calcular atraso para a estratégia.")
        return None

    # Trata NAs (dezenas que nunca saíram no período, improvável no geral mas possível)
    # Atribuímos um atraso 'baixo' (ex: -1) para NAs para que não sejam selecionadas por nlargest
    delay_series_filled = delay_series.fillna(-1)

    # Seleciona as N dezenas com maior atraso (maior valor)
    most_delayed = delay_series_filled.nlargest(num_to_select)

    if len(most_delayed) < num_to_select:
        logger.warning(f"Menos de {num_to_select} dezenas válidas encontradas. Selecionando disponíveis.")

    selected_numbers = set(most_delayed.index)

    # Verifica se algum número com atraso -1 (originalmente NA) foi selecionado indevidamente
    if -1 in most_delayed.values:
        logger.warning("Estratégia 'Mais Atrasadas' selecionou dezenas nunca vistas no período analisado (atraso NA).")
        # Poderia remover esses números se necessário, mas nlargest deve priorizar os com atraso real.

    logger.debug(f"Estratégia 'Mais Atrasadas': Selecionadas {len(selected_numbers)} dezenas: {sorted(list(selected_numbers))}")
    return selected_numbers