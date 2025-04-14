# src/strategies/scoring_strategies.py

from typing import Optional, Set, Dict, Any
import pandas as pd

from src.config import logger
# Importa o agregador e o scorer
from src.analysis_aggregator import get_consolidated_analysis
from src.scorer import calculate_scores

NUM_DEZENAS_LOTOFACIL = 15

def select_top_scored(concurso_maximo: int,
                      num_to_select: int = NUM_DEZENAS_LOTOFACIL,
                      scoring_config: Optional[Dict[str, Dict]] = None # Permite passar config customizada no futuro
                      ) -> Optional[Set[int]]:
    """
    Seleciona as N dezenas com maior pontuação calculada pelo scorer,
    baseado nas análises consolidadas até concurso_maximo.

    Args:
        concurso_maximo (int): O último concurso a considerar para as análises.
        num_to_select (int): Quantas dezenas selecionar.
        scoring_config (Optional[Dict[str, Dict]]): Configuração para o scorer (usa default se None).

    Returns:
        Optional[Set[int]]: Conjunto com as N dezenas selecionadas, ou None se falhar.
    """
    logger.debug(f"Estratégia 'Top Score': Agregando análises até {concurso_maximo}...")

    # 1. Obter resultados consolidados das análises
    analysis_results = get_consolidated_analysis(concurso_maximo)
    if analysis_results is None:
        logger.error("Falha ao obter análises consolidadas para estratégia de score.")
        return None

    # 2. Calcular os scores
    scores = calculate_scores(analysis_results, config=scoring_config)
    if scores is None:
        logger.error("Falha ao calcular scores para estratégia.")
        return None
    if scores.empty:
         logger.warning("Scores calculados resultaram em Series vazia.")
         return None

    # 3. Selecionar os top N scores
    top_scored = scores.nlargest(num_to_select)

    if len(top_scored) < num_to_select:
        logger.warning(f"Menos de {num_to_select} dezenas com score válido. Selecionando as disponíveis.")

    selected_numbers = set(top_scored.index)
    logger.debug(f"Estratégia 'Top Score': Selecionadas {len(selected_numbers)} dezenas: {sorted(list(selected_numbers))}")
    # logger.debug(f"Top scores:\n{top_scored.to_string()}") # Log opcional dos scores

    return selected_numbers