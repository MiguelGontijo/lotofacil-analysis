# src/strategies/scoring_strategies.py

from typing import Optional, Set, Dict, Any
import pandas as pd

from src.config import logger, ALL_NUMBERS
# Importa APENAS o scorer para calcular o score a partir dos dados recebidos
from src.scorer import calculate_scores
# NÃO importa mais o agregador

NUM_DEZENAS_LOTOFACIL = 15
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))

# --- Assinatura da função alterada ---
def select_top_scored(current_analysis: Dict[str, Any],
                      num_to_select: int = NUM_DEZENAS_LOTOFACIL,
                      scoring_config: Optional[Dict[str, Dict]] = None
                      ) -> Optional[Set[int]]:
    """
    Seleciona N dezenas com maior pontuação, usando dados JÁ CALCULADOS.
    """
    logger.debug(f"Estratégia 'Top Score': Usando dados pré-calculados...")

    # *** NÃO CHAMA MAIS o agregador get_consolidated_analysis ***
    # Os dados necessários já devem estar em current_analysis

    # 1. Calcula os scores a partir dos dados recebidos
    scores = calculate_scores(current_analysis, config=scoring_config) # Passa o dict recebido
    if scores is None:
        logger.error("Falha ao calcular scores para estratégia 'Top Score'.")
        return None
    if scores.empty:
         logger.warning("Scores calculados resultaram em Series vazia.")
         return None

    # 2. Seleciona os top N scores
    top_scored = scores.nlargest(num_to_select)
    selected_numbers = set(top_scored.index)

    logger.debug(f"Estratégia 'Top Score': Selecionadas {len(selected_numbers)}")
    return selected_numbers