# src/analysis/statistical_tests_analysis.py
import pandas as pd
import numpy as np
from scipy import stats # Para o teste Qui-Quadrado
import logging
import json # <<< IMPORTAÇÃO ADICIONADA
from typing import Dict, Any, Optional 

logger = logging.getLogger(__name__)

def perform_chi_square_test_number_frequencies(
    observed_frequencies_df: pd.DataFrame, 
    total_draws: int, 
    config: Any,
    alpha: float = 0.05
) -> Optional[Dict[str, Any]]:
    """
    Realiza um teste Qui-Quadrado de aderência para verificar se as frequências
    observadas das dezenas seguem uma distribuição uniforme.

    Args:
        observed_frequencies_df (pd.DataFrame): DataFrame com as frequências absolutas
                                                observadas de cada dezena.
                                                Colunas esperadas: 'Dezena', 'Frequencia Absoluta'.
        total_draws (int): O número total de sorteios no histórico.
        config (Any): Objeto de configuração, que deve ter os atributos:
                      NUMBERS_PER_DRAW, ALL_NUMBERS.
        alpha (float): Nível de significância para a conclusão do teste.

    Returns:
        Optional[Dict[str, Any]]: Um dicionário com os resultados do teste, incluindo:
            'test_name', 'chi2_statistic', 'p_value', 'degrees_of_freedom',
            'alpha_level', 'conclusion', 'parameters' (JSON com N e E_i).
            Retorna None se a entrada for inválida.
    """
    test_name = "ChiSquare_NumberFrequencies_Uniformity"
    logger.info(f"Iniciando teste: {test_name}")

    if observed_frequencies_df.empty:
        logger.warning(f"{test_name}: DataFrame de frequências observadas está vazio.")
        return None
    if not all(col in observed_frequencies_df.columns for col in ['Dezena', 'Frequencia Absoluta']):
        logger.error(f"{test_name}: Colunas 'Dezena' ou 'Frequencia Absoluta' ausentes no DataFrame de frequências.")
        return None
    if total_draws <= 0:
        logger.error(f"{test_name}: Número total de sorteios ({total_draws}) deve ser positivo.")
        return None

    observed_frequencies_df = observed_frequencies_df.sort_values(by='Dezena').reset_index(drop=True)
    observed_counts = observed_frequencies_df['Frequencia Absoluta'].values
    
    if len(observed_counts) != len(config.ALL_NUMBERS):
        logger.error(f"{test_name}: Número de dezenas observadas ({len(observed_counts)}) difere do esperado ({len(config.ALL_NUMBERS)}).")
        return None

    total_drawn_ball_slots = total_draws * config.NUMBERS_PER_DRAW
    expected_frequency_per_number = total_drawn_ball_slots / len(config.ALL_NUMBERS)
    expected_counts = np.full_like(observed_counts, fill_value=expected_frequency_per_number, dtype=float)

    if np.any(expected_counts < 5):
        logger.warning(f"{test_name}: Algumas frequências esperadas são menores que 5. "
                       "O teste Qui-Quadrado pode não ser preciso. (Menor E_i: {expected_counts.min()})")

    try:
        chi2_statistic, p_value = stats.chisquare(f_obs=observed_counts, f_exp=expected_counts)
    except Exception as e:
        logger.error(f"{test_name}: Erro ao calcular o teste Qui-Quadrado: {e}", exc_info=True)
        return None

    degrees_of_freedom = len(config.ALL_NUMBERS) - 1 
    
    conclusion = ""
    if p_value < alpha:
        conclusion = (f"Rejeita H0 (p={p_value:.4f} < alpha={alpha}). "
                      "Evidência de que as dezenas NÃO são sorteadas uniformemente.")
    else:
        conclusion = (f"Não rejeita H0 (p={p_value:.4f} >= alpha={alpha}). "
                      "Sem evidência estatística contra a uniformidade das dezenas.")

    parameters_dict = {
        "total_observations": int(np.sum(observed_counts)),
        "expected_frequency_per_category": round(expected_frequency_per_number, 2),
        "number_of_categories": len(config.ALL_NUMBERS)
    }

    result = {
        "Test_Name": test_name,
        "Chi2_Statistic": round(chi2_statistic, 4),
        "P_Value": round(p_value, 6),
        "Degrees_Freedom": degrees_of_freedom,
        "Alpha_Level": alpha,
        "Conclusion": conclusion,
        "Parameters": json.dumps(parameters_dict), # json.dumps agora deve funcionar
        "Notes": "H0: As frequências observadas das dezenas são consistentes com uma distribuição uniforme."
    }
    logger.info(f"{test_name} concluído. P-valor: {p_value:.6f}. Conclusão (alpha={alpha}): {conclusion}")
    return result