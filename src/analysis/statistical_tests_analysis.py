# src/analysis/statistical_tests_analysis.py
import pandas as pd
import numpy as np
from scipy import stats 
import logging
import json 
from typing import Dict, Any, Optional, List 

logger = logging.getLogger(__name__)

def perform_chi_square_test_number_frequencies(
    observed_frequencies_df: pd.DataFrame, 
    total_draws: int, 
    config: Any,
    alpha: float = 0.05
) -> Optional[Dict[str, Any]]:
    # ... (código existente desta função, sem alterações) ...
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
        "Parameters": json.dumps(parameters_dict),
        "Notes": "H0: As frequências observadas das dezenas são consistentes com uma distribuição uniforme."
    }
    logger.info(f"{test_name} concluído. P-valor: {p_value:.6f}. Conclusão (alpha={alpha}): {conclusion}")
    return result

def perform_normality_test_for_sum_of_numbers(
    sum_of_numbers_series: pd.Series,
    config: Any,
    method: str = 'chi_square_bins',
    alpha: float = 0.05
) -> Optional[Dict[str, Any]]:
    # ... (código existente desta função, sem alterações) ...
    if sum_of_numbers_series.empty or sum_of_numbers_series.nunique() < 2 :
        logger.warning(f"Série de soma das dezenas está vazia ou não tem variação suficiente para o teste de normalidade.")
        return None

    test_name_base = "NormalityTest_SumOfNumbers"
    test_result_dict: Dict[str, Any] = {}
    sample_size = len(sum_of_numbers_series)
    sample_mean = sum_of_numbers_series.mean()
    sample_std_dev = sum_of_numbers_series.std(ddof=0) 

    parameters_dict: Dict[str, Any] = {
        "sample_size": sample_size,
        "sample_mean": round(sample_mean, 2),
        "sample_std_dev": round(sample_std_dev, 2)
    }

    statistic = np.nan
    p_value = np.nan
    degrees_of_freedom = None
    notes = "H0: A distribuição da soma das dezenas é Normal."

    if method == 'chi_square_bins':
        test_name = f"{test_name_base}_ChiSquareBins"
        logger.info(f"Iniciando teste: {test_name}")
        
        num_bins = getattr(config, 'SUM_NORMALITY_TEST_BINS', 10)
        parameters_dict["num_bins"] = num_bins

        observed_freq, bin_edges = np.histogram(sum_of_numbers_series, bins=num_bins)
        
        if sample_std_dev == 0:
            logger.warning(f"{test_name}: Desvio padrão da soma das dezenas é zero. Teste não aplicável.")
            return None
            
        cdf_values = stats.norm.cdf(bin_edges, loc=sample_mean, scale=sample_std_dev)
        expected_prob = np.diff(cdf_values)
        expected_freq = expected_prob * sample_size
        
        if np.sum(expected_freq) > 0 : 
            expected_freq = (expected_freq / np.sum(expected_freq)) * np.sum(observed_freq)
        
        if np.any(expected_freq < 1):
            logger.warning(f"{test_name}: Algumas frequências esperadas são menores que 1 (idealmente >= 5). "
                           "O teste Qui-Quadrado pode não ser preciso. Considere agrupar bins ou usar outro teste. "
                           f"Menor E_i: {expected_freq.min():.2f}. Soma Observada: {np.sum(observed_freq)}, Soma Esperada Ajustada: {np.sum(expected_freq):.2f}")
            
        valid_indices = (observed_freq > 0) | (expected_freq > 0.00001) 
        observed_freq_filtered = observed_freq[valid_indices]
        expected_freq_filtered = expected_freq[valid_indices]

        if len(observed_freq_filtered) < 2:
            logger.warning(f"{test_name}: Menos de 2 bins com frequências não nulas após filtragem. Teste não pode ser realizado.")
            return None

        try:
            statistic, p_value = stats.chisquare(f_obs=observed_freq_filtered, f_exp=expected_freq_filtered)
            degrees_of_freedom = len(observed_freq_filtered) - 1 - 2 
            if degrees_of_freedom <= 0:
                logger.warning(f"{test_name}: Graus de liberdade não positivos ({degrees_of_freedom}). Teste inválido.")
                degrees_of_freedom = None 
                p_value = np.nan
        except ValueError as ve:
            logger.error(f"{test_name}: Erro de valor no cálculo do Qui-Quadrado: {ve}")
            logger.debug(f"Observed Freq (filtered): {observed_freq_filtered}, Sum: {np.sum(observed_freq_filtered)}")
            logger.debug(f"Expected Freq (filtered): {expected_freq_filtered}, Sum: {np.sum(expected_freq_filtered)}")
            return None
        notes += f" Teste Qui-Quadrado com {num_bins} bins. Estimados mu e sigma da amostra."

    elif method == 'kolmogorov_smirnov':
        test_name = f"{test_name_base}_KolmogorovSmirnov"
        logger.info(f"Iniciando teste: {test_name}")
        if sample_std_dev == 0:
            logger.warning(f"{test_name}: Desvio padrão da soma das dezenas é zero. Teste não aplicável.")
            return None
        try:
            statistic, p_value = stats.kstest(sum_of_numbers_series, 'norm', args=(sample_mean, sample_std_dev))
        except Exception as e:
            logger.error(f"{test_name}: Erro ao calcular o teste Kolmogorov-Smirnov: {e}", exc_info=True)
            return None
        notes += " Teste Kolmogorov-Smirnov contra Normal com mu e sigma estimados da amostra."
    
    else:
        logger.error(f"Método de teste de normalidade desconhecido: {method}")
        return None

    conclusion = ""
    if pd.notna(p_value):
        if p_value < alpha:
            conclusion = (f"Rejeita H0 (p={p_value:.4f} < alpha={alpha}). "
                          "Evidência de que a distribuição da soma das dezenas NÃO é Normal.")
        else:
            conclusion = (f"Não rejeita H0 (p={p_value:.4f} >= alpha={alpha}). "
                          "Sem evidência estatística contra a normalidade da soma das dezenas.")
    else:
        conclusion = "Inconclusivo devido a erro ou dados insuficientes para o teste."

    test_result_dict = {
        "Test_Name": test_name,
        "Chi2_Statistic": round(statistic, 4) if pd.notna(statistic) else None,
        "P_Value": round(p_value, 6) if pd.notna(p_value) else None,
        "Degrees_Freedom": degrees_of_freedom,
        "Alpha_Level": alpha,
        "Conclusion": conclusion,
        "Parameters": json.dumps(parameters_dict),
        "Notes": notes
    }
    logger.info(f"{test_name} concluído. P-valor: {test_result_dict['P_Value']}. Conclusão (alpha={alpha}): {conclusion}")
    return test_result_dict

# --- NOVA FUNÇÃO PARA TESTE DE ADERÊNCIA À DISTRIBUIÇÃO DE POISSON ---
def perform_poisson_distribution_test(
    observed_event_counts_series: pd.Series, # Série com as contagens do evento por unidade (ex: N.primos por sorteio)
    event_config: Dict[str, Any], # Configuração específica para este evento (ex: de POISSON_DISTRIBUTION_TEST_CONFIG)
    alpha: float = 0.05
) -> Optional[Dict[str, Any]]:
    """
    Realiza um teste Qui-Quadrado de aderência para verificar se a contagem de um evento
    (ex: número de dezenas primas por sorteio) segue uma distribuição de Poisson.

    Args:
        observed_event_counts_series (pd.Series): Série Pandas contendo as contagens
                                                  observadas do evento por unidade (ex: por sorteio).
        event_config (Dict[str, Any]): Dicionário de configuração para este teste específico,
                                       espera-se 'column_name' e 'max_observed_count_for_chi2'.
        alpha (float): Nível de significância para a conclusão do teste.

    Returns:
        Optional[Dict[str, Any]]: Dicionário com os resultados do teste ou None em caso de erro.
    """
    event_name = event_config.get("event_description", event_config.get("column_name", "EventoDesconhecido"))
    test_name = f"PoissonDistributionTest_{event_name}"
    logger.info(f"Iniciando teste: {test_name}")

    if observed_event_counts_series.empty:
        logger.warning(f"{test_name}: Série de contagens observadas do evento está vazia.")
        return None

    # 1. Calcular a taxa média observada (lambda)
    lambda_observed = observed_event_counts_series.mean()
    if pd.isna(lambda_observed) or lambda_observed < 0: # Lambda não pode ser negativo
        logger.error(f"{test_name}: Lambda observado inválido ({lambda_observed}).")
        return None

    # 2. Gerar frequências observadas de k eventos (0, 1, 2, ..., max_val_plus)
    # Agrupa contagens raras na cauda para o teste Qui-Quadrado
    max_k_for_chi2 = event_config.get("max_observed_count_for_chi2", int(observed_event_counts_series.max()))
    
    # Cria os bins/categorias para o Qui-Quadrado.
    # Categorias: 0, 1, 2, ..., max_k_for_chi2, >max_k_for_chi2 (se necessário)
    observed_counts_binned = []
    categories = []
    
    # Frequências para 0, 1, ..., max_k_for_chi2 - 1
    for k_val in range(max_k_for_chi2):
        observed_counts_binned.append((observed_event_counts_series == k_val).sum())
        categories.append(str(k_val))
    
    # Frequência para max_k_for_chi2 e acima (categoria ">= max_k_for_chi2")
    observed_counts_binned.append((observed_event_counts_series >= max_k_for_chi2).sum())
    categories.append(f">={max_k_for_chi2}")
    
    observed_freq = np.array(observed_counts_binned)
    num_categories = len(observed_freq)

    # 3. Calcular frequências esperadas segundo uma Poisson com lambda_observed
    expected_freq = np.zeros(num_categories, dtype=float)
    total_observations = len(observed_event_counts_series)

    for i in range(num_categories):
        if i < num_categories - 1: # Categorias 0, 1, ..., max_k_for_chi2 - 1
            k_val_cat = int(categories[i])
            expected_prob_k = stats.poisson.pmf(k_val_cat, lambda_observed)
            expected_freq[i] = expected_prob_k * total_observations
        else: # Última categoria: P(X >= max_k_for_chi2) = 1 - CDF(max_k_for_chi2 - 1)
            # Ou, mais simples, o restante para a soma das probabilidades ser 1.
            # Mas para Poisson, é 1 - sum(P(X=k) for k < max_k_for_chi2)
            # Usamos a probabilidade de sobrevivência (1 - CDF)
            prob_ge_max_k = 1.0 - stats.poisson.cdf(max_k_for_chi2 - 1, lambda_observed)
            expected_freq[i] = prob_ge_max_k * total_observations
            
    # Pequeno ajuste para garantir que a soma das frequências esperadas seja igual ao total de observações
    # Isso ajuda a evitar erros de precisão no teste Qui-Quadrado.
    if np.sum(expected_freq) > 0 and total_observations > 0:
        expected_freq = (expected_freq / np.sum(expected_freq)) * total_observations


    # Agrupar categorias se as frequências esperadas forem muito baixas (critério comum: < 5)
    # Esta é uma parte complexa. Para simplificar, apenas emitiremos um aviso por agora.
    # A implementação robusta de agrupamento de bins é não trivial.
    if np.any(expected_freq < 1): # Critério mais frouxo para aviso
        logger.warning(f"{test_name}: Algumas frequências esperadas são < 1 (idealmente >= 5). "
                       f"O teste Qui-Quadrado pode não ser preciso. Menor E_i: {expected_freq.min():.2f}. "
                       f"Observadas: {observed_freq.tolist()}, Esperadas: {[round(ef,2) for ef in expected_freq.tolist()]}")
        # Se houver zeros em expected_freq e não em observed_freq, stats.chisquare dará erro.
        # Vamos filtrar onde expected_freq é muito próximo de zero se observed_freq também é zero,
        # ou onde expected_freq é zero e observed não é (indicando um problema).
        
    # Filtra categorias onde a frequência esperada é zero (ou muito pequena)
    # e a observada também é zero, para evitar problemas com o teste.
    # Se a esperada é zero e a observada não é, o teste Qui-Quadrado falhará.
    valid_indices_chi2 = (expected_freq > 0.00001) # Filtra esperadas muito pequenas
    observed_freq_chi2 = observed_freq[valid_indices_chi2]
    expected_freq_chi2 = expected_freq[valid_indices_chi2]

    if len(observed_freq_chi2) < 2:
        logger.warning(f"{test_name}: Menos de 2 categorias válidas para o teste Qui-Quadrado após filtragem. Teste não pode ser realizado.")
        return None

    try:
        chi2_statistic, p_value = stats.chisquare(f_obs=observed_freq_chi2, f_exp=expected_freq_chi2)
        degrees_of_freedom = len(observed_freq_chi2) - 1 - 1 # k_categorias - 1 - num_param_estimados (lambda)
        if degrees_of_freedom <= 0:
            logger.warning(f"{test_name}: Graus de liberdade não positivos ({degrees_of_freedom}). Teste inválido.")
            degrees_of_freedom = None
            p_value = np.nan 
    except ValueError as ve:
        logger.error(f"{test_name}: Erro de valor no cálculo do Qui-Quadrado para Poisson: {ve}")
        logger.debug(f"Observed Freq (filtered for chi2): {observed_freq_chi2}, Sum: {np.sum(observed_freq_chi2)}")
        logger.debug(f"Expected Freq (filtered for chi2): {expected_freq_chi2}, Sum: {np.sum(expected_freq_chi2)}")
        return None
    except Exception as e:
        logger.error(f"{test_name}: Erro ao calcular o teste Qui-Quadrado para Poisson: {e}", exc_info=True)
        return None
        
    conclusion = ""
    if pd.notna(p_value):
        if p_value < alpha:
            conclusion = (f"Rejeita H0 (p={p_value:.4f} < alpha={alpha}). Evidência de que a "
                          f"distribuição das contagens de '{event_name}' NÃO segue uma Poisson(lambda~{lambda_observed:.2f}).")
        else:
            conclusion = (f"Não rejeita H0 (p={p_value:.4f} >= alpha={alpha}). Sem evidência contra "
                          f"a distribuição de Poisson(lambda~{lambda_observed:.2f}) para '{event_name}'.")
    else:
        conclusion = "Inconclusivo devido a erro ou dados insuficientes para o teste."

    parameters_dict = {
        "event_name": event_name,
        "estimated_lambda": round(lambda_observed, 4),
        "total_units_observed": total_observations, # Ex: número de sorteios
        "max_k_for_chi2_categories": max_k_for_chi2,
        "categories_used_in_chi2": categories # Pode ser útil saber os bins
    }

    result = {
        "Test_Name": test_name,
        "Chi2_Statistic": round(chi2_statistic, 4) if pd.notna(chi2_statistic) else None,
        "P_Value": round(p_value, 6) if pd.notna(p_value) else None,
        "Degrees_Freedom": degrees_of_freedom,
        "Alpha_Level": alpha,
        "Conclusion": conclusion,
        "Parameters": json.dumps(parameters_dict),
        "Notes": f"H0: A contagem de '{event_name}' por unidade segue uma distribuição de Poisson."
    }
    logger.info(f"{test_name} concluído. Lambda estimado: {lambda_observed:.2f}. P-valor: {result['P_Value']}. Conclusão: {conclusion}")
    return result