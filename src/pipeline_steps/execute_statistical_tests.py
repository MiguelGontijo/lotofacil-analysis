# src/pipeline_steps/execute_statistical_tests.py
import logging
import pandas as pd
from typing import Any, Dict, List 

# Importa as funções de análise
from src.analysis.statistical_tests_analysis import (
    perform_chi_square_test_number_frequencies,
    perform_normality_test_for_sum_of_numbers 
)

logger = logging.getLogger(__name__)

def run_statistical_tests_step(
    all_data_df: pd.DataFrame,
    db_manager: Any, 
    config: Any,      
    shared_context: Dict[str, Any], 
    **kwargs
) -> bool:
    """
    Executa a etapa de Testes Estatísticos, incluindo:
    1. Teste Qui-Quadrado para uniformidade da frequência das dezenas.
    2. Teste de Normalidade para a soma das dezenas sorteadas (ambos os métodos).
    Os argumentos são injetados pelo Orchestrator.
    """
    step_name = "Testes Estatísticos"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")
    
    if not hasattr(config, 'STATISTICAL_TESTS_RESULTS_TABLE_NAME') or \
       not hasattr(config, 'NUMBERS_PER_DRAW') or \
       not hasattr(config, 'ALL_NUMBERS') or \
       not hasattr(config, 'SUM_NORMALITY_TEST_BINS'):
        logger.error("Atributos de configuração necessários para Testes Estatísticos não encontrados.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    if not isinstance(all_data_df, pd.DataFrame) or all_data_df.empty:
        logger.error("DataFrame de sorteios (all_data_df) injetado está inválido ou vazio.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    
    if not hasattr(db_manager, 'save_dataframe') or \
       not hasattr(db_manager, 'load_dataframe') or \
       not hasattr(db_manager, 'table_exists'):
        logger.error("Objeto db_manager injetado não possui os métodos necessários.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    statistical_tests_table_name = config.STATISTICAL_TESTS_RESULTS_TABLE_NAME
    all_test_results: List[Dict[str, Any]] = [] 

    # --- Teste 1: Qui-Quadrado para Frequência das Dezenas ---
    try:
        sub_step_name_freq = "Qui-Quadrado de Uniformidade da Frequência das Dezenas"
        logger.info(f"--- Iniciando sub-etapa: {sub_step_name_freq} ---")
        
        freq_abs_table_name = "frequencia_absoluta" 
        if not db_manager.table_exists(freq_abs_table_name):
            logger.error(f"Tabela '{freq_abs_table_name}' não existe. Etapa 'frequency_analysis' é pré-requisito.")
            raise FileNotFoundError(f"Tabela '{freq_abs_table_name}' não encontrada.")
        
        observed_frequencies_df = db_manager.load_dataframe(freq_abs_table_name)
        if not isinstance(observed_frequencies_df, pd.DataFrame) or observed_frequencies_df.empty:
            logger.error(f"DataFrame de frequência absoluta da tabela '{freq_abs_table_name}' vazio ou inválido.")
            raise ValueError(f"Dados de frequência absoluta inválidos.")
        
        if not all(col in observed_frequencies_df.columns for col in ['Dezena', 'Frequencia Absoluta']):
            logger.error(f"Tabela '{freq_abs_table_name}' não possui colunas 'Dezena' e 'Frequencia Absoluta'.")
            raise ValueError(f"Colunas esperadas ausentes em '{freq_abs_table_name}'.")

        total_draws = len(all_data_df)
        if total_draws == 0:
            logger.warning("Total de sorteios é 0. Pulando teste Qui-Quadrado de frequência.")
        else:
            chi_square_freq_result = perform_chi_square_test_number_frequencies(
                observed_frequencies_df, total_draws, config
            )
            if chi_square_freq_result:
                all_test_results.append(chi_square_freq_result)
                logger.info(f"Resultado do {sub_step_name_freq} obtido.")
            else:
                logger.warning(f"Falha ao obter resultado para {sub_step_name_freq}.")
        logger.info(f"--- Sub-etapa: {sub_step_name_freq} CONCLUÍDA ---")

    except Exception as e_freq_test:
        logger.error(f"Erro durante a sub-etapa {sub_step_name_freq}: {e_freq_test}", exc_info=True)

    # --- Teste 2: Teste de Normalidade para a Soma das Dezenas ---
    try:
        sub_step_name_sum_norm = "Teste de Normalidade da Soma das Dezenas"
        logger.info(f"--- Iniciando sub-etapa: {sub_step_name_sum_norm} ---")
        
        props_table_name = getattr(config, 'PROPRIEDADES_NUMERICAS_TABLE_NAME', 'propriedades_numericas_por_concurso')
        sum_column_name = 'soma_dezenas'

        if not db_manager.table_exists(props_table_name):
            logger.error(f"Tabela '{props_table_name}' não existe. Etapa 'number_properties' é pré-requisito.")
            raise FileNotFoundError(f"Tabela '{props_table_name}' não encontrada.")

        properties_df = db_manager.load_dataframe(props_table_name)
        if not isinstance(properties_df, pd.DataFrame) or properties_df.empty:
            logger.error(f"DataFrame da tabela '{props_table_name}' vazio ou inválido.")
            raise ValueError(f"Dados de propriedades numéricas inválidos.")

        if sum_column_name not in properties_df.columns:
            logger.error(f"Coluna '{sum_column_name}' não encontrada na tabela '{props_table_name}'.")
            raise ValueError(f"Coluna '{sum_column_name}' ausente.")

        sum_of_numbers_series = properties_df[sum_column_name].dropna()

        if sum_of_numbers_series.empty or sum_of_numbers_series.nunique() < 2:
            logger.warning(f"Série de '{sum_column_name}' está vazia ou sem variação suficiente. Pulando testes de normalidade.")
        else:
            # Teste com Qui-Quadrado (bins)
            sum_normality_chi2_result = perform_normality_test_for_sum_of_numbers(
                sum_of_numbers_series, config, method='chi_square_bins'
            )
            if sum_normality_chi2_result:
                all_test_results.append(sum_normality_chi2_result)
                logger.info("Resultado do Teste de Normalidade da Soma (Qui-Quadrado com Bins) obtido.")
            else:
                logger.warning("Falha ao obter resultado para Teste de Normalidade da Soma (Qui-Quadrado com Bins).")

            # Teste com Kolmogorov-Smirnov
            sum_normality_ks_result = perform_normality_test_for_sum_of_numbers(
                sum_of_numbers_series, config, method='kolmogorov_smirnov'
            )
            if sum_normality_ks_result:
                all_test_results.append(sum_normality_ks_result)
                logger.info("Resultado do Teste de Normalidade da Soma (Kolmogorov-Smirnov) obtido.")
            else:
                logger.warning("Falha ao obter resultado para Teste de Normalidade da Soma (Kolmogorov-Smirnov).")
        logger.info(f"--- Sub-etapa: {sub_step_name_sum_norm} CONCLUÍDA ---")

    except Exception as e_sum_norm_test:
        logger.error(f"Erro durante a sub-etapa {sub_step_name_sum_norm}: {e_sum_norm_test}", exc_info=True)

    # Salvar todos os resultados de testes coletados
    if not all_test_results:
        logger.warning(f"Nenhum resultado de teste estatístico foi gerado para a etapa {step_name}.")
    else:
        results_df_to_save = pd.DataFrame(all_test_results)
        try:
            db_manager.save_dataframe(results_df_to_save, 
                                      statistical_tests_table_name, 
                                      if_exists='append')
            logger.info(f"Resultados dos testes estatísticos ({len(results_df_to_save)} testes) salvos na tabela: {statistical_tests_table_name}")
        except Exception as e_save:
            logger.error(f"Erro ao salvar resultados dos testes estatísticos: {e_save}", exc_info=True)
            logger.info(f"==== Etapa: {step_name} FALHOU (ao salvar resultados) ====")
            return False
            
    shared_context['statistical_tests_results_list'] = all_test_results
    logger.info(f"Lista de resultados dos Testes Estatísticos adicionada ao shared_context.")
            
    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True