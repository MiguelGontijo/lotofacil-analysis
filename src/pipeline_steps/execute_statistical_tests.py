# src/pipeline_steps/execute_statistical_tests.py
import logging
import pandas as pd
from typing import Any, Dict, List 

# Importa as funções de análise
from src.analysis.statistical_tests_analysis import (
    perform_chi_square_test_number_frequencies,
    perform_normality_test_for_sum_of_numbers,
    perform_poisson_distribution_test # Nova importação
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
    2. Teste de Normalidade para a soma das dezenas sorteadas (Qui-Quadrado e K-S).
    3. Teste de Aderência à Distribuição de Poisson para contagens de eventos configurados.
    Os argumentos são injetados pelo Orchestrator.
    """
    step_name = "Testes Estatísticos"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")
    
    # Validações básicas
    if not hasattr(config, 'STATISTICAL_TESTS_RESULTS_TABLE_NAME') or \
       not hasattr(config, 'NUMBERS_PER_DRAW') or \
       not hasattr(config, 'ALL_NUMBERS') or \
       not hasattr(config, 'SUM_NORMALITY_TEST_BINS') or \
       not hasattr(config, 'POISSON_DISTRIBUTION_TEST_CONFIG'): # Nova verificação
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
    # Tabela de propriedades numéricas é carregada uma vez aqui
    props_table_name = getattr(config, 'PROPRIEDADES_NUMERICAS_TABLE_NAME', 'propriedades_numericas_por_concurso')
    properties_df = None
    if db_manager.table_exists(props_table_name):
        properties_df = db_manager.load_dataframe(props_table_name)
    
    if not isinstance(properties_df, pd.DataFrame) or properties_df.empty:
        logger.warning(f"DataFrame da tabela '{props_table_name}' vazio, inválido ou não existe. Pulando testes que dependem dele (Normalidade da Soma, Poisson).")
    else:
        sum_column_name = 'soma_dezenas'
        if sum_column_name not in properties_df.columns:
            logger.warning(f"Coluna '{sum_column_name}' não encontrada na tabela '{props_table_name}'. Pulando teste de normalidade da soma.")
        else:
            sum_of_numbers_series = properties_df[sum_column_name].dropna()
            if sum_of_numbers_series.empty or sum_of_numbers_series.nunique() < 2:
                logger.warning(f"Série de '{sum_column_name}' está vazia ou sem variação. Pulando testes de normalidade da soma.")
            else:
                # Teste com Qui-Quadrado (bins)
                try:
                    logger.info("--- Iniciando sub-etapa: Teste de Normalidade da Soma (Qui-Quadrado Bins) ---")
                    sum_normality_chi2_result = perform_normality_test_for_sum_of_numbers(
                        sum_of_numbers_series, config, method='chi_square_bins'
                    )
                    if sum_normality_chi2_result:
                        all_test_results.append(sum_normality_chi2_result)
                        logger.info("Resultado do Teste de Normalidade da Soma (Qui-Quadrado com Bins) obtido.")
                    else:
                        logger.warning("Falha ao obter resultado para Teste de Normalidade da Soma (Qui-Quadrado com Bins).")
                    logger.info("--- Sub-etapa: Teste de Normalidade da Soma (Qui-Quadrado Bins) CONCLUÍDA ---")
                except Exception as e_sum_norm_chi2:
                    logger.error(f"Erro no Teste de Normalidade da Soma (Qui-Quadrado Bins): {e_sum_norm_chi2}", exc_info=True)

                # Teste com Kolmogorov-Smirnov
                try:
                    logger.info("--- Iniciando sub-etapa: Teste de Normalidade da Soma (Kolmogorov-Smirnov) ---")
                    sum_normality_ks_result = perform_normality_test_for_sum_of_numbers(
                        sum_of_numbers_series, config, method='kolmogorov_smirnov'
                    )
                    if sum_normality_ks_result:
                        all_test_results.append(sum_normality_ks_result)
                        logger.info("Resultado do Teste de Normalidade da Soma (Kolmogorov-Smirnov) obtido.")
                    else:
                        logger.warning("Falha ao obter resultado para Teste de Normalidade da Soma (Kolmogorov-Smirnov).")
                    logger.info("--- Sub-etapa: Teste de Normalidade da Soma (Kolmogorov-Smirnov) CONCLUÍDA ---")
                except Exception as e_sum_norm_ks:
                    logger.error(f"Erro no Teste de Normalidade da Soma (Kolmogorov-Smirnov): {e_sum_norm_ks}", exc_info=True)

        # --- Teste 3: Testes de Aderência à Distribuição de Poisson ---
        poisson_test_config = getattr(config, 'POISSON_DISTRIBUTION_TEST_CONFIG', {})
        if not poisson_test_config:
            logger.info("Nenhuma configuração para testes de Poisson encontrada. Pulando.")
        else:
            for event_key, event_params in poisson_test_config.items():
                try:
                    col_name_for_poisson = event_params.get("column_name")
                    event_desc_for_log = event_params.get("event_description", event_key) # Usa event_key como fallback
                    
                    sub_step_name_poisson = f"Teste de Poisson para '{event_desc_for_log}'"
                    logger.info(f"--- Iniciando sub-etapa: {sub_step_name_poisson} ---")

                    if not col_name_for_poisson:
                        logger.warning(f"Nome da coluna não especificado para o teste de Poisson '{event_key}'. Pulando este teste.")
                        continue
                    
                    if col_name_for_poisson not in properties_df.columns:
                        logger.warning(f"Coluna '{col_name_for_poisson}' para o teste de Poisson '{event_key}' não encontrada na tabela '{props_table_name}'. Pulando.")
                        continue
                    
                    observed_event_counts_series = properties_df[col_name_for_poisson].dropna()

                    if observed_event_counts_series.empty:
                        logger.warning(f"Série de contagens para '{col_name_for_poisson}' (evento '{event_key}') está vazia. Pulando teste de Poisson.")
                        continue
                    
                    # Passa a descrição do evento para a função de análise, se não já em event_params
                    if "event_description" not in event_params:
                         event_params_copy = event_params.copy()
                         event_params_copy["event_description"] = event_desc_for_log # Adiciona descrição para logs dentro da função
                    else:
                         event_params_copy = event_params

                    poisson_result = perform_poisson_distribution_test(
                        observed_event_counts_series, event_params_copy # Passa a cópia com a descrição
                    )
                    if poisson_result:
                        all_test_results.append(poisson_result)
                        logger.info(f"Resultado do {sub_step_name_poisson} obtido.")
                    else:
                        logger.warning(f"Falha ao obter resultado para {sub_step_name_poisson}.")
                    logger.info(f"--- Sub-etapa: {sub_step_name_poisson} CONCLUÍDA ---")
                except Exception as e_poisson_test:
                    logger.error(f"Erro durante a sub-etapa {sub_step_name_poisson}: {e_poisson_test}", exc_info=True)


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
            return False # Considera falha da etapa se não conseguir salvar
            
    shared_context['statistical_tests_results_list'] = all_test_results
    logger.info(f"Lista de resultados dos Testes Estatísticos adicionada ao shared_context.")
            
    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True