# src/pipeline_steps/execute_statistical_tests.py
import logging
import pandas as pd
from typing import Any, Dict, List # Para os type hints dos argumentos

# Importa a função de análise principal
from src.analysis.statistical_tests_analysis import perform_chi_square_test_number_frequencies

logger = logging.getLogger(__name__)

def run_statistical_tests_step(
    all_data_df: pd.DataFrame,
    db_manager: Any,  # Espera-se uma instância de DatabaseManager
    config: Any,      # Espera-se uma instância de Config
    shared_context: Dict[str, Any], # Dicionário de contexto compartilhado
    **kwargs
) -> bool:
    """
    Executa a etapa de Testes Estatísticos, começando com o Teste Qui-Quadrado
    para a uniformidade da frequência das dezenas.
    Os argumentos são injetados pelo Orchestrator.
    """
    step_name = "Testes Estatísticos (Qui-Quadrado de Frequência)"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")
    
    # Validações básicas
    if not hasattr(config, 'STATISTICAL_TESTS_RESULTS_TABLE_NAME') or \
       not hasattr(config, 'NUMBERS_PER_DRAW') or \
       not hasattr(config, 'ALL_NUMBERS'):
        logger.error("Atributos de configuração necessários para Testes Estatísticos não encontrados.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    if not isinstance(all_data_df, pd.DataFrame) or all_data_df.empty:
        logger.error("DataFrame de sorteios (all_data_df) injetado está inválido ou vazio.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    
    if not hasattr(db_manager, 'save_dataframe') or not hasattr(db_manager, 'load_dataframe') or not hasattr(db_manager, 'table_exists'):
        logger.error("Objeto db_manager injetado não possui os métodos necessários.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    statistical_tests_table_name = config.STATISTICAL_TESTS_RESULTS_TABLE_NAME
    # A tabela de frequência absoluta pode ter um nome diferente no config, mas o default é 'frequencia_absoluta'
    # Vamos assumir que o nome da coluna de frequência é "Frequencia Absoluta" e de dezena é "Dezena"
    # conforme o schema definido em database_manager para _create_table_frequencia_absoluta
    freq_abs_table_name = "frequencia_absoluta" 

    try:
        # 1. Carregar o DataFrame de frequências absolutas observadas
        logger.info(f"Carregando dados de frequência absoluta da tabela '{freq_abs_table_name}'...")
        if not db_manager.table_exists(freq_abs_table_name):
            logger.error(f"Tabela de frequência absoluta '{freq_abs_table_name}' não existe. A etapa 'frequency_analysis' precisa ser executada primeiro.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False
        
        observed_frequencies_df = db_manager.load_dataframe(freq_abs_table_name)
        if not isinstance(observed_frequencies_df, pd.DataFrame) or observed_frequencies_df.empty:
            logger.error(f"DataFrame de frequência absoluta carregado da tabela '{freq_abs_table_name}' está vazio ou inválido.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False
        
        # Verifica se as colunas esperadas existem
        if not all(col in observed_frequencies_df.columns for col in ['Dezena', 'Frequencia Absoluta']):
            logger.error(f"Tabela '{freq_abs_table_name}' não possui as colunas esperadas 'Dezena' e 'Frequencia Absoluta'. Colunas: {observed_frequencies_df.columns}")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False

        # 2. Obter o número total de sorteios
        total_draws = len(all_data_df)
        if total_draws == 0:
            logger.warning("Número total de sorteios é 0. Não é possível realizar o teste Qui-Quadrado.")
            logger.info(f"==== Etapa: {step_name} CONCLUÍDA (sem dados para teste) ====")
            return True # Considera sucesso pois não houve erro, apenas sem dados.
        
        # 3. Realizar o Teste Qui-Quadrado
        logger.info("Realizando Teste Qui-Quadrado para uniformidade da frequência das dezenas...")
        chi_square_result_dict = perform_chi_square_test_number_frequencies(
            observed_frequencies_df, total_draws, config
        )

        if chi_square_result_dict is None:
            logger.error("Falha ao realizar o Teste Qui-Quadrado (resultado None). Verifique logs da função de análise.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False
        
        # 4. Salvar o resultado
        # O resultado é um dicionário, transformamos em um DataFrame de uma linha para salvar
        result_df_to_save = pd.DataFrame([chi_square_result_dict])
        
        if result_df_to_save.empty:
            logger.info(f"Resultado do Teste Qui-Quadrado está vazio. Nada será salvo na tabela '{statistical_tests_table_name}'.")
        else:
            # Usar 'append' para esta tabela, pois podemos adicionar resultados de outros testes no futuro.
            # Ou 'replace' se cada execução desta etapa deve sobrescrever apenas este teste específico.
            # Para uma tabela genérica de resultados de testes, 'append' é mais comum.
            # No entanto, se o Test_ID é AUTOINCREMENT, 'append' é ideal.
            # Se o Test_Name for usado para identificar unicamente um teste, poderíamos ter uma lógica de update/insert.
            # Para simplicidade, vamos usar append.
            db_manager.save_dataframe(result_df_to_save, 
                                      statistical_tests_table_name, 
                                      if_exists='append') # Alterado para append
            logger.info(f"Resultado do Teste Qui-Quadrado salvo na tabela: {statistical_tests_table_name}")
        
        # Adicionar ao contexto compartilhado (o dicionário de resultado)
        # Poderíamos adicionar uma lista de resultados se esta etapa realizar múltiplos testes
        shared_context['chi_square_freq_uniformity_result'] = chi_square_result_dict
        logger.info(f"Resultado do Teste Qui-Quadrado adicionado ao dicionário shared_context.")

    except Exception as e:
        logger.error(f"Erro durante a execução da {step_name}: {e}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
            
    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True