# src/pipeline_steps/execute_seasonality_analysis.py
import logging
import pandas as pd
from typing import Any, Dict # Para os type hints dos argumentos

# Importa a função de análise principal
from src.analysis.seasonality_analysis import analyze_monthly_number_frequency

logger = logging.getLogger(__name__)

def run_seasonality_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: Any,  # Espera-se uma instância de DatabaseManager
    config: Any,      # Espera-se uma instância de Config
    shared_context: Dict[str, Any], # Dicionário de contexto compartilhado
    **kwargs
) -> bool:
    """
    Executa a etapa de Análise Sazonal (Frequência de Dezenas por Mês).
    Os argumentos são injetados pelo Orchestrator.
    """
    step_name = "Análise Sazonal (Frequência Mensal de Dezenas)"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")
    
    # Validações básicas dos argumentos e configurações injetados
    if not hasattr(config, 'DATE_COLUMN_NAME') or \
       not hasattr(config, 'DRAWN_NUMBERS_COLUMN_NAME') or \
       not hasattr(config, 'ALL_NUMBERS') or \
       not hasattr(config, 'MONTHLY_NUMBER_FREQUENCY_TABLE_NAME'):
        logger.error("Atributos de configuração necessários para a Análise Sazonal não encontrados.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    if not isinstance(all_data_df, pd.DataFrame) or all_data_df.empty:
        logger.error("DataFrame de sorteios (all_data_df) injetado está inválido ou vazio.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    
    if not hasattr(db_manager, 'save_dataframe'):
        logger.error("Objeto db_manager injetado não possui o método 'save_dataframe'.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    monthly_freq_table_name = config.MONTHLY_NUMBER_FREQUENCY_TABLE_NAME

    try:
        logger.info("Calculando frequência mensal das dezenas...")
        monthly_frequency_df = analyze_monthly_number_frequency(all_data_df, config)

        # Validação do DataFrame retornado
        if not isinstance(monthly_frequency_df, pd.DataFrame):
            logger.error("A análise de frequência mensal não retornou um DataFrame.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False
        
        # Salvar resultados
        if monthly_frequency_df.empty:
            # A função de análise já loga se o df de entrada estava vazio.
            # Aqui, logamos se o resultado é vazio mesmo com dados de entrada.
            if not all_data_df.empty:
                 logger.warning(f"DataFrame de frequência mensal está vazio apesar dos dados de entrada. Nada será salvo na tabela '{monthly_freq_table_name}'.")
            else:
                 logger.info(f"DataFrame de frequência mensal está vazio (dados de entrada vazios). Nada será salvo na tabela '{monthly_freq_table_name}'.")
        else:
            db_manager.save_dataframe(monthly_frequency_df, 
                                      monthly_freq_table_name, 
                                      if_exists='replace')
            logger.info(f"Frequência mensal das dezenas salva na tabela: {monthly_freq_table_name}")
        
        # Adicionar ao contexto compartilhado
        shared_context['monthly_number_frequency_df'] = monthly_frequency_df
        logger.info(f"Resultado da {step_name} adicionado ao dicionário shared_context como 'monthly_number_frequency_df'.")

    except Exception as e:
        logger.error(f"Erro durante a execução da {step_name}: {e}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
            
    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True