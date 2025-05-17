# src/pipeline_steps/execute_positional_analysis.py
import logging
import pandas as pd
from typing import Any, Dict # Para os type hints dos argumentos
from src.analysis.positional_analysis import analyze_draw_position_frequency
# Nenhuma importação da classe SharedContext é necessária aqui

logger = logging.getLogger(__name__)

def run_positional_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: Any,  # Espera-se uma instância de DatabaseManager
    config: Any,      # Espera-se uma instância de Config
    shared_context: Dict[str, Any], # Dicionário de contexto compartilhado
    **kwargs
) -> bool:
    """
    Executa a etapa de análise de frequência posicional.
    Calcula e salva a frequência com que cada dezena aparece em cada
    posição de sorteio (1ª a 15ª bola).
    Os argumentos são injetados pelo Orchestrator.
    """
    step_name = "Análise de Frequência Posicional"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")
    
    # Acessa config, db_manager, e all_data_df diretamente dos argumentos.
    # A constante DRAW_POSITION_FREQUENCY_TABLE_NAME é acessada via objeto config.
    # É importante que 'config' realmente seja uma instância da classe Config
    # e 'db_manager' uma instância de DatabaseManager para que os atributos
    # e métodos esperados (config.DRAW_POSITION_FREQUENCY_TABLE_NAME, db_manager.save_dataframe) existam.

    if not hasattr(config, 'DRAW_POSITION_FREQUENCY_TABLE_NAME'):
        logger.error("Atributo 'DRAW_POSITION_FREQUENCY_TABLE_NAME' não encontrado no objeto config injetado.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    draw_position_frequency_table_name = config.DRAW_POSITION_FREQUENCY_TABLE_NAME

    if not isinstance(all_data_df, pd.DataFrame) or all_data_df.empty:
        logger.error("DataFrame de sorteios (all_data_df) injetado está inválido ou vazio.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    
    if not hasattr(db_manager, 'save_dataframe'):
        logger.error("Objeto db_manager injetado não possui o método 'save_dataframe'.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    try:
        logger.info("Chamando analyze_draw_position_frequency...")
        # A função de análise recebe o DataFrame e o objeto config
        positional_freq_df = analyze_draw_position_frequency(all_data_df, config)
        
        if not isinstance(positional_freq_df, pd.DataFrame):
            logger.error("A análise de frequência posicional não retornou um DataFrame.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False

        if positional_freq_df.empty and not all_data_df.empty:
            logger.warning("A análise de frequência posicional resultou em um DataFrame vazio, embora os dados de entrada não estivessem vazios. Verifique os logs da função de análise.")
        
        if positional_freq_df.empty:
            logger.info(f"DataFrame de frequência posicional está vazio. Nada será salvo na tabela '{draw_position_frequency_table_name}'.")
        else:
            db_manager.save_dataframe(positional_freq_df, 
                                      draw_position_frequency_table_name, 
                                      if_exists='replace')
            logger.info(f"Frequência posicional salva na tabela: {draw_position_frequency_table_name}")
        
        # Adiciona dados de volta ao dicionário shared_context, seguindo o padrão
        shared_context['positional_frequency_df'] = positional_freq_df
        logger.info(f"Resultado da {step_name} adicionado ao dicionário shared_context como 'positional_frequency_df'.")

    except Exception as e:
        logger.error(f"Erro durante a execução da {step_name}: {e}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
            
    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True