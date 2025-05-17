# src/pipeline_steps/execute_temporal_trend_analysis.py
import logging
import pandas as pd
from typing import Any, Dict # Para os type hints dos argumentos

# Importa as funções de análise do módulo de tendências temporais
from src.analysis.temporal_trend_analysis import (
    get_full_draw_matrix, 
    calculate_moving_average_frequency,
    get_historical_delay_matrix,      # Nova importação
    calculate_moving_average_delay    # Nova importação
)

logger = logging.getLogger(__name__)

def run_temporal_trend_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: Any,  # Espera-se uma instância de DatabaseManager
    config: Any,      # Espera-se uma instância de Config
    shared_context: Dict[str, Any], # Dicionário de contexto compartilhado
    **kwargs
) -> bool:
    """
    Executa a etapa de análise de tendências temporais, incluindo:
    1. Média Móvel de Frequência Geral.
    2. Média Móvel de Atraso (Atual Instantâneo) Geral.
    Os argumentos são injetados pelo Orchestrator.
    """
    step_name = "Análise de Tendências Temporais (Médias Móveis)" # Nome da etapa atualizado
    logger.info(f"==== Iniciando Etapa: {step_name} ====")
    
    # Validações básicas dos argumentos injetados
    if not isinstance(all_data_df, pd.DataFrame) or all_data_df.empty:
        logger.error("DataFrame de sorteios (all_data_df) injetado está inválido ou vazio.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    
    if not hasattr(db_manager, 'save_dataframe'):
        logger.error("Objeto db_manager injetado não possui o método 'save_dataframe'.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    if not config:
        logger.error("Objeto config não foi injetado corretamente.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    # --- Média Móvel de Frequência ---
    try:
        logger.info("--- Iniciando sub-etapa: Média Móvel de Frequência ---")
        if not hasattr(config, 'GERAL_MA_FREQUENCY_WINDOWS') or \
           not hasattr(config, 'GERAL_MA_FREQUENCY_TABLE_NAME'):
            logger.error("Atributos de configuração para M.A. de Frequência não encontrados.")
            raise AttributeError("Configuração para M.A. de Frequência ausente.")

        geral_ma_frequency_table_name = config.GERAL_MA_FREQUENCY_TABLE_NAME
        freq_windows = config.GERAL_MA_FREQUENCY_WINDOWS

        logger.info("Gerando matriz completa de sorteios (ocorrências)...")
        draw_matrix = get_full_draw_matrix(all_data_df, config)

        if draw_matrix.empty:
            if not all_data_df.empty:
                 logger.error("Falha ao gerar a matriz de sorteios (ocorrências) a partir de dados não vazios.")
                 raise ValueError("Matriz de ocorrências não pôde ser gerada.")
            else:
                 logger.info("Dados de entrada vazios, nenhuma M.A. de Frequência para calcular.")
        else:
            logger.info(f"Calculando média móvel de frequência para janelas: {freq_windows}...")
            ma_frequency_df = calculate_moving_average_frequency(draw_matrix, freq_windows, config)

            if not isinstance(ma_frequency_df, pd.DataFrame):
                logger.error("A análise de média móvel de frequência não retornou um DataFrame.")
                raise TypeError("Resultado inesperado da análise de M.A. de Frequência.")
            
            if ma_frequency_df.empty and not draw_matrix.empty:
                logger.warning("M.A. de Frequência resultou em DataFrame vazio, embora a matriz de entrada não estivesse.")
            
            if ma_frequency_df.empty:
                logger.info(f"DataFrame de M.A. de Frequência vazio. Nada salvo em '{geral_ma_frequency_table_name}'.")
            else:
                db_manager.save_dataframe(ma_frequency_df, geral_ma_frequency_table_name, if_exists='replace')
                logger.info(f"M.A. de Frequência salva na tabela: {geral_ma_frequency_table_name}")
            
            shared_context['geral_ma_frequency_df'] = ma_frequency_df
            logger.info("Resultado da M.A. de Frequência adicionado ao shared_context.")
        logger.info("--- Sub-etapa: Média Móvel de Frequência CONCLUÍDA ---")

    except Exception as e_freq:
        logger.error(f"Erro durante a sub-etapa de Média Móvel de Frequência: {e_freq}", exc_info=True)
        # Decide se a falha em uma sub-etapa deve parar toda a etapa temporal_trend_analysis
        # Por ora, vamos continuar para a M.A. de Atraso, mas logamos o erro.
        # Se for crítico, pode-se retornar False aqui.


    # --- Média Móvel de Atraso (Atual Instantâneo) ---
    # A matriz de ocorrências (draw_matrix) já foi gerada acima e pode ser reutilizada.
    try:
        logger.info("--- Iniciando sub-etapa: Média Móvel de Atraso ---")
        if not hasattr(config, 'GERAL_MA_DELAY_WINDOWS') or \
           not hasattr(config, 'GERAL_MA_DELAY_TABLE_NAME'):
            logger.error("Atributos de configuração para M.A. de Atraso não encontrados.")
            raise AttributeError("Configuração para M.A. de Atraso ausente.")

        geral_ma_delay_table_name = config.GERAL_MA_DELAY_TABLE_NAME
        delay_windows = config.GERAL_MA_DELAY_WINDOWS

        if draw_matrix.empty: # Se a draw_matrix não pôde ser gerada antes
            if not all_data_df.empty:
                logger.error("Matriz de ocorrências está vazia, não é possível calcular M.A. de Atraso.")
                raise ValueError("Matriz de ocorrências necessária para M.A. de Atraso está vazia.")
            else:
                logger.info("Dados de entrada vazios, nenhuma M.A. de Atraso para calcular.")
        else:
            logger.info("Gerando matriz de atraso histórico...")
            historical_delay_matrix = get_historical_delay_matrix(draw_matrix, config)

            if historical_delay_matrix.empty:
                if not draw_matrix.empty:
                    logger.error("Falha ao gerar a matriz de atraso histórico a partir de uma matriz de ocorrências não vazia.")
                    raise ValueError("Matriz de atraso histórico não pôde ser gerada.")
                else: # draw_matrix estava vazia, o que é esperado se all_data_df era vazio.
                    logger.info("Matriz de ocorrências vazia, nenhuma M.A. de Atraso para calcular.")
            else:
                logger.info(f"Calculando média móvel de atraso para janelas: {delay_windows}...")
                ma_delay_df = calculate_moving_average_delay(historical_delay_matrix, delay_windows, config)

                if not isinstance(ma_delay_df, pd.DataFrame):
                    logger.error("A análise de média móvel de atraso não retornou um DataFrame.")
                    raise TypeError("Resultado inesperado da análise de M.A. de Atraso.")
                
                if ma_delay_df.empty and not historical_delay_matrix.empty:
                    logger.warning("M.A. de Atraso resultou em DataFrame vazio, embora a matriz de entrada não estivesse.")

                if ma_delay_df.empty:
                    logger.info(f"DataFrame de M.A. de Atraso vazio. Nada salvo em '{geral_ma_delay_table_name}'.")
                else:
                    db_manager.save_dataframe(ma_delay_df, geral_ma_delay_table_name, if_exists='replace')
                    logger.info(f"M.A. de Atraso salva na tabela: {geral_ma_delay_table_name}")
                
                shared_context['geral_ma_delay_df'] = ma_delay_df
                logger.info("Resultado da M.A. de Atraso adicionado ao shared_context.")
        logger.info("--- Sub-etapa: Média Móvel de Atraso CONCLUÍDA ---")

    except Exception as e_delay:
        logger.error(f"Erro durante a sub-etapa de Média Móvel de Atraso: {e_delay}", exc_info=True)
        # Se esta sub-etapa falhar, a etapa geral falha.
        logger.info(f"==== Etapa: {step_name} FALHOU (devido a erro na M.A. de Atraso) ====")
        return False
            
    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True