# src/pipeline_steps/execute_temporal_trend_analysis.py
import logging
import pandas as pd
from typing import Any, Dict # Para os type hints dos argumentos

# Importa as funções de análise do novo módulo
from src.analysis.temporal_trend_analysis import get_full_draw_matrix, calculate_moving_average_frequency

logger = logging.getLogger(__name__)

def run_temporal_trend_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: Any,  # Espera-se uma instância de DatabaseManager
    config: Any,      # Espera-se uma instância de Config
    shared_context: Dict[str, Any], # Dicionário de contexto compartilhado
    **kwargs
) -> bool:
    """
    Executa a etapa de análise de tendências temporais, começando com a
    Média Móvel de Frequência Geral.
    Os argumentos são injetados pelo Orchestrator.
    """
    step_name = "Análise de Tendências Temporais (Média Móvel de Frequência)"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")
    
    # Validações básicas dos argumentos injetados (opcional, mas bom para robustez)
    if not hasattr(config, 'GERAL_MA_FREQUENCY_WINDOWS') or \
       not hasattr(config, 'GERAL_MA_FREQUENCY_TABLE_NAME') or \
       not hasattr(config, 'CONTEST_ID_COLUMN_NAME') or \
       not hasattr(config, 'DRAWN_NUMBERS_COLUMN_NAME') or \
       not hasattr(config, 'ALL_NUMBERS'):
        logger.error("Atributos de configuração necessários para a análise de média móvel de frequência não encontrados.")
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

    geral_ma_frequency_table_name = config.GERAL_MA_FREQUENCY_TABLE_NAME
    windows = config.GERAL_MA_FREQUENCY_WINDOWS

    try:
        # 1. Gerar a matriz de ocorrências
        logger.info("Gerando matriz completa de sorteios para análise de média móvel...")
        draw_matrix = get_full_draw_matrix(all_data_df, config)

        if draw_matrix.empty:
            logger.warning("Matriz de sorteios gerada está vazia. Não é possível calcular médias móveis.")
            # Considerar se isso deve ser uma falha da etapa ou apenas um aviso.
            # Se draw_matrix vazia for um estado esperado (ex: all_data_df vazio já tratado),
            # então talvez apenas logar e retornar True. Mas se all_data_df não era vazio,
            # uma draw_matrix vazia indica um problema em get_full_draw_matrix.
            # A função get_full_draw_matrix já loga se all_data_df for vazio.
            # Se all_data_df não é vazio mas draw_matrix é, é um problema.
            if not all_data_df.empty:
                 logger.error("Falha ao gerar a matriz de sorteios a partir de dados não vazios.")
                 logger.info(f"==== Etapa: {step_name} FALHOU ====")
                 return False
            else: # all_data_df estava vazio, get_full_draw_matrix retornou vazio, o que é esperado.
                 logger.info("Dados de entrada vazios, nenhuma média móvel de frequência para calcular.")
                 logger.info(f"==== Etapa: {step_name} CONCLUÍDA (sem dados) ====")
                 return True


        # 2. Calcular a Média Móvel da Frequência
        logger.info(f"Calculando média móvel de frequência para janelas: {windows}...")
        ma_frequency_df = calculate_moving_average_frequency(draw_matrix, windows, config)

        if not isinstance(ma_frequency_df, pd.DataFrame):
            logger.error("A análise de média móvel de frequência não retornou um DataFrame.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False
        
        if ma_frequency_df.empty and not draw_matrix.empty:
            logger.warning("A análise de média móvel de frequência resultou em um DataFrame vazio, embora a matriz de entrada não estivesse vazia.")
        
        # 3. Salvar os resultados
        if ma_frequency_df.empty:
            logger.info(f"DataFrame de média móvel de frequência está vazio. Nada será salvo na tabela '{geral_ma_frequency_table_name}'.")
        else:
            db_manager.save_dataframe(ma_frequency_df, 
                                      geral_ma_frequency_table_name, 
                                      if_exists='replace')
            logger.info(f"Média móvel de frequência salva na tabela: {geral_ma_frequency_table_name}")
        
        # Adicionar ao contexto compartilhado, se necessário para etapas futuras
        shared_context['geral_ma_frequency_df'] = ma_frequency_df
        logger.info(f"Resultado da Média Móvel de Frequência adicionado ao dicionário shared_context como 'geral_ma_frequency_df'.")

    except Exception as e:
        logger.error(f"Erro durante a execução da {step_name}: {e}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
            
    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True