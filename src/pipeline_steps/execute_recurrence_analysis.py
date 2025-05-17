# src/pipeline_steps/execute_recurrence_analysis.py
import logging
import pandas as pd
from typing import Any, Dict # Para os type hints dos argumentos

# Importa a função de análise principal
from src.analysis.recurrence_analysis import analyze_recurrence
# Importa a função auxiliar para obter a matriz de sorteios, que é necessária
from src.analysis.temporal_trend_analysis import get_full_draw_matrix

logger = logging.getLogger(__name__)

def run_recurrence_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: Any,  # Espera-se uma instância de DatabaseManager
    config: Any,      # Espera-se uma instância de Config
    shared_context: Dict[str, Any], # Dicionário de contexto compartilhado
    **kwargs
) -> bool:
    """
    Executa a etapa de Análise de Recorrência para cada dezena.
    Calcula a CDF do atraso atual com base nos gaps históricos.
    Os argumentos são injetados pelo Orchestrator.
    """
    step_name = "Análise de Recorrência Geral"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")
    
    # Validações básicas dos argumentos injetados
    if not hasattr(config, 'RECURRENCE_ANALYSIS_TABLE_NAME') or \
       not hasattr(config, 'CONTEST_ID_COLUMN_NAME') or \
       not hasattr(config, 'DRAWN_NUMBERS_COLUMN_NAME') or \
       not hasattr(config, 'ALL_NUMBERS'):
        logger.error("Atributos de configuração necessários para a Análise de Recorrência não encontrados.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    if not isinstance(all_data_df, pd.DataFrame) or all_data_df.empty:
        logger.error("DataFrame de sorteios (all_data_df) injetado está inválido ou vazio.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    
    if not hasattr(db_manager, 'save_dataframe') or not hasattr(db_manager, 'load_dataframe') or not hasattr(db_manager, 'table_exists'):
        logger.error("Objeto db_manager injetado não possui os métodos necessários ('save_dataframe', 'load_dataframe', 'table_exists').")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    recurrence_table_name = config.RECURRENCE_ANALYSIS_TABLE_NAME
    current_delay_table_name = "atraso_atual" # Nome da tabela de onde carregaremos os atrasos atuais

    try:
        # 1. Carregar o DataFrame de atrasos atuais
        logger.info(f"Carregando dados de atraso atual da tabela '{current_delay_table_name}'...")
        if not db_manager.table_exists(current_delay_table_name):
            logger.error(f"Tabela de atraso atual '{current_delay_table_name}' não existe. A etapa de análise de atraso precisa ser executada primeiro.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False
        
        current_delays_df = db_manager.load_dataframe(current_delay_table_name)
        if not isinstance(current_delays_df, pd.DataFrame) or current_delays_df.empty:
            logger.error(f"DataFrame de atraso atual carregado da tabela '{current_delay_table_name}' está vazio ou inválido.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False
        
        # Renomear colunas se necessário para corresponder ao esperado por analyze_recurrence
        # A tabela 'atraso_atual' tem colunas "Dezena" e "Atraso Atual"
        # A função analyze_recurrence espera 'Dezena' e 'Atraso_Atual'
        if "Dezena" not in current_delays_df.columns or "Atraso Atual" not in current_delays_df.columns:
            logger.error(f"Tabela '{current_delay_table_name}' não possui as colunas esperadas 'Dezena' e 'Atraso Atual'.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False

        # 2. Gerar a matriz de ocorrências (draw_matrix)
        logger.info("Gerando matriz completa de sorteios (ocorrências) para análise de recorrência...")
        draw_matrix = get_full_draw_matrix(all_data_df, config)

        if draw_matrix.empty:
            if not all_data_df.empty:
                 logger.error("Falha ao gerar a matriz de sorteios (ocorrências) a partir de dados não vazios.")
                 logger.info(f"==== Etapa: {step_name} FALHOU ====")
                 return False
            else:
                 logger.info("Dados de entrada vazios, nenhuma análise de recorrência para calcular.")
                 logger.info(f"==== Etapa: {step_name} CONCLUÍDA (sem dados) ====")
                 return True
        
        # 3. Calcular as estatísticas de recorrência
        logger.info("Calculando estatísticas de recorrência...")
        recurrence_stats_df = analyze_recurrence(draw_matrix, current_delays_df, config)

        if not isinstance(recurrence_stats_df, pd.DataFrame):
            logger.error("A análise de recorrência não retornou um DataFrame.")
            logger.info(f"==== Etapa: {step_name} FALHOU ====")
            return False
        
        if recurrence_stats_df.empty and not draw_matrix.empty: # Se draw_matrix não era vazia mas o resultado é
            logger.warning("A análise de recorrência resultou em um DataFrame vazio, embora a matriz de entrada não estivesse vazia.")
        
        # 4. Salvar os resultados
        if recurrence_stats_df.empty:
            logger.info(f"DataFrame de análise de recorrência está vazio. Nada será salvo na tabela '{recurrence_table_name}'.")
        else:
            db_manager.save_dataframe(recurrence_stats_df, 
                                      recurrence_table_name, 
                                      if_exists='replace')
            logger.info(f"Estatísticas de recorrência salvas na tabela: {recurrence_table_name}")
        
        # Adicionar ao contexto compartilhado
        shared_context['geral_recurrence_analysis_df'] = recurrence_stats_df
        logger.info(f"Resultado da {step_name} adicionado ao dicionário shared_context como 'geral_recurrence_analysis_df'.")

    except Exception as e:
        logger.error(f"Erro durante a execução da {step_name}: {e}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
            
    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True