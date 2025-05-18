# src/pipeline_steps/execute_seasonality_analysis.py
import logging
import pandas as pd
from typing import Any, Dict 

# Importa as funções de análise do módulo de sazonalidade
from src.analysis.seasonality_analysis import (
    analyze_monthly_number_frequency,
    analyze_monthly_draw_properties # Nova importação
)

logger = logging.getLogger(__name__)

def run_seasonality_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: Any, 
    config: Any,      
    shared_context: Dict[str, Any], 
    **kwargs
) -> bool:
    """
    Executa a etapa de Análise Sazonal, incluindo:
    1. Frequência de Dezenas por Mês.
    2. Sumário de Propriedades Numéricas dos Sorteios por Mês.
    Os argumentos são injetados pelo Orchestrator.
    """
    step_name = "Análise Sazonal" # Nome geral da etapa
    logger.info(f"==== Iniciando Etapa: {step_name} ====")
    
    # Validações básicas (podem ser expandidas conforme necessário)
    if not isinstance(all_data_df, pd.DataFrame) or all_data_df.empty:
        logger.error("DataFrame de sorteios (all_data_df) injetado está inválido ou vazio.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    if not hasattr(db_manager, 'save_dataframe') or not hasattr(db_manager, 'load_dataframe'):
        logger.error("Objeto db_manager injetado não possui os métodos 'save_dataframe' ou 'load_dataframe'.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    if not config:
        logger.error("Objeto config não foi injetado corretamente.")
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False

    # --- Sub-etapa 1: Frequência Mensal de Dezenas ---
    try:
        sub_step_name_freq = "Frequência Mensal de Dezenas"
        logger.info(f"--- Iniciando sub-etapa: {sub_step_name_freq} ---")

        if not hasattr(config, 'MONTHLY_NUMBER_FREQUENCY_TABLE_NAME'):
            logger.error("Config 'MONTHLY_NUMBER_FREQUENCY_TABLE_NAME' não encontrada.")
            raise AttributeError("Configuração para tabela de frequência mensal ausente.")

        monthly_freq_table_name = config.MONTHLY_NUMBER_FREQUENCY_TABLE_NAME
        
        monthly_frequency_df = analyze_monthly_number_frequency(all_data_df, config)

        if not isinstance(monthly_frequency_df, pd.DataFrame):
            logger.error("A análise de frequência mensal não retornou um DataFrame.")
            raise TypeError("Resultado inesperado da análise de frequência mensal.")
        
        if monthly_frequency_df.empty and not all_data_df.empty:
            logger.warning(f"DataFrame de frequência mensal vazio. Nada será salvo na tabela '{monthly_freq_table_name}'.")
        elif not monthly_frequency_df.empty:
            db_manager.save_dataframe(monthly_frequency_df, monthly_freq_table_name, if_exists='replace')
            logger.info(f"Frequência mensal das dezenas salva na tabela: {monthly_freq_table_name}")
        
        shared_context['monthly_number_frequency_df'] = monthly_frequency_df
        logger.info(f"Resultado da {sub_step_name_freq} adicionado ao shared_context.")
        logger.info(f"--- Sub-etapa: {sub_step_name_freq} CONCLUÍDA ---")

    except Exception as e_freq:
        logger.error(f"Erro durante a sub-etapa {sub_step_name_freq}: {e_freq}", exc_info=True)
        # Decide se continua para a próxima sub-etapa ou falha a etapa inteira
        # Por ora, vamos logar e tentar continuar. Se for crítico, retorne False.


    # --- Sub-etapa 2: Sumário Mensal de Propriedades Numéricas ---
    try:
        sub_step_name_props = "Sumário Mensal de Propriedades Numéricas"
        logger.info(f"--- Iniciando sub-etapa: {sub_step_name_props} ---")

        if not hasattr(config, 'MONTHLY_DRAW_PROPERTIES_TABLE_NAME'):
            logger.error("Config 'MONTHLY_DRAW_PROPERTIES_TABLE_NAME' não encontrada.")
            raise AttributeError("Configuração para tabela de propriedades mensais ausente.")
        
        # Nome da tabela de onde carregar as propriedades por concurso
        # (Assumindo que esta tabela é gerada por uma etapa anterior como 'number_properties')
        props_per_concurso_table_name = getattr(config, 'PROPRIEDADES_NUMERICAS_TABLE_NAME', 'propriedades_numericas_por_concurso')
        
        if not db_manager.table_exists(props_per_concurso_table_name):
            logger.error(f"Tabela de propriedades por concurso '{props_per_concurso_table_name}' não existe. Etapa 'number_properties' é pré-requisito.")
            raise FileNotFoundError(f"Tabela '{props_per_concurso_table_name}' não encontrada.")

        properties_df = db_manager.load_dataframe(props_per_concurso_table_name)
        if not isinstance(properties_df, pd.DataFrame) or properties_df.empty:
            logger.error(f"DataFrame da tabela '{props_per_concurso_table_name}' vazio ou inválido.")
            raise ValueError(f"Dados de propriedades numéricas por concurso inválidos para {sub_step_name_props}.")

        monthly_props_table_name = config.MONTHLY_DRAW_PROPERTIES_TABLE_NAME
        monthly_draw_properties_df = analyze_monthly_draw_properties(all_data_df, properties_df, config)

        if not isinstance(monthly_draw_properties_df, pd.DataFrame):
            logger.error("A análise de propriedades mensais não retornou um DataFrame.")
            raise TypeError("Resultado inesperado da análise de propriedades mensais.")

        if monthly_draw_properties_df.empty:
            logger.info(f"DataFrame de sumário de propriedades mensais vazio. Nada será salvo na tabela '{monthly_props_table_name}'.")
        else:
            db_manager.save_dataframe(monthly_draw_properties_df, monthly_props_table_name, if_exists='replace')
            logger.info(f"Sumário de propriedades mensais salvo na tabela: {monthly_props_table_name}")
            
        shared_context['monthly_draw_properties_df'] = monthly_draw_properties_df
        logger.info(f"Resultado do {sub_step_name_props} adicionado ao shared_context.")
        logger.info(f"--- Sub-etapa: {sub_step_name_props} CONCLUÍDA ---")

    except Exception as e_props:
        logger.error(f"Erro durante a sub-etapa {sub_step_name_props}: {e_props}", exc_info=True)
        # Se esta sub-etapa falhar, a etapa geral pode ser considerada falha
        logger.info(f"==== Etapa: {step_name} FALHOU (devido a erro na sub-etapa de propriedades) ====")
        return False
            
    logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
    return True