# Arquivo: src/pipeline_steps/execute_cycles.py
# (Conteúdo completo da minha sugestão anterior, corrigindo a chamada e usando as chaves e nomes de tabela padronizados)

import pandas as pd
import logging
from typing import Dict, Any, Optional

# Importa a função de análise e as chaves padronizadas
from src.analysis.cycle_analysis import identify_and_process_cycles, KEY_CYCLE_DETAILS_DF, KEY_CYCLE_SUMMARY_DF
# from src.database_manager import DatabaseManager # Para type hint se usar db_manager: DatabaseManager
# from src.config import Config # Para type hint se usar config: Config

logger = logging.getLogger(__name__)

def run_cycle_identification_step(
    all_data_df: pd.DataFrame,
    db_manager: Any, 
    config: Any, 
    shared_context: Dict[str, Any],
    **kwargs
) -> Optional[pd.DataFrame]: 
    step_name = "Cycle Identification and Basic Stats"
    logger.info(f"Iniciando etapa do pipeline: {step_name}")
    df_details_to_return: Optional[pd.DataFrame] = None 

    try:
        # CHAMADA CORRIGIDA: usa 'config' diretamente, que é o config_obj
        # E a função identify_and_process_cycles é importada diretamente.
        cycle_analysis_results = identify_and_process_cycles(all_data_df, config)

        if not cycle_analysis_results:
            logger.warning(f"Nenhum resultado de ciclo retornado para {step_name}.")
            logger.info(f"==== Etapa: {step_name} CONCLUÍDA (sem dados de ciclo) ====")
            return None

        saved_any = False
        # Usando a chave padronizada para obter o DataFrame de detalhes
        df_details = cycle_analysis_results.get(KEY_CYCLE_DETAILS_DF)
        if df_details is not None and not df_details.empty:
            try:
                # Usando o nome da tabela do config para salvar os detalhes
                db_manager.save_dataframe(df_details, config.ANALYSIS_CYCLES_DETAIL_TABLE_NAME, if_exists='replace')
                logger.info(f"Resultados de detalhes de ciclo salvos na tabela '{config.ANALYSIS_CYCLES_DETAIL_TABLE_NAME}'.")
                df_details_to_return = df_details 
                saved_any = True
            except Exception as e_save:
                logger.error(f"Erro ao salvar tabela '{config.ANALYSIS_CYCLES_DETAIL_TABLE_NAME}': {e_save}", exc_info=True)
        else:
            logger.info(f"DataFrame para '{KEY_CYCLE_DETAILS_DF}' vazio ou nulo.")
            if isinstance(df_details, pd.DataFrame): 
                df_details_to_return = df_details


        # Usando a chave padronizada para obter o DataFrame de sumário
        df_summary = cycle_analysis_results.get(KEY_CYCLE_SUMMARY_DF)
        if df_summary is not None and not df_summary.empty:
            try:
                # Usando o nome da tabela do config para salvar o sumário
                db_manager.save_dataframe(df_summary, config.ANALYSIS_CYCLES_SUMMARY_TABLE_NAME, if_exists='replace')
                logger.info(f"Resultados de sumário de ciclo salvos na tabela '{config.ANALYSIS_CYCLES_SUMMARY_TABLE_NAME}'.")
                saved_any = True
            except Exception as e_save_sum:
                logger.error(f"Erro ao salvar tabela '{config.ANALYSIS_CYCLES_SUMMARY_TABLE_NAME}': {e_save_sum}", exc_info=True)
        else:
            logger.info(f"DataFrame para '{KEY_CYCLE_SUMMARY_DF}' vazio ou nulo.")

        if saved_any:
            logger.info(f"Etapa {step_name} concluída com dados salvos.")
        else:
            logger.info(f"Etapa {step_name} concluída, sem novos dados salvos (ou dados já existentes).")

        # Retorna o DataFrame de detalhes para o shared_context, se o orchestrator estiver configurado com output_key
        # (No main.py, para 'cycle_identification', o output_key é 'cycles_detail_df')
        return df_details_to_return

    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        return None