# Arquivo: src/pipeline_steps/execute_cycles.py
# Conteúdo baseado no seu all_project_sources.txt
# com as devidas correções.

import pandas as pd
import logging
from typing import Dict, Any, Optional

# Importa a função de análise e as chaves padronizadas
from src.analysis.cycle_analysis import identify_and_process_cycles, KEY_CYCLE_DETAILS_DF, KEY_CYCLE_SUMMARY_DF

logger = logging.getLogger(__name__)

def run_cycle_identification_step(
    all_data_df: pd.DataFrame,
    db_manager: Any, 
    config: Any, 
    shared_context: Dict[str, Any],
    **kwargs
) -> Optional[pd.DataFrame]: 
    step_name = "Cycle Identification and Basic Stats"
    logger.info(f"Iniciando etapa do pipeline: {step_name}") # [cite: 844]
    df_details_to_return: Optional[pd.DataFrame] = None 

    try:
        # CHAMADA CORRIGIDA: usa 'config' diretamente, que é o config_obj
        cycle_analysis_results = identify_and_process_cycles(all_data_df, config) # [cite: 845]

        if not cycle_analysis_results: # [cite: 845]
            logger.warning(f"Nenhum resultado de ciclo retornado para {step_name}.")
            logger.info(f"==== Etapa: {step_name} CONCLUÍDA (sem dados de ciclo) ====") # [cite: 847]
            return None

        saved_any = False
        # Usando a chave padronizada para obter o DataFrame de detalhes
        df_details = cycle_analysis_results.get(KEY_CYCLE_DETAILS_DF) # CHAVE CORRIGIDA
        if df_details is not None and not df_details.empty: # [cite: 847]
            try:
                # Usando o nome da tabela do config para salvar os detalhes
                db_manager.save_dataframe(df_details, config.ANALYSIS_CYCLES_DETAIL_TABLE_NAME, if_exists='replace') # NOME DE TABELA CORRIGIDO
                logger.info(f"Resultados de detalhes de ciclo salvos na tabela '{config.ANALYSIS_CYCLES_DETAIL_TABLE_NAME}'.") # [cite: 848]
                df_details_to_return = df_details 
                saved_any = True
            except Exception as e_save:
                logger.error(f"Erro ao salvar tabela '{config.ANALYSIS_CYCLES_DETAIL_TABLE_NAME}': {e_save}", exc_info=True) # [cite: 848]
        else:
            logger.info(f"DataFrame para '{KEY_CYCLE_DETAILS_DF}' vazio ou nulo.") # [cite: 849]
            if isinstance(df_details, pd.DataFrame): # [cite: 850]
                df_details_to_return = df_details


        # Usando a chave padronizada para obter o DataFrame de sumário
        df_summary = cycle_analysis_results.get(KEY_CYCLE_SUMMARY_DF) # CHAVE CORRIGIDA
        if df_summary is not None and not df_summary.empty: # [cite: 850]
            try:
                # Usando o nome da tabela do config para salvar o sumário
                db_manager.save_dataframe(df_summary, config.ANALYSIS_CYCLES_SUMMARY_TABLE_NAME, if_exists='replace') # NOME DE TABELA CORRIGIDO
                logger.info(f"Resultados de sumário de ciclo salvos na tabela '{config.ANALYSIS_CYCLES_SUMMARY_TABLE_NAME}'.") # [cite: 851]
                saved_any = True
            except Exception as e_save_sum:
                logger.error(f"Erro ao salvar tabela '{config.ANALYSIS_CYCLES_SUMMARY_TABLE_NAME}': {e_save_sum}", exc_info=True) # [cite: 851]
        else:
            logger.info(f"DataFrame para '{KEY_CYCLE_SUMMARY_DF}' vazio ou nulo.") # [cite: 851]

        if saved_any: # [cite: 851]
            logger.info(f"Etapa {step_name} concluída com dados salvos.") # [cite: 852]
        else:
            logger.info(f"Etapa {step_name} concluída, sem novos dados salvos (ou dados já existentes).")

        return df_details_to_return

    except AttributeError as ae:
        # Este except é para o caso de config.config_obj, que não deve mais ocorrer com a chamada corrigida.
        logger.error(f"Erro de atributo na etapa {step_name}: {ae}. Verifique se 'identify_and_process_cycles' está acessível.", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True) # [cite: 852]
        return None # [cite: 853]