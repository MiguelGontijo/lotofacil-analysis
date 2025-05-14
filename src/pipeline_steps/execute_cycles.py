# src/pipeline_steps/execute_cycles.py
import pandas as pd
import logging
from typing import Dict, Any

from src.analysis.cycle_analysis import identify_and_process_cycles
# from src.database_manager import DatabaseManager # Para type hint
# from src.config import Config # Para type hint

logger = logging.getLogger(__name__)

def run_cycle_identification_step( # Nome correto da função
    all_data_df: pd.DataFrame,
    db_manager: Any, # DatabaseManager
    config: Any, # Config
    shared_context: Dict[str, Any],
    **kwargs
) -> bool:
    step_name = "Cycle Identification and Basic Stats"
    logger.info(f"Iniciando etapa do pipeline: {step_name}")

    try:
        # Passa config para a função de análise
        cycle_analysis_results = identify_and_process_cycles(all_data_df, config) 

        if not cycle_analysis_results:
            logger.warning(f"Nenhum resultado de ciclo retornado para {step_name}.")
            return True 

        saved_any = False
        # Salva 'ciclos_detalhe'
        df_details = cycle_analysis_results.get('ciclos_detalhe')
        if df_details is not None and not df_details.empty:
            try:
                db_manager.save_dataframe(df_details, 'ciclos_detalhe', if_exists='replace')
                logger.info(f"Resultados de 'ciclos_detalhe' salvos na tabela 'ciclos_detalhe'.")
                saved_any = True
            except Exception as e_save:
                logger.error(f"Erro ao salvar tabela 'ciclos_detalhe': {e_save}", exc_info=True)
        else:
            logger.info(f"DataFrame para 'ciclos_detalhe' vazio ou nulo.")
        
        # Salva 'ciclos_sumario_estatisticas' se o step "cycle_stats" não for fazer isso
        # Se "cycle_stats" também chama identify_and_process_cycles, pode haver duplicação de cálculo.
        # Idealmente, identify_and_process_cycles é chamado uma vez, e seus resultados são passados
        # ou lidos do DB pelos steps subsequentes.
        # Por ora, se este step é "identification and basic stats", ele pode salvar ambos.
        df_summary = cycle_analysis_results.get('ciclos_sumario_estatisticas')
        if df_summary is not None and not df_summary.empty:
            try:
                db_manager.save_dataframe(df_summary, 'ciclos_sumario_estatisticas', if_exists='replace')
                logger.info(f"Resultados de 'ciclos_sumario_estatisticas' salvos na tabela 'ciclos_sumario_estatisticas'.")
                saved_any = True # Atualiza saved_any
            except Exception as e_save_sum:
                logger.error(f"Erro ao salvar tabela 'ciclos_sumario_estatisticas': {e_save_sum}", exc_info=True)
        else:
            logger.info(f"DataFrame para 'ciclos_sumario_estatisticas' vazio ou nulo.")
            
        if saved_any: logger.info(f"Etapa {step_name} concluída com dados salvos.")
        else: logger.info(f"Etapa {step_name} concluída, sem novos dados salvos.")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        return False