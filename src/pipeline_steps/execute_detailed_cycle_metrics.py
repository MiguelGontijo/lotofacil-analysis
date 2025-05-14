# src/pipeline_steps/execute_detailed_cycle_metrics.py
import logging
import pandas as pd
from typing import Any, Dict, Optional

from src.analysis.cycle_analysis import identify_and_process_cycles, calculate_detailed_metrics_per_closed_cycle

logger = logging.getLogger(__name__)

def run_detailed_cycle_metrics_step(
    all_data_df: pd.DataFrame, 
    db_manager: Any, 
    config: Any, 
    shared_context: Dict[str, Any],
    **kwargs
) -> bool:
    step_name = "Detailed Cycle Metrics Calculation"
    logger.info(f"Iniciando etapa do pipeline: {step_name}")

    try:
        # Tenta carregar 'ciclos_detalhe' do banco de dados
        df_ciclos_detalhe = db_manager.load_dataframe("ciclos_detalhe") 

        if df_ciclos_detalhe is None or df_ciclos_detalhe.empty:
            logger.warning(f"DataFrame 'ciclos_detalhe' não encontrado no banco ou vazio na etapa {step_name}. Tentando identificar ciclos agora.")
            # Fallback: Se não encontrar no DB, recalcula.
            cycle_identification_results = identify_and_process_cycles(all_data_df, config) # Passa config
            df_ciclos_detalhe = cycle_identification_results.get('ciclos_detalhe')
            
            if df_ciclos_detalhe is None or df_ciclos_detalhe.empty:
                logger.error(f"Não foi possível obter 'ciclos_detalhe' nem do DB nem por novo cálculo na etapa {step_name}. Abortando step.")
                return False

        # Passa config para calculate_detailed_metrics_per_closed_cycle
        dict_metric_dfs = calculate_detailed_metrics_per_closed_cycle(all_data_df, df_ciclos_detalhe, config)

        if not dict_metric_dfs:
            logger.warning(f"Nenhuma métrica detalhada de ciclo retornada pela análise para a etapa {step_name}.")
            return True 

        saved_any = False
        for table_key, df_to_save in dict_metric_dfs.items():
            if df_to_save is not None and not df_to_save.empty:
                try:
                    db_manager.save_dataframe(df_to_save, table_key, if_exists='replace')
                    logger.info(f"Métricas detalhadas de ciclo '{table_key}' salvas na tabela '{table_key}'.")
                    saved_any = True
                except Exception as e_save:
                    logger.error(f"Erro ao salvar a tabela de métricas detalhadas de ciclo '{table_key}': {e_save}", exc_info=True)
            else:
                logger.info(f"DataFrame para métricas detalhadas de ciclo '{table_key}' está vazio ou nulo.")
        
        if saved_any: 
            logger.info(f"Etapa {step_name} concluída com dados salvos.")
        else: 
            logger.info(f"Etapa {step_name} concluída, mas nenhum novo dado foi salvo.")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao executar a etapa {step_name}: {e}", exc_info=True)
        return False