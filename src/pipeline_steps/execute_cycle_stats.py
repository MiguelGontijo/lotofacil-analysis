# src/pipeline_steps/execute_cycle_stats.py
import pandas as pd
import logging
from typing import Dict, Any
from src.analysis.cycle_analysis import identify_and_process_cycles # Esta função já retorna o sumário
# from src.database_manager import DatabaseManager
# from src.config import Config

logger = logging.getLogger(__name__)

def run_cycle_stats_step( # Nome da função como esperado pelo main.py
    all_data_df: pd.DataFrame, # Padronizado
    db_manager: Any, 
    config: Any, 
    shared_context: Dict[str, Any], # Adicionado
    **kwargs
) -> bool:
    step_name = "Cycle Summary Stats Calculation"
    logger.info(f"Iniciando etapa: {step_name}.")
    try:
        # identify_and_process_cycles já calcula o sumário.
        # O step run_cycle_identification_step também chama identify_and_process_cycles.
        # Para evitar recálculo, poderíamos pegar 'ciclos_detalhe' do shared_context
        # se o step de identificação o salvar lá com uma "output_key".
        # Por ora, vamos recalcular, mas isso é um ponto de otimização.
        
        # Alternativamente, se run_cycle_identification_step já salvou 'ciclos_detalhe'
        # e 'ciclos_sumario_estatisticas', este step poderia ser apenas para garantir
        # ou para outras estatísticas. O código atual do execute_cycle_stats.py
        # que você forneceu também chama identify_and_process_cycles.
        
        cycle_analysis_results = identify_and_process_cycles(all_data_df, config) # Passa config
        
        saved_summary = False
        if cycle_analysis_results:
            df_summary = cycle_analysis_results.get('ciclos_sumario_estatisticas')
            if df_summary is not None and not df_summary.empty:
                db_manager.save_dataframe(df_summary, 'ciclos_sumario_estatisticas', if_exists='replace')
                logger.info(f"Sumário estatístico dos ciclos salvo na tabela 'ciclos_sumario_estatisticas'.")
                saved_summary = True
            else:
                logger.info("Nenhum DataFrame de sumário estatístico de ciclo para salvar.")
                
            # Opcional: salvar 'ciclos_detalhe' também se não foi salvo pelo step de identificação
            df_details = cycle_analysis_results.get('ciclos_detalhe')
            if df_details is not None and not df_details.empty:
                 if not db_manager.table_exists('ciclos_detalhe'): # Salva se não existir
                    db_manager.save_dataframe(df_details, 'ciclos_detalhe', if_exists='replace')
                    logger.info("Detalhes dos ciclos salvos pela etapa de stats (pois não existia).")
        else:
            logger.warning(f"Análise de ciclo não retornou resultados para {step_name}.")

        logger.info(f"Etapa: {step_name} concluída.")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        return False