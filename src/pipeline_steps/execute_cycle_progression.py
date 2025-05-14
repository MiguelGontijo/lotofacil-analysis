# src/pipeline_steps/execute_cycle_progression.py
import logging
import pandas as pd
from typing import Any, Dict # Adicionado
from src.analysis.cycle_progression_analysis import calculate_cycle_progression
# from src.database_manager import DatabaseManager # Para type hint
# from src.config import Config # Para type hint

logger = logging.getLogger(__name__)

def run_cycle_progression_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: Any, # DatabaseManager
    config: Any, # Config
    shared_context: Dict[str, Any], # Adicionado
    **kwargs
) -> bool:
    step_name = "Cycle Progression Analysis"
    logger.info(f"Iniciando etapa: {step_name}.")
    try:
        if all_data_df.empty:
            logger.warning(f"DataFrame de dados (all_data_df) está vazio para a etapa {step_name}. Pulando.")
            return True
            
        df_progression = calculate_cycle_progression(all_data_df, config) # Passa config
        
        if df_progression is not None and not df_progression.empty:
            table_name = "ciclo_progression"
            db_manager.save_dataframe(df_progression, table_name, if_exists='replace')
            logger.info(f"Dados de progressão de ciclo salvos na tabela '{table_name}' ({len(df_progression)} registros).")
        else:
            logger.warning("Nenhum dado de progressão de ciclo foi gerado ou o DataFrame resultante está vazio.")
        
        logger.info(f"Etapa: {step_name} concluída.")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        return False