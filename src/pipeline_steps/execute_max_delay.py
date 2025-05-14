# src/pipeline_steps/execute_max_delay.py
import pandas as pd
import logging
from typing import Any, Dict # Adicionado
from src.analysis.delay_analysis import calculate_max_delay
# from src.database_manager import DatabaseManager # Para type hint
# from src.config import Config # Para type hint

logger = logging.getLogger(__name__)

def run_max_delay_analysis_step(
    all_data_df: pd.DataFrame, 
    db_manager: Any, # DatabaseManager
    config: Any, # Config
    shared_context: Dict[str, Any], # Adicionado
    **kwargs
) -> bool:
    step_name = "Max Delay Analysis (Separate)"
    logger.info(f"Iniciando etapa: {step_name}.")
    try:
        max_delay_df = calculate_max_delay(all_data_df, config) # Passa config
        if max_delay_df is not None and not max_delay_df.empty:
            db_manager.save_dataframe(max_delay_df, 'atraso_maximo_separado', if_exists='replace')
            logger.info(f"Atraso máximo (etapa separada) salvo na tabela 'atraso_maximo_separado'.")
        else:
            logger.warning(f"Não foi possível calcular ou DataFrame de atraso máximo (etapa separada) vazio.")
            
        logger.info(f"Etapa: {step_name} concluída.")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        return False