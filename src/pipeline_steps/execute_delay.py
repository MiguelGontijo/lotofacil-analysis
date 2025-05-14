# src/pipeline_steps/execute_delay.py
import pandas as pd
import logging
from typing import Any, Dict # Adicionado
from src.analysis.delay_analysis import calculate_current_delay, calculate_max_delay, calculate_mean_delay
# from src.database_manager import DatabaseManager # Para type hint
# from src.config import Config # Para type hint

logger = logging.getLogger(__name__)

def run_delay_analysis(
    all_data_df: pd.DataFrame, 
    db_manager: Any, # DatabaseManager
    config: Any, # Config
    shared_context: Dict[str, Any], # Adicionado
    **kwargs
) -> bool:
    step_name = "Delay Analysis"
    logger.info(f"Iniciando etapa: {step_name}.")
    try:
        current_delay_df = calculate_current_delay(all_data_df, config) # Passa config
        if current_delay_df is not None and not current_delay_df.empty:
            db_manager.save_dataframe(current_delay_df, 'atraso_atual', if_exists='replace')
            logger.info("Atraso atual salvo no banco de dados.")
        else:
            logger.warning("Não foi possível calcular ou DataFrame de atraso atual vazio.")

        max_delay_df = calculate_max_delay(all_data_df, config) # Passa config
        if max_delay_df is not None and not max_delay_df.empty:
            db_manager.save_dataframe(max_delay_df, 'atraso_maximo', if_exists='replace')
            logger.info("Atraso máximo salvo no banco de dados.")
        else:
            logger.warning("Não foi possível calcular ou DataFrame de atraso máximo vazio.")

        mean_delay_df = calculate_mean_delay(all_data_df, config) # Passa config
        if mean_delay_df is not None and not mean_delay_df.empty:
            db_manager.save_dataframe(mean_delay_df, 'atraso_medio', if_exists='replace')
            logger.info("Atraso médio salvo no banco de dados.")
        else:
            logger.warning("Não foi possível calcular ou DataFrame de atraso médio vazio.")

        logger.info(f"Etapa: {step_name} concluída.")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        return False