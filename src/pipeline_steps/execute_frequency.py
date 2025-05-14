# src/pipeline_steps/execute_frequency.py
import pandas as pd
import logging
from typing import Any, Dict 
from src.analysis.frequency_analysis import calculate_frequency, calculate_relative_frequency
# Para type hints mais fortes, se desejar:
# from src.database_manager import DatabaseManager 
# from src.config import Config # Este seria o tipo de 'config'

logger = logging.getLogger(__name__)

def run_frequency_analysis(
    all_data_df: pd.DataFrame, 
    db_manager: Any, # Espera-se DatabaseManager
    config: Any, # Espera-se Config
    shared_context: Dict[str, Any], 
    **kwargs 
) -> bool:
    step_name = "Frequency Analysis" 
    logger.info(f"Iniciando etapa: {step_name}.")
    try:
        # Passar 'config' para as funções de análise, pois elas agora esperam
        absolute_freq_df = calculate_frequency(all_data_df, config) 
        
        if absolute_freq_df is not None and not absolute_freq_df.empty:
            db_manager.save_dataframe(absolute_freq_df, 'frequencia_absoluta', if_exists='replace')
            logger.info("Frequência absoluta salva no banco de dados.")

            total_contests = len(all_data_df)
            # Passar config para calculate_relative_frequency
            relative_freq_df = calculate_relative_frequency(absolute_freq_df, total_contests, config)
            if relative_freq_df is not None and not relative_freq_df.empty:
                db_manager.save_dataframe(relative_freq_df, 'frequencia_relativa', if_exists='replace')
                logger.info("Frequência relativa salva no banco de dados.")
            else:
                logger.warning("DataFrame de frequência relativa vazio ou não pôde ser calculado.")
        else:
            logger.warning("DataFrame de frequência absoluta vazio ou não pôde ser calculado.")
        
        logger.info(f"Etapa: {step_name} concluída.")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        return False