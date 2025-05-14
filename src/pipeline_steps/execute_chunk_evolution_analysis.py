# src/pipeline_steps/execute_chunk_evolution_analysis.py
import logging
import pandas as pd
from typing import Any, Dict # Adicionado

# A função em chunk_analysis.py já interage com db_manager,
# e foi corrigida para aceitar 'config'
from src.analysis.chunk_analysis import calculate_chunk_metrics_and_persist
# from src.database_manager import DatabaseManager # Para type hint
# from src.config import Config # Para type hint

logger = logging.getLogger(__name__)

def run_chunk_evolution_analysis_step( 
    all_data_df: pd.DataFrame, # CORRIGIDO de all_draws_df
    db_manager: Any, 
    config: Any, 
    shared_context: Dict[str, Any],
    **kwargs 
) -> bool:
    step_name = "Chunk Evolution Metrics Calculation"
    logger.info(f"Iniciando etapa do pipeline: {step_name}.")
    
    try:
        if all_data_df.empty:
            logger.warning(f"O DataFrame de dados (all_data_df) está vazio para a etapa {step_name}. Pulando.")
            return True 
        
        # calculate_chunk_metrics_and_persist espera (all_data_df, db_manager, config)
        calculate_chunk_metrics_and_persist(
            all_data_df=all_data_df, 
            db_manager=db_manager,
            config=config 
        )
        logger.info(f"Etapa do pipeline: {step_name} concluída com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        return False