# src/pipeline_steps/execute_rank_trend_analysis.py
import logging
import pandas as pd # Adicionado para o type hint Optional[pd.DataFrame]
from typing import Any, Dict, Optional # ADICIONADO Optional (e Any, Dict para consistência)

# from src.database_manager import DatabaseManager # Para type hint, se usado
# from src.config import Config # Para type hint, se usado
from src.analysis.rank_trend_analysis import (
    calculate_and_persist_rank_per_chunk,
    calculate_and_persist_general_rank
)

logger = logging.getLogger(__name__)

def run_rank_trend_analysis_step(
    db_manager: Any, # DatabaseManager
    config: Any, # Config
    shared_context: Dict[str, Any],
    all_data_df: Optional[pd.DataFrame] = None, # Agora Optional é conhecido
    **kwargs
) -> bool:
    all_successful = True # Assume sucesso até que uma falha ocorra
    step_name = "Rank Trend Analysis"
    logger.info(f"Iniciando etapa: {step_name}.")
    
    try:
        logger.info("Sub-etapa: Calculando rank por chunk.")
        # A função calculate_and_persist_rank_per_chunk espera (db_manager, config)
        calculate_and_persist_rank_per_chunk(db_manager, config) 
        logger.info("Sub-etapa: Cálculo de rank por chunk concluída.")

        logger.info("Sub-etapa: Calculando rank geral.")
        # A função calculate_and_persist_general_rank espera (db_manager, config)
        calculate_and_persist_general_rank(db_manager, config)
        logger.info("Sub-etapa: Cálculo de rank geral concluída.")
        
        logger.info(f"Etapa: {step_name} concluída com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro crítico durante a etapa {step_name}: {e}", exc_info=True)
        all_successful = False # Marca como falha se uma exceção não tratada ocorrer aqui
        
    return all_successful