# src/pipeline_steps/execute_block_aggregation.py
import logging
from typing import Any, Dict
import pandas as pd # Adicionado para o caso de all_data_df ser usado no futuro
# from src.database_manager import DatabaseManager # Para type hint
# from src.config import Config # Para type hint
from src.analysis.block_aggregator import (
    aggregate_block_data_to_wide_format,
    aggregate_cycle_data_to_wide_format 
)

logger = logging.getLogger(__name__)

def run_block_aggregation_step(
    db_manager: Any, 
    config: Any, 
    shared_context: Dict[str, Any], # Adicionado para consistência
    # all_data_df não é usado diretamente aqui, mas poderia ser passado por consistência
    # Se não for usado, pode ser removido da lista 'args' no main.py para este step
    **kwargs
) -> bool:
    block_aggregation_successful = False
    cycle_aggregation_successful = False
    step_name = "Block and Cycle Data Aggregation"
    logger.info(f"Iniciando etapa: {step_name}")
    
    try:
        logger.info("Sub-etapa: Agregação de Dados de Bloco para Formato Largo.")
        aggregate_block_data_to_wide_format(db_manager, config) # Passa config
        logger.info("Sub-etapa: Agregação de Dados de Bloco para Formato Largo concluída.")
        block_aggregation_successful = True
    except Exception as e:
        logger.error(f"Erro crítico durante a sub-etapa de agregação de dados de bloco: {e}", exc_info=True)
        block_aggregation_successful = False
        
    try:
        logger.info("Sub-etapa: Agregação de Dados de Ciclo para Formato Largo.")
        aggregate_cycle_data_to_wide_format(db_manager, config) # Passa config
        logger.info("Sub-etapa: Agregação de Dados de Ciclo para Formato Largo concluída.")
        cycle_aggregation_successful = True
    except Exception as e:
        logger.error(f"Erro crítico durante a sub-etapa de agregação de dados de ciclo: {e}", exc_info=True)
        cycle_aggregation_successful = False

    final_success = block_aggregation_successful and cycle_aggregation_successful
    if final_success:
        logger.info(f"Etapa {step_name} concluída com sucesso.")
    else:
        logger.warning(f"Etapa {step_name} concluída com uma ou mais falhas nas sub-etapas.")
        
    return final_success