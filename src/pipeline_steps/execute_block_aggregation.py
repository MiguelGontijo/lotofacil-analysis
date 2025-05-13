# src/pipeline_steps/execute_block_aggregation.py
import logging
from src.database_manager import DatabaseManager
from src.analysis.block_aggregator import (
    aggregate_block_data_to_wide_format,
    aggregate_cycle_data_to_wide_format # <<< NOVA FUNÇÃO IMPORTADA
)

logger = logging.getLogger(__name__)

def run_block_aggregation_step(db_manager: DatabaseManager, **kwargs) -> bool:
    """
    Executa a etapa de agregação de dados de bloco E de ciclo para o formato largo.
    """
    all_successful = True
    try:
        logger.info("Iniciando etapa: Agregação de Dados de Bloco para Formato Largo.")
        aggregate_block_data_to_wide_format(db_manager)
        logger.info("Sub-etapa: Agregação de Dados de Bloco para Formato Largo concluída.")
    except Exception as e:
        logger.error(f"Erro crítico durante a sub-etapa de agregação de dados de bloco: {e}", exc_info=True)
        all_successful = False
        
    try:
        logger.info("Iniciando sub-etapa: Agregação de Dados de Ciclo para Formato Largo.")
        aggregate_cycle_data_to_wide_format(db_manager) # <<< CHAMADA DA NOVA FUNÇÃO
        logger.info("Sub-etapa: Agregação de Dados de Ciclo para Formato Largo concluída.")
    except Exception as e:
        logger.error(f"Erro crítico durante a sub-etapa de agregação de dados de ciclo: {e}", exc_info=True)
        all_successful = False

    if all_successful:
        logger.info("Etapa de Agregação (Blocos e Ciclos) concluída com sucesso.")
    else:
        logger.warning("Etapa de Agregação (Blocos e Ciclos) concluída com uma ou mais falhas.")
        
    return all_successful