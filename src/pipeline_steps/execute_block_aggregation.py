# src/pipeline_steps/execute_block_aggregation.py
import logging
from src.database_manager import DatabaseManager
# Importar AMBAS as funções de agregação do módulo block_aggregator
from src.analysis.block_aggregator import (
    aggregate_block_data_to_wide_format,
    aggregate_cycle_data_to_wide_format 
)

logger = logging.getLogger(__name__)

def run_block_aggregation_step(db_manager: DatabaseManager, **kwargs) -> bool:
    """
    Executa a etapa de agregação de dados de bloco E de ciclo para o formato largo.

    Args:
        db_manager: Instância do DatabaseManager.
        **kwargs: Argumentos adicionais do pipeline.

    Returns:
        True se ambas as sub-etapas de agregação foram concluídas com sucesso (ou não falharam criticamente),
        False caso contrário.
    """
    block_aggregation_successful = False
    cycle_aggregation_successful = False
    
    try:
        logger.info("Iniciando sub-etapa: Agregação de Dados de Bloco para Formato Largo.")
        aggregate_block_data_to_wide_format(db_manager)
        logger.info("Sub-etapa: Agregação de Dados de Bloco para Formato Largo concluída.")
        block_aggregation_successful = True
    except Exception as e:
        logger.error(f"Erro crítico durante a sub-etapa de agregação de dados de bloco: {e}", exc_info=True)
        block_aggregation_successful = False # Marca como falha
        
    try:
        logger.info("Iniciando sub-etapa: Agregação de Dados de Ciclo para Formato Largo.")
        aggregate_cycle_data_to_wide_format(db_manager)
        logger.info("Sub-etapa: Agregação de Dados de Ciclo para Formato Largo concluída.")
        cycle_aggregation_successful = True
    except Exception as e:
        logger.error(f"Erro crítico durante a sub-etapa de agregação de dados de ciclo: {e}", exc_info=True)
        cycle_aggregation_successful = False # Marca como falha

    final_success = block_aggregation_successful and cycle_aggregation_successful
    if final_success:
        logger.info("Etapa de Agregação (Blocos e Ciclos) concluída com sucesso.")
    else:
        logger.warning("Etapa de Agregação (Blocos e Ciclos) concluída com uma ou mais falhas nas sub-etapas.")
        
    return final_success