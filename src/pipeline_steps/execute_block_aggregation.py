# src/pipeline_steps/execute_block_aggregation.py
import logging
from src.database_manager import DatabaseManager
from src.analysis.block_aggregator import aggregate_block_data_to_wide_format

logger = logging.getLogger(__name__)

def run_block_aggregation_step(db_manager: DatabaseManager, **kwargs) -> bool:
    """
    Executa a etapa de agregação de dados de bloco para o formato largo.

    Args:
        db_manager: Instância do DatabaseManager.
        **kwargs: Argumentos adicionais do pipeline (não usados aqui).

    Returns:
        True se a agregação foi concluída com sucesso (ou não falhou criticamente),
        False caso contrário.
    """
    try:
        logger.info("Iniciando etapa: Agregação de Dados de Bloco para Formato Largo.")
        
        aggregate_block_data_to_wide_format(db_manager)
        
        logger.info("Etapa: Agregação de Dados de Bloco para Formato Largo concluída com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro crítico durante a etapa de agregação de dados de bloco: {e}", exc_info=True)
        return False