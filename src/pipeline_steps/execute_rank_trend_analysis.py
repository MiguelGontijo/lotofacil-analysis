# src/pipeline_steps/execute_rank_trend_analysis.py
import logging
from src.database_manager import DatabaseManager
# Importar as funções do módulo de análise de rank
from src.analysis.rank_trend_analysis import (
    calculate_and_persist_rank_per_chunk,
    calculate_and_persist_general_rank
)

logger = logging.getLogger(__name__)

def run_rank_trend_analysis_step(db_manager: DatabaseManager, **kwargs) -> bool:
    """
    Executa a análise de tendências de ranking, incluindo rank por chunk e rank geral.

    Args:
        db_manager: Instância do DatabaseManager.
        **kwargs: Argumentos adicionais do pipeline (não usados diretamente aqui,
                  mas incluídos para compatibilidade com o orquestrador).

    Returns:
        True se todas as sub-etapas da análise de rank foram concluídas com sucesso (ou não falharam criticamente),
        False caso contrário.
    """
    all_successful = True
    try:
        logger.info("Iniciando etapa: Análise de Tendências de Ranking.")
        
        # Calcular e persistir rank por chunk
        logger.info("Sub-etapa: Calculando rank por chunk.")
        calculate_and_persist_rank_per_chunk(db_manager)
        logger.info("Sub-etapa: Cálculo de rank por chunk concluída.")

        # Calcular e persistir rank geral
        logger.info("Sub-etapa: Calculando rank geral.")
        calculate_and_persist_general_rank(db_manager)
        logger.info("Sub-etapa: Cálculo de rank geral concluída.")
        
        logger.info("Etapa: Análise de Tendências de Ranking concluída com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro crítico durante a etapa de análise de tendências de ranking: {e}", exc_info=True)
        all_successful = False # Marca como falha se uma exceção não tratada ocorrer aqui
        
    return all_successful