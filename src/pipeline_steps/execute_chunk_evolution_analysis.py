# src/pipeline_steps/execute_chunk_evolution_analysis.py
import logging
import pandas as pd
from src.analysis.chunk_analysis import calculate_chunk_metrics_and_persist
from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def run_chunk_evolution_analysis(
    all_data_df: pd.DataFrame,
    db_manager: DatabaseManager,
    **kwargs # Aceita kwargs extras para compatibilidade com o orquestrador
) -> bool:
    """
    Executa o cálculo e persistência das métricas de evolução de chunks.

    Args:
        all_data_df: DataFrame com todos os dados dos concursos.
        db_manager: Instância do DatabaseManager.
        **kwargs: Argumentos adicionais do pipeline (não usados aqui).


    Returns:
        True se a análise foi concluída com sucesso, False caso contrário.
    """
    try:
        logger.info("Iniciando etapa: Cálculo de Métricas de Evolução de Chunks.")
        if all_data_df.empty:
            logger.warning("O DataFrame de dados está vazio. Pulando análise de evolução de chunks.")
            return False
        
        # A função calculate_chunk_metrics_and_persist já lida com logging interno
        calculate_chunk_metrics_and_persist(all_data_df=all_data_df, db_manager=db_manager)
        logger.info("Etapa: Cálculo de Métricas de Evolução de Chunks concluída com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa de cálculo de métricas de evolução de chunks: {e}", exc_info=True)
        return False