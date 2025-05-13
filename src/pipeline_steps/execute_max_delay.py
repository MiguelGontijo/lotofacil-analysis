# src/pipeline_steps/execute_max_delay.py
import pandas as pd
import logging # Adicionado
from src.analysis.delay_analysis import calculate_max_delay # Supondo que esta função existe em delay_analysis.py
from src.database_manager import DatabaseManager
# from src.config import logger # Removido

logger = logging.getLogger(__name__) # Corrigido

def run_max_delay_analysis_step(all_data_df: pd.DataFrame, db_manager: DatabaseManager, **kwargs) -> bool:
    """
    Executa a análise de atraso máximo e salva o resultado.
    Nomeado _step para evitar conflito se houver outra função com nome similar.
    """
    try:
        logger.info("Iniciando análise de atraso máximo (etapa separada).")
        
        max_delay_df = calculate_max_delay(all_data_df)
        if max_delay_df is not None and not max_delay_df.empty:
            db_manager.save_dataframe_to_db(max_delay_df, 'atraso_maximo_separado') # Nome de tabela diferente para evitar conflito se calculado em run_delay_analysis
            logger.info("Atraso máximo (etapa separada) salvo no banco de dados.")
        else:
            logger.warning("Não foi possível calcular ou DataFrame de atraso máximo (etapa separada) vazio.")
            
        logger.info("Análise de atraso máximo (etapa separada) concluída.")
        return True
    except Exception as e:
        logger.error(f"Erro na análise de atraso máximo (etapa separada): {e}", exc_info=True)
        return False