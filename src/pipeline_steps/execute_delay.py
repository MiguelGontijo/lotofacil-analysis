# src/pipeline_steps/execute_delay.py
import pandas as pd
import logging # Adicionado
from src.analysis.delay_analysis import calculate_current_delay, calculate_max_delay, calculate_mean_delay # Supondo estas funções existem
from src.database_manager import DatabaseManager
# from src.config import logger # Removido

logger = logging.getLogger(__name__) # Corrigido

def run_delay_analysis(all_data_df: pd.DataFrame, db_manager: DatabaseManager, **kwargs) -> bool:
    """
    Executa a análise de atraso (delay) e salva os resultados.
    """
    try:
        logger.info("Iniciando análise de atraso (delay).")

        # Atraso Atual
        current_delay_df = calculate_current_delay(all_data_df)
        if current_delay_df is not None and not current_delay_df.empty:
            db_manager.save_dataframe_to_db(current_delay_df, 'atraso_atual')
            logger.info("Atraso atual salvo no banco de dados.")
        else:
            logger.warning("Não foi possível calcular ou DataFrame de atraso atual vazio.")

        # Atraso Máximo
        # Nota: Você tem um execute_max_delay.py separado. Decida se calcula aqui ou lá.
        # Se delay_analysis.py já calcula tudo, pode ser centralizado.
        # Por ora, vou assumir que delay_analysis.py tem calculate_max_delay.
        max_delay_df = calculate_max_delay(all_data_df)
        if max_delay_df is not None and not max_delay_df.empty:
            db_manager.save_dataframe_to_db(max_delay_df, 'atraso_maximo')
            logger.info("Atraso máximo salvo no banco de dados.")
        else:
            logger.warning("Não foi possível calcular ou DataFrame de atraso máximo vazio.")

        # Atraso Médio
        mean_delay_df = calculate_mean_delay(all_data_df)
        if mean_delay_df is not None and not mean_delay_df.empty:
            db_manager.save_dataframe_to_db(mean_delay_df, 'atraso_medio')
            logger.info("Atraso médio salvo no banco de dados.")
        else:
            logger.warning("Não foi possível calcular ou DataFrame de atraso médio vazio.")

        logger.info("Análise de atraso (delay) concluída.")
        return True
    except Exception as e:
        logger.error(f"Erro na análise de atraso (delay): {e}", exc_info=True)
        return False