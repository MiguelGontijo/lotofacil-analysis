# src/pipeline_steps/execute_pairs.py
import pandas as pd
import logging # Adicionado
from src.analysis.combination_analysis import calculate_pair_frequencies # Supondo esta função para pares
from src.database_manager import DatabaseManager
# from src.config import logger # Removido

logger = logging.getLogger(__name__) # Corrigido

def run_pair_combination_analysis(all_data_df: pd.DataFrame, db_manager: DatabaseManager, **kwargs) -> bool:
    """
    Executa a análise de combinações de pares.
    """
    try:
        logger.info("Iniciando análise de combinações de pares.")
        
        pair_freq_df = calculate_pair_frequencies(all_data_df) # Esta função deve vir de combination_analysis.py
        if pair_freq_df is not None and not pair_freq_df.empty:
            db_manager.save_dataframe_to_db(pair_freq_df, 'frequencia_pares')
            logger.info("Frequência de pares salva no banco de dados.")
        else:
            logger.warning("Não foi possível calcular ou DataFrame de frequência de pares vazio.")
            
        logger.info("Análise de combinações de pares concluída.")
        return True
    except Exception as e:
        logger.error(f"Erro na análise de combinações de pares: {e}", exc_info=True)
        return False