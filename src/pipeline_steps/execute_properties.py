# src/pipeline_steps/execute_properties.py
import pandas as pd
import logging # Adicionado
from src.analysis.number_properties_analysis import analyze_number_properties # Supondo esta função principal
from src.database_manager import DatabaseManager
# from src.config import logger # Removido

logger = logging.getLogger(__name__) # Corrigido

def run_number_properties_analysis(all_data_df: pd.DataFrame, db_manager: DatabaseManager, **kwargs) -> bool:
    """
    Executa a análise de propriedades numéricas (pares, ímpares, primos, etc.) por concurso.
    """
    try:
        logger.info("Iniciando análise de propriedades numéricas.")
        
        properties_by_contest_df = analyze_number_properties(all_data_df) # Esta função deve retornar um DataFrame
        
        if properties_by_contest_df is not None and not properties_by_contest_df.empty:
            db_manager.save_dataframe_to_db(properties_by_contest_df, 'propriedades_numericas_por_concurso')
            logger.info("Análise de propriedades numéricas por concurso salva no banco de dados.")
        else:
            logger.warning("Não foi possível calcular ou DataFrame de propriedades numéricas vazio.")
            
        logger.info("Análise de propriedades numéricas concluída.")
        return True
    except Exception as e:
        logger.error(f"Erro na análise de propriedades numéricas: {e}", exc_info=True)
        return False