# src/pipeline_steps/execute_properties.py
import pandas as pd
import logging
from typing import Any, Dict # Adicionado para type hints
from src.analysis.number_properties_analysis import analyze_number_properties
# from src.database_manager import DatabaseManager # Para type hint
# from src.config import Config # Para type hint

logger = logging.getLogger(__name__)

def run_number_properties_analysis(
    all_data_df: pd.DataFrame, 
    db_manager: Any, # DatabaseManager
    config: Any, # Config
    shared_context: Dict[str, Any], 
    **kwargs
) -> bool:
    step_name = "Number Properties Analysis" # Adicionado para clareza no log
    logger.info(f"Iniciando etapa: {step_name}.")
    try:
        if all_data_df.empty:
            logger.warning(f"DataFrame de dados (all_data_df) está vazio para a etapa {step_name}. Pulando.")
            return True 
            
        # CORRIGIDO: Passa config para a função de análise
        properties_by_contest_df = analyze_number_properties(all_data_df, config) 
        
        if properties_by_contest_df is not None and not properties_by_contest_df.empty:
            # O nome da tabela 'propriedades_numericas_por_concurso' é usado na função de análise
            # mas o save_dataframe usa o nome da tabela que você definir aqui ou no config.
            # Se analyze_number_properties já renomeia a coluna de concurso para "Concurso",
            # e a tabela no DB espera "Concurso", está OK.
            table_name = 'propriedades_numericas_por_concurso'
            db_manager.save_dataframe(properties_by_contest_df, table_name, if_exists='replace')
            logger.info(f"Análise de propriedades numéricas por concurso salva na tabela '{table_name}'.")
        else:
            logger.warning("Não foi possível calcular ou DataFrame de propriedades numéricas vazio.")
            
        logger.info(f"Etapa: {step_name} concluída.")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        return False