# src/pipeline_steps/execute_repetition_analysis.py
import logging
import pandas as pd
from typing import Any, Dict # Adicionado
from src.analysis.repetition_analysis import calculate_previous_draw_repetitions
# from src.database_manager import DatabaseManager # Para type hint
# from src.config import Config # Para type hint

logger = logging.getLogger(__name__)

def run_repetition_analysis_step(
    all_data_df: pd.DataFrame, 
    db_manager: Any, # DatabaseManager
    config: Any, # Config
    shared_context: Dict[str, Any], # Adicionado
    **kwargs
) -> bool:
    step_name = "Previous Draw Repetition Analysis"
    logger.info(f"Iniciando etapa: {step_name}.")
    try:
        if all_data_df.empty or len(all_data_df) < 2:
            logger.warning(f"DataFrame de dados (all_data_df) insuficiente para {step_name}. Pulando.")
            return True 
            
        # CORRIGIDO: Passa config para a função de análise
        df_repetitions = calculate_previous_draw_repetitions(all_data_df, config) 
        
        if df_repetitions is not None and not df_repetitions.empty:
            table_name = "analise_repeticao_concurso_anterior"
            db_manager.save_dataframe(df_repetitions, table_name, if_exists='replace')
            logger.info(f"Dados de repetição salvos na tabela '{table_name}' ({len(df_repetitions)} registros).")
        else:
            logger.warning("Nenhum dado de repetição foi gerado ou o DataFrame resultante está vazio.")
        
        logger.info(f"Etapa: {step_name} concluída.")
        return True # Retorna True mesmo se nada for salvo, pois a análise rodou.
    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        return False