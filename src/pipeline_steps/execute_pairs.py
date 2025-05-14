# Lotofacil_Analysis/src/pipeline_steps/execute_pairs.py
import pandas as pd
import logging
from typing import Any, Dict

# Para type hints mais específicos (opcional):
# from src.analysis.combination_analysis import CombinationAnalyzer
# from src.database_manager import DatabaseManager
# from src.config import Config

logger = logging.getLogger(__name__)

def run_pair_analysis_step(
    all_data_df: pd.DataFrame, # CORRIGIDO para all_data_df
    db_manager: Any, 
    combination_analyzer: Any, 
    config: Any, 
    shared_context: Dict[str, Any],
    **kwargs
) -> bool:
    step_name = "Pair Analysis"
    logger.info(f"Iniciando etapa do pipeline: {step_name}")

    try:
        drawn_numbers_col = config.DRAWN_NUMBERS_COLUMN_NAME
        contest_id_col = config.CONTEST_ID_COLUMN_NAME
        
        # O método analyze_pairs do CombinationAnalyzer espera 'all_draws_df'.
        # Passamos nosso all_data_df para esse parâmetro.
        pairs_df = combination_analyzer.analyze_pairs(
            all_draws_df=all_data_df, 
            drawn_numbers_col=drawn_numbers_col,
            contest_id_col=contest_id_col
        )
        
        if pairs_df is not None and not pairs_df.empty:
            table_name = "pair_metrics" 
            db_manager.save_dataframe(pairs_df, table_name, if_exists='replace')
            logger.info(f"Métricas de pares salvas na tabela '{table_name}'.")
        else:
            logger.warning("Não foi possível calcular ou DataFrame de métricas de pares vazio.")
            
        logger.info(f"Etapa do pipeline: {step_name} concluída.")
        return True
    except AttributeError as e:
        logger.error(f"Erro na etapa {step_name} (verifique config ou combination_analyzer): {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Erro ao executar a etapa {step_name}: {e}", exc_info=True)
        return False