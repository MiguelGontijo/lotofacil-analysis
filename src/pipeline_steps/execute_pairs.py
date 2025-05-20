# Lotofacil_Analysis/src/pipeline_steps/execute_pairs.py
import pandas as pd
import logging
from typing import Any, Dict

# Para type hints mais específicos:
from src.analysis.combination_analysis import CombinationAnalyzer
# from src.database_manager import DatabaseManager
# from src.config import Config

logger = logging.getLogger(__name__)

def run_pair_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: Any, # DatabaseManager
    config: Any, # Config
    shared_context: Dict[str, Any],
    combination_analyzer_instance: CombinationAnalyzer, # CORRIGIDO
    **kwargs
) -> bool:
    step_name = "Pair Analysis"
    logger.info(f"Iniciando etapa do pipeline: {step_name}")

    try:
        required_attrs = [
            'DRAWN_NUMBERS_COLUMN_NAME', 'CONTEST_ID_COLUMN_NAME',
            'ANALYSIS_PAIR_METRICS_TABLE_NAME'
        ]
        for attr in required_attrs:
            if not hasattr(config, attr):
                logger.error(f"{step_name}: Atributo de config '{attr}' não encontrado.")
                return False

        if combination_analyzer_instance is None:
            logger.error(f"{step_name}: Instância 'combination_analyzer_instance' é None.")
            return False # Falha crítica se o analyzer não estiver disponível
        
        if not hasattr(combination_analyzer_instance, 'analyze_pairs'):
             logger.error(f"{step_name}: 'combination_analyzer_instance' não possui método 'analyze_pairs'.")
             return False

        drawn_numbers_col = config.DRAWN_NUMBERS_COLUMN_NAME
        contest_id_col = config.CONTEST_ID_COLUMN_NAME
        pair_metrics_table_name = config.ANALYSIS_PAIR_METRICS_TABLE_NAME


        pairs_df = combination_analyzer_instance.analyze_pairs(
            all_draws_df=all_data_df,
            drawn_numbers_col=drawn_numbers_col,
            contest_id_col=contest_id_col
        )

        expected_cols = ['pair_str', 'frequency', 'last_contest', 'current_delay']
        if pairs_df is not None and not pairs_df.empty:
            cols_to_save = [col for col in expected_cols if col in pairs_df.columns]
            # Adiciona colunas faltantes com NA se não vierem da análise mas são esperadas na tabela
            for эко_col in expected_cols:
                if эко_col not in pairs_df.columns:
                    pairs_df[эко_col] = pd.NA # ou np.nan se float
            
            db_manager.save_dataframe(pairs_df[cols_to_save], pair_metrics_table_name, if_exists='replace')
            logger.info(f"Métricas de pares salvas na tabela '{pair_metrics_table_name}'.")
        else:
            logger.warning(f"{step_name}: Não foi possível calcular ou DataFrame de métricas de pares vazio.")
            db_manager.save_dataframe(pd.DataFrame(columns=expected_cols), pair_metrics_table_name, if_exists='replace')


        logger.info(f"Etapa do pipeline: {step_name} concluída.")
        return True
    except AttributeError as e:
        logger.error(f"Erro de atributo na etapa {step_name}: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Erro ao executar a etapa {step_name}: {e}", exc_info=True)
        return False