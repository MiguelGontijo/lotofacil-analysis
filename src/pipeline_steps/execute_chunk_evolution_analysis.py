# Lotofacil_Analysis/src/pipeline_steps/execute_chunk_evolution_analysis.py
import logging
import pandas as pd
from typing import Any, Dict

from src.analysis.chunk_analysis import calculate_chunk_metrics_and_persist
# Para type hints, se desejar ser mais específico com os parâmetros:
# from src.config import Config 
# from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def run_chunk_evolution_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: Any, # DatabaseManager
    config: Any, # CORRIGIDO de config_param para config
    shared_context: Dict[str, Any],
    **kwargs
) -> bool:
    step_name = "Chunk Evolution Metrics Calculation"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")

    try:
        if all_data_df.empty:
            logger.warning(f"DataFrame de dados (all_data_df) está vazio para a etapa {step_name}. Pulando.")
            return True

        # Validação do objeto config injetado
        required_attrs = [
            'CHUNK_TYPES_CONFIG', 'ALL_NUMBERS',
            'EVOL_METRIC_FREQUENCY_BLOCK_PREFIX', # Assegura que os prefixos de tabela estejam no config
            'CONTEST_ID_COLUMN_NAME', 'BALL_NUMBER_COLUMNS',
            'EVOL_BLOCK_GROUP_METRICS_PREFIX'
        ]
        missing_attrs = [attr for attr in required_attrs if not hasattr(config, attr)]
        if missing_attrs:
            logger.error(f"{step_name}: Objeto 'config' injetado não possui os atributos esperados: {missing_attrs}. Abortando.")
            return False

        # Passa o objeto 'config' injetado e validado
        calculate_chunk_metrics_and_persist(
            all_data_df=all_data_df,
            db_manager=db_manager,
            config=config # Usar o config injetado
        )
        logger.info(f"Etapa do pipeline: {step_name} concluída com sucesso.")
        return True
    except AttributeError as ae:
        logger.error(f"Erro de atributo durante a {step_name}. Verifique o objeto de configuração: {ae}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False