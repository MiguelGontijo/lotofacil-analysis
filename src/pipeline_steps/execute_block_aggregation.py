# Lotofacil_Analysis/src/pipeline_steps/execute_block_aggregation.py
import logging
from typing import Any, Dict

from src.analysis.block_aggregator import (
    aggregate_block_data_to_wide_format,
    aggregate_cycle_data_to_wide_format
)
# Para type hints, se desejar ser mais específico com os parâmetros:
# from src.config import Config 
# from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def run_block_aggregation_step(
    db_manager: Any, # DatabaseManager
    config: Any, # CORRIGIDO de config_param para config
    shared_context: Dict[str, Any],
    **kwargs
) -> bool:
    block_aggregation_successful = True
    cycle_aggregation_successful = True
    step_name = "Block and Cycle Data Aggregation"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")

    # Validação do objeto config injetado
    required_attrs = [
        'CHUNK_TYPES_CONFIG', 'BLOCK_ANALISES_CONSOLIDADAS_PREFIX',
        'CYCLE_ANALISES_CONSOLIDADAS_TABLE_NAME',
        'EVOL_METRIC_FREQUENCY_BLOCK_PREFIX', # Usado por aggregate_block_data_to_wide_format
        'EVOL_RANK_FREQUENCY_BLOCK_PREFIX',   # Exemplo de outro prefixo
        'EVOL_BLOCK_GROUP_METRICS_PREFIX',    # Usado por aggregate_block_data_to_wide_format
        'ANALYSIS_CYCLES_DETAIL_TABLE_NAME',    # Usado por aggregate_cycle_data_to_wide_format
        'CYCLE_METRIC_FREQUENCY_TABLE_NAME',    # Usado por aggregate_cycle_data_to_wide_format
        'CYCLE_GROUP_METRICS_TABLE_NAME',       # Usado por aggregate_cycle_data_to_wide_format
        'ALL_NUMBERS'
    ]
    # Adicionar todas as constantes de prefixo de tabela usadas em per_dezena_metric_configs
    # em block_aggregator.py à lista required_attrs aqui também, para uma verificação completa.
    # Ex: 'EVOL_METRIC_ATRASO_MEDIO_BLOCK_PREFIX', etc.
    # Por simplicidade, esta lista está um pouco mais curta, mas idealmente seria exaustiva.

    missing_attrs = [attr for attr in required_attrs if not hasattr(config, attr)]
    if missing_attrs:
        logger.error(f"{step_name}: Objeto 'config' injetado não possui os atributos esperados: {missing_attrs}. Abortando.")
        return False

    try:
        logger.info("Sub-etapa: Agregação de Dados de Bloco para Formato Largo.")
        aggregate_block_data_to_wide_format(db_manager, config) # Passa o config injetado
        logger.info("Sub-etapa: Agregação de Dados de Bloco para Formato Largo concluída.")
    except Exception as e:
        logger.error(f"Erro crítico durante a sub-etapa de agregação de dados de bloco: {e}", exc_info=True)
        block_aggregation_successful = False

    try:
        logger.info("Sub-etapa: Agregação de Dados de Ciclo para Formato Largo.")
        aggregate_cycle_data_to_wide_format(db_manager, config) # Passa o config injetado
        logger.info("Sub-etapa: Agregação de Dados de Ciclo para Formato Largo concluída.")
    except Exception as e:
        logger.error(f"Erro crítico durante a sub-etapa de agregação de dados de ciclo: {e}", exc_info=True)
        cycle_aggregation_successful = False

    final_success = block_aggregation_successful and cycle_aggregation_successful
    if final_success:
        logger.info(f"Etapa {step_name} concluída com sucesso.")
    else:
        logger.warning(f"Etapa {step_name} concluída com uma ou mais falhas (bloco: {block_aggregation_successful}, ciclo: {cycle_aggregation_successful}).")

    return final_success