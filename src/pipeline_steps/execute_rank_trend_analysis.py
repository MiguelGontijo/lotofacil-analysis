# Lotofacil_Analysis/src/pipeline_steps/execute_rank_trend_analysis.py
import logging
import pandas as pd
from typing import Any, Dict, Optional
import numpy as np

# Para type hints mais específicos:
from src.config import Config
# from src.database_manager import DatabaseManager

from src.analysis.rank_trend_analysis import (
    calculate_and_persist_rank_per_chunk,
    calculate_historical_rank_trends
)

logger = logging.getLogger(__name__)

def run_rank_trend_analysis_step(
    db_manager: Any, # DatabaseManager
    config: Config,  # CORRIGIDO de config_param para config
    shared_context: Dict[str, Any],
    all_data_df: pd.DataFrame, # Assinatura alinhada com main.py
    **kwargs
) -> bool:
    step_name = "Rank Trend Metrics Analysis (Slope/Status)"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")

    # CORREÇÃO: Usar o nome da constante como está no config.py
    block_agg_table_name_const = 'BLOCK_AGGREGATED_DATA_FOR_RANK_TREND_TABLE_NAME'

    required_attrs_for_rank_trend = [
        block_agg_table_name_const, # Nome da constante corrigido
        'RANK_ANALYSIS_TYPE_FILTER_FOR_TREND', 'RANK_VALUE_COLUMN_FOR_TREND',
        'RANK_TREND_WINDOW_BLOCKS', 'RANK_TREND_SLOPE_IMPROVING_THRESHOLD',
        'RANK_TREND_SLOPE_WORSENING_THRESHOLD', 'ANALYSIS_RANK_TREND_METRICS_TABLE_NAME',
        'ALL_NUMBERS', 'CONTEST_ID_COLUMN_NAME', 'CHUNK_TYPES_CONFIG',
        'EVOL_RANK_FREQUENCY_BLOCK_PREFIX' # Usado por calculate_and_persist_rank_per_chunk
    ]
    missing_attrs = [attr for attr in required_attrs_for_rank_trend if not hasattr(config, attr)]
    if missing_attrs:
        logger.error(f"{step_name}: Objeto 'config' não possui atributos: {missing_attrs}. Abortando.")
        return False

    try:
        logger.info("Sub-etapa: Gerando/Atualizando ranks de frequência por chunk.")
        calculate_and_persist_rank_per_chunk(db_manager, config) # Usar config injetado
        logger.info("Sub-etapa: Geração de ranks de frequência por chunk concluída.")

        logger.info("Sub-etapa: Calculando slope e status da tendência de rank.")
        aggregated_block_table_name = getattr(config, block_agg_table_name_const)
        
        rank_type_filter = config.RANK_ANALYSIS_TYPE_FILTER_FOR_TREND
        # A rank_value_column_name agora é usada para filtrar o tipo_analise,
        # mas a função calculate_historical_rank_trends itera sobre as dezenas.
        # rank_value_column_name não é o nome da coluna da dezena aqui, mas sim o tipo de rank.
        rank_value_col_identifier = config.RANK_VALUE_COLUMN_FOR_TREND # Ex: 'rank_no_bloco' (nome da métrica PIVOTADA)


        trend_window = config.RANK_TREND_WINDOW_BLOCKS
        improving_thresh = config.RANK_TREND_SLOPE_IMPROVING_THRESHOLD
        worsening_thresh = config.RANK_TREND_SLOPE_WORSENING_THRESHOLD

        if not db_manager.table_exists(aggregated_block_table_name):
            logger.error(f"Tabela base '{aggregated_block_table_name}' não encontrada. Verifique 'Block Aggregation'.")
            return False

        df_rank_trends = calculate_historical_rank_trends(
            db_manager=db_manager,
            config=config, 
            aggregated_block_table_name=aggregated_block_table_name,
            rank_analysis_type_filter=rank_type_filter, # Ex: "rank_freq_bloco"
            rank_value_column_name=rank_value_col_identifier, # Passa o identificador da coluna de rank
            trend_window_blocks=trend_window,
            slope_improving_threshold=improving_thresh,
            slope_worsening_threshold=worsening_thresh
        )

        output_table_name = config.ANALYSIS_RANK_TREND_METRICS_TABLE_NAME
        expected_cols = [config.CONTEST_ID_COLUMN_NAME, config.DEZENA_COLUMN_NAME, config.RANK_SLOPE_COLUMN_NAME, config.TREND_STATUS_COLUMN_NAME]

        if df_rank_trends is None or df_rank_trends.empty:
            logger.warning(f"Nenhum dado de tendência de rank gerado. Tabela '{output_table_name}' vazia.")
            empty_trends_df = pd.DataFrame(columns=expected_cols)
            db_manager.save_dataframe(empty_trends_df, output_table_name, if_exists='replace')
        else:
            # Assegurar colunas e ordem e tipos corretos
            final_df = pd.DataFrame(columns=expected_cols) # Cria com ordem correta
            for col in expected_cols:
                if col in df_rank_trends.columns:
                    final_df[col] = df_rank_trends[col]
                else: # Preenche com defaults apropriados se alguma coluna estiver faltando no resultado
                    if col == config.RANK_SLOPE_COLUMN_NAME: final_df[col] = np.nan
                    elif col == config.TREND_STATUS_COLUMN_NAME: final_df[col] = 'indefinido'
                    elif col == config.CONTEST_ID_COLUMN_NAME: final_df[col] = pd.NA # Ou um ID default
                    elif col == config.DEZENA_COLUMN_NAME: final_df[col] = pd.NA # Ou um ID default

            db_manager.save_dataframe(final_df, output_table_name, if_exists='replace')
            logger.info(f"Métricas de tendência de rank salvas na '{output_table_name}' ({len(final_df)}).")

        logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
        return True

    except AttributeError as ae:
        logger.error(f"Erro de atributo em {step_name} (verifique config): {ae}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Erro crítico na etapa {step_name}: {e}", exc_info=True)
        return False