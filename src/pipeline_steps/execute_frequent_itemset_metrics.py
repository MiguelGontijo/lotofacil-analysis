# src/pipeline_steps/execute_frequent_itemset_metrics.py
import pandas as pd
from typing import Dict, Any
import logging
import json

try:
    from ..analysis.frequent_itemset_metrics_analysis import calculate_frequent_itemset_delay_metrics
    from ..config import Config, CONTEST_ID_COLUMN_NAME # Importa CONTEST_ID_COLUMN_NAME
    from ..database_manager import DatabaseManager
except ImportError:
    from src.analysis.frequent_itemset_metrics_analysis import calculate_frequent_itemset_delay_metrics
    from src.config import Config, CONTEST_ID_COLUMN_NAME
    from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def run_frequent_itemset_metrics_step(
    all_data_df: pd.DataFrame, 
    db_manager: DatabaseManager, 
    config: Config, 
    shared_context: Dict[str, Any], 
    **kwargs 
) -> None:
    logger.info("Iniciando etapa: Cálculo de Métricas de Atraso para Itemsets Frequentes.")
    
    if all_data_df is None:
        logger.error("Argumento 'all_data_df' recebido como None. Abortando etapa.")
        return
    if all_data_df.empty:
        logger.error("Argumento 'all_data_df' recebido como DataFrame vazio. Abortando etapa.")
        return
        
    # <<< VERIFICAÇÃO E USO DE CONTEST_ID_COLUMN_NAME >>>
    if CONTEST_ID_COLUMN_NAME not in all_data_df.columns:
        logger.error(f"Coluna '{CONTEST_ID_COLUMN_NAME}' (esperada do config) ausente no DataFrame 'all_data_df'. Colunas presentes: {all_data_df.columns.tolist()}. Abortando etapa.")
        return
        
    try:
        # Usa a coluna de ID padronizada do config
        latest_contest_id = int(all_data_df[CONTEST_ID_COLUMN_NAME].max())
    except Exception as e:
        logger.error(f"Não foi possível obter latest_contest_id de '{CONTEST_ID_COLUMN_NAME}' em 'all_data_df': {e}", exc_info=True)
        return

    #logger.info(f"DataFrame 'all_data_df' recebido com {len(all_data_df)} linhas. Colunas: {all_data_df.columns.tolist()}. Usando '{CONTEST_ID_COLUMN_NAME}' como ID.")

    frequent_itemsets_table_name = "frequent_itemsets"
    if not db_manager.table_exists(frequent_itemsets_table_name):
        logger.warning(f"Tabela '{frequent_itemsets_table_name}' não existe. A etapa 'frequent_itemsets_analysis' precisa ser executada primeiro. Pulando.")
        return
    
    frequent_itemsets_df = db_manager.load_dataframe(frequent_itemsets_table_name)
    if frequent_itemsets_df is None or frequent_itemsets_df.empty:
        logger.warning(f"Tabela '{frequent_itemsets_table_name}' está vazia. Não há itemsets para analisar. Pulando.")
        return

    try:
        itemset_delay_metrics_df = calculate_frequent_itemset_delay_metrics(
            all_data_df.copy(), 
            frequent_itemsets_df, 
            latest_contest_id, # <<< PASSANDO latest_contest_id
            config 
        )
    except ValueError as ve:
        logger.error(f"Erro de valor ao calcular métricas de atraso para itemsets: {ve}", exc_info=True)
        return
    except Exception as e:
        logger.error(f"Erro inesperado ao calcular métricas de atraso para itemsets frequentes: {e}", exc_info=True)
        return

    output_table_name = "frequent_itemset_metrics"
    if itemset_delay_metrics_df is not None and not itemset_delay_metrics_df.empty:
        try:
            db_manager.save_dataframe(itemset_delay_metrics_df, output_table_name, if_exists="replace")
            logger.info(f"Métricas de atraso para itemsets frequentes salvas na tabela '{output_table_name}'. {len(itemset_delay_metrics_df)} registros.")
            if shared_context is not None: 
                 shared_context[output_table_name + '_df'] = itemset_delay_metrics_df 
        except Exception as e:
            logger.error(f"Erro ao salvar métricas de atraso para itemsets na tabela '{output_table_name}': {e}", exc_info=True)
    else:
        logger.info("Nenhuma métrica de atraso para itemsets frequentes foi gerada ou retornada para salvar.")

    logger.info("Etapa: Cálculo de Métricas de Atraso para Itemsets Frequentes concluída.")