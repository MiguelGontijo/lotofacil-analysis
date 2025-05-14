# src/pipeline_steps/execute_detailed_cycle_metrics.py
import logging
import pandas as pd
from src.analysis.cycle_analysis import calculate_detailed_metrics_per_closed_cycle
from src.database_manager import DatabaseManager
from pathlib import Path # Importado para output_dir_from_pipeline type hint, mesmo que não usado no corpo da função

logger = logging.getLogger(__name__)

def run_detailed_cycle_metrics_step(
    all_data_df: pd.DataFrame, 
    db_manager: DatabaseManager,
    **kwargs 
) -> bool:
    try:
        logger.info("Iniciando etapa: Cálculo de Métricas Detalhadas por Ciclo Fechado (incluindo Rank e Grupos).")
        
        df_ciclos_detalhe = db_manager.load_dataframe_from_db("ciclos_detalhe")
        if df_ciclos_detalhe is None or df_ciclos_detalhe.empty:
            logger.warning("Tabela 'ciclos_detalhe' não encontrada ou vazia. Não é possível calcular métricas detalhadas por ciclo.")
            return True 

        if all_data_df.empty:
            logger.warning("O DataFrame de dados de concursos (all_data_df) está vazio. Pulando.")
            return True

        detailed_metrics_dfs = calculate_detailed_metrics_per_closed_cycle(all_data_df, df_ciclos_detalhe)
        
        saved_any = False
        if detailed_metrics_dfs:
            for table_name, df_metric in detailed_metrics_dfs.items(): # A chave já é o nome da tabela
                if df_metric is not None and not df_metric.empty:
                    db_manager.save_dataframe_to_db(df_metric, table_name, if_exists='replace')
                    logger.info(f"Métricas de ciclo '{table_name}' salvas ({len(df_metric)} registros).")
                    saved_any = True
                else:
                    logger.info(f"Nenhum dado para métrica de ciclo (tabela: {table_name}).")
        else:
            logger.warning("Nenhum DataFrame de métrica detalhada por ciclo foi retornado.")

        logger.info("Etapa: Cálculo de Métricas Detalhadas por Ciclo Fechado (incluindo Rank e Grupos) concluída.")
        return True 
    except Exception as e:
        logger.error(f"Erro na etapa de cálculo de métricas detalhadas por ciclo: {e}", exc_info=True)
        return False