# src/pipeline_steps/execute_detailed_cycle_metrics.py
import logging
import pandas as pd
from src.analysis.cycle_analysis import calculate_detailed_metrics_per_closed_cycle
from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def run_detailed_cycle_metrics_step(
    all_data_df: pd.DataFrame, 
    db_manager: DatabaseManager,
    **kwargs 
) -> bool:
    """
    Calcula e salva métricas detalhadas por dezena para cada ciclo fechado.
    Depende da tabela 'ciclos_detalhe' já existir.
    """
    try:
        logger.info("Iniciando etapa: Cálculo de Métricas Detalhadas por Ciclo Fechado.")
        
        df_ciclos_detalhe = db_manager.load_dataframe_from_db("ciclos_detalhe")
        if df_ciclos_detalhe is None or df_ciclos_detalhe.empty:
            logger.warning("Tabela 'ciclos_detalhe' não encontrada ou vazia. Não é possível calcular métricas detalhadas por ciclo.")
            # Se 'ciclos_detalhe' é fundamental, talvez retornar False aqui.
            # Mas se o pipeline puder continuar, True é aceitável.
            return True 

        if all_data_df.empty:
            logger.warning("O DataFrame de dados de concursos (all_data_df) está vazio. Pulando.")
            return True

        detailed_metrics_dfs = calculate_detailed_metrics_per_closed_cycle(all_data_df, df_ciclos_detalhe)
        
        saved_any = False
        if detailed_metrics_dfs:
            for table_key_suffix, df_metric in detailed_metrics_dfs.items():
                if df_metric is not None and not df_metric.empty:
                    # O nome da tabela já está no table_key_suffix, ex: "ciclo_metric_frequency"
                    table_name = table_key_suffix 
                    db_manager.save_dataframe_to_db(df_metric, table_name, if_exists='replace')
                    logger.info(f"Métricas de ciclo '{table_name}' salvas ({len(df_metric)} registros).")
                    saved_any = True
                else:
                    logger.info(f"Nenhum dado para métrica de ciclo '{table_key_suffix}'.")
        else:
            logger.warning("Nenhum DataFrame de métrica detalhada por ciclo foi retornado.")

        logger.info("Etapa: Cálculo de Métricas Detalhadas por Ciclo Fechado concluída.")
        return True # Ou 'saved_any' se o salvamento for crítico

    except Exception as e:
        logger.error(f"Erro na etapa de cálculo de métricas detalhadas por ciclo: {e}", exc_info=True)
        return False