# src/pipeline_steps/execute_repetition_analysis.py
import logging
import pandas as pd
from src.analysis.repetition_analysis import calculate_previous_draw_repetitions
from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def run_repetition_analysis_step(all_data_df: pd.DataFrame, db_manager: DatabaseManager, **kwargs) -> bool:
    """
    Executa a análise de repetição de dezenas do concurso anterior e salva no banco de dados.

    Args:
        all_data_df: DataFrame com todos os dados dos concursos.
        db_manager: Instância do DatabaseManager.
        **kwargs: Argumentos adicionais do pipeline.

    Returns:
        True se a análise foi concluída com sucesso e os dados salvos, False caso contrário.
    """
    try:
        logger.info("Iniciando etapa: Análise de Repetição do Sorteio Anterior.")
        if all_data_df.empty or len(all_data_df) < 2:
            logger.warning("DataFrame de dados insuficiente para análise de repetição. Pulando.")
            return True 
            
        df_repetitions = calculate_previous_draw_repetitions(all_data_df)
        
        if df_repetitions is not None and not df_repetitions.empty:
            table_name = "analise_repeticao_concurso_anterior"
            db_manager.save_dataframe_to_db(df_repetitions, table_name, if_exists='replace')
            logger.info(f"Dados de repetição do sorteio anterior salvos na tabela '{table_name}' ({len(df_repetitions)} registros).")
            logger.info("Etapa: Análise de Repetição do Sorteio Anterior concluída com sucesso.")
            return True
        else:
            logger.warning("Nenhum dado de repetição foi gerado ou o DataFrame resultante está vazio.")
            logger.info("Etapa: Análise de Repetição do Sorteio Anterior concluída (sem dados para salvar).")
            return True

    except Exception as e:
        logger.error(f"Erro na etapa de análise de repetição do sorteio anterior: {e}", exc_info=True)
        return False