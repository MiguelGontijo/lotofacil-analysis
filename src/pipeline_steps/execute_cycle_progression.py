# src/pipeline_steps/execute_cycle_progression.py
import logging
import pandas as pd
from src.analysis.cycle_progression_analysis import calculate_cycle_progression
from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def run_cycle_progression_analysis_step(all_data_df: pd.DataFrame, db_manager: DatabaseManager, **kwargs) -> bool:
    """
    Executa o cálculo da progressão dos ciclos concurso a concurso e salva no banco de dados.

    Args:
        all_data_df: DataFrame com todos os dados dos concursos.
        db_manager: Instância do DatabaseManager.
        **kwargs: Argumentos adicionais do pipeline.

    Returns:
        True se a análise foi concluída com sucesso e os dados salvos, False caso contrário.
    """
    try:
        logger.info("Iniciando etapa: Cálculo da Progressão de Ciclos Concurso a Concurso.")
        if all_data_df.empty:
            logger.warning("O DataFrame de dados está vazio. Pulando cálculo de progressão de ciclos.")
            return False # Ou True, dependendo se considera "sucesso" pular uma etapa vazia
            
        df_progression = calculate_cycle_progression(all_data_df)
        
        if df_progression is not None and not df_progression.empty:
            table_name = "ciclo_progressao_por_concurso"
            db_manager.save_dataframe_to_db(df_progression, table_name, if_exists='replace')
            logger.info(f"Dados de progressão de ciclo salvos na tabela '{table_name}' ({len(df_progression)} registros).")
            logger.info("Etapa: Cálculo da Progressão de Ciclos Concurso a Concurso concluída com sucesso.")
            return True
        else:
            logger.warning("Nenhum dado de progressão de ciclo foi gerado ou o DataFrame resultante está vazio.")
            logger.info("Etapa: Cálculo da Progressão de Ciclos Concurso a Concurso concluída (sem dados para salvar).")
            return True # Considera sucesso mesmo sem dados, pois a lógica rodou.

    except Exception as e:
        logger.error(f"Erro na etapa de cálculo da progressão de ciclos: {e}", exc_info=True)
        return False