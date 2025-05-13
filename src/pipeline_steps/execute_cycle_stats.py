# src/pipeline_steps/execute_cycle_stats.py
import pandas as pd
import logging
# A função principal agora é identify_and_process_cycles
from src.analysis.cycle_analysis import identify_and_process_cycles
from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def run_cycle_identification_and_stats(all_data_df: pd.DataFrame, db_manager: DatabaseManager, **kwargs) -> bool:
    """
    Identifica ciclos, calcula estatísticas e salva os resultados.
    Usa a função consolidada de cycle_analysis.py.
    """
    try:
        logger.info("Iniciando identificação de ciclos e cálculo de estatísticas consolidadas.")
        
        # Chama a função principal que retorna um dicionário de DataFrames
        cycle_analysis_results = identify_and_process_cycles(all_data_df)
        
        saved_any_table = False
        if cycle_analysis_results:
            df_details = cycle_analysis_results.get('ciclos_detalhe')
            if df_details is not None and not df_details.empty:
                db_manager.save_dataframe_to_db(df_details, 'ciclos_detalhe')
                logger.info(f"Detalhes dos ciclos salvos na tabela 'ciclos_detalhe' ({len(df_details)} registros).")
                saved_any_table = True
            else:
                logger.info("Nenhum DataFrame de detalhes de ciclo para salvar.")

            df_summary = cycle_analysis_results.get('ciclos_sumario_estatisticas')
            if df_summary is not None and not df_summary.empty:
                db_manager.save_dataframe_to_db(df_summary, 'ciclos_sumario_estatisticas')
                logger.info(f"Sumário estatístico dos ciclos salvo na tabela 'ciclos_sumario_estatisticas' ({len(df_summary)} registros).")
                saved_any_table = True
            else:
                logger.info("Nenhum DataFrame de sumário estatístico de ciclo para salvar.")
        else:
            logger.warning("Análise de ciclo não retornou resultados.")

        if not saved_any_table:
            logger.warning("Nenhuma tabela de análise de ciclo foi salva.")
            
        logger.info("Identificação de ciclos e cálculo de estatísticas consolidadas concluída.")
        return True # Retorna True mesmo se nenhuma tabela foi salva, mas a etapa rodou.
                    # Poderia retornar saved_any_table se quisermos ser mais estritos.
    except Exception as e:
        logger.error(f"Erro na identificação de ciclos e cálculo de estatísticas: {e}", exc_info=True)
        return False