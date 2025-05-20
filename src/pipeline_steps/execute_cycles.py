import pandas as pd
import logging
from typing import Dict, Any, Optional # Adicionado Optional

from src.analysis.cycle_analysis import identify_and_process_cycles
# from src.database_manager import DatabaseManager # Para type hint
# from src.config import Config # Para type hint

logger = logging.getLogger(__name__)

def run_cycle_identification_step(
    all_data_df: pd.DataFrame,
    db_manager: Any, # DatabaseManager
    config: Any, # Config
    shared_context: Dict[str, Any],
    **kwargs
) -> Optional[pd.DataFrame]: # Tipo de retorno alterado para o DataFrame ou None
    step_name = "Cycle Identification and Basic Stats"
    logger.info(f"Iniciando etapa do pipeline: {step_name}")
    df_details_to_return: Optional[pd.DataFrame] = None # DataFrame a ser retornado

    try:
        cycle_analysis_results = identify_and_process_cycles(all_data_df, config)

        if not cycle_analysis_results:
            logger.warning(f"Nenhum resultado de ciclo retornado para {step_name}.")
            # Mesmo sem resultados, podemos querer retornar um DataFrame vazio se o output_key espera um.
            # Ou o Orchestrator pode lidar com None e não adicionar ao shared_context.
            # Vamos retornar None se não houver resultados válidos.
            logger.info(f"==== Etapa: {step_name} CONCLUÍDA (sem dados de ciclo) ====")
            return None

        saved_any = False
        df_details = cycle_analysis_results.get('ciclos_detalhe')
        if df_details is not None and not df_details.empty:
            try:
                db_manager.save_dataframe(df_details, 'ciclos_detalhe', if_exists='replace')
                logger.info(f"Resultados de 'ciclos_detalhe' salvos na tabela 'ciclos_detalhe'.")
                df_details_to_return = df_details # Armazena para retornar
                saved_any = True
            except Exception as e_save:
                logger.error(f"Erro ao salvar tabela 'ciclos_detalhe': {e_save}", exc_info=True)
                # Não retorna df_details se o salvamento falhar, ou decide com base na criticidade
        else:
            logger.info(f"DataFrame para 'ciclos_detalhe' vazio ou nulo.")
            # Se for vazio mas válido, pode ser retornado.
            if isinstance(df_details, pd.DataFrame): # Checa se é um DataFrame mesmo que vazio
                df_details_to_return = df_details


        df_summary = cycle_analysis_results.get('ciclos_sumario_estatisticas')
        if df_summary is not None and not df_summary.empty:
            try:
                db_manager.save_dataframe(df_summary, 'ciclos_sumario_estatisticas', if_exists='replace')
                logger.info(f"Resultados de 'ciclos_sumario_estatisticas' salvos na tabela 'ciclos_sumario_estatisticas'.")
                saved_any = True
            except Exception as e_save_sum:
                logger.error(f"Erro ao salvar tabela 'ciclos_sumario_estatisticas': {e_save_sum}", exc_info=True)
        else:
            logger.info(f"DataFrame para 'ciclos_sumario_estatisticas' vazio ou nulo.")

        if saved_any:
            logger.info(f"Etapa {step_name} concluída com dados salvos.")
        else:
            logger.info(f"Etapa {step_name} concluída, sem novos dados salvos (ou dados já existentes).")

        # Retorna o DataFrame que deve ser colocado no shared_context via output_key
        return df_details_to_return

    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        return None # Retorna None em caso de falha na etapa