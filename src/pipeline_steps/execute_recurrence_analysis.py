# src/pipeline_steps/execute_recurrence_analysis.py
import logging
import pandas as pd
from typing import Any, Dict, List, Optional

from src.config import Config
from src.database_manager import DatabaseManager

from src.analysis.recurrence_analysis import analyze_recurrence
from src.analysis.delay_analysis import get_draw_matrix 

logger = logging.getLogger(__name__)

def run_recurrence_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: DatabaseManager,
    config: Config,
    shared_context: Dict[str, Any],
    force_full_recalculation: bool = False, # Adicionado para controle incremental
    **kwargs
) -> bool:
    step_name = "Recurrence Analysis (Historical CDF Incremental)"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")

    required_attrs = [
        'CONTEST_ID_COLUMN_NAME', 'DEZENA_COLUMN_NAME', 'ALL_NUMBERS',
        'ANALYSIS_RECURRENCE_CDF_TABLE_NAME', 'RECURRENCE_CDF_COLUMN_NAME',
        'ANALYSIS_DELAYS_TABLE_NAME', 'CURRENT_DELAY_COLUMN_NAME',
        'MIN_CONTESTS_FOR_HISTORICAL_RECURRENCE'
    ]
    for attr in required_attrs:
        if not hasattr(config, attr):
            logger.error(f"{step_name}: Atributo de config '{attr}' não encontrado. Abortando.")
            return False

    if not isinstance(all_data_df, pd.DataFrame) or all_data_df.empty:
        logger.warning(f"{step_name}: DataFrame de sorteios (all_data_df) inválido ou vazio. Etapa pulada.")
        return True 

    contest_id_col = config.CONTEST_ID_COLUMN_NAME
    dezena_col = config.DEZENA_COLUMN_NAME
    recurrence_cdf_col = config.RECURRENCE_CDF_COLUMN_NAME
    delays_table_name = config.ANALYSIS_DELAYS_TABLE_NAME
    table_name_to_save = config.ANALYSIS_RECURRENCE_CDF_TABLE_NAME
    min_hist_contests = config.MIN_CONTESTS_FOR_HISTORICAL_RECURRENCE

    start_processing_from_contest_id = 1
    if_exists_mode = 'replace'

    # Lógica Incremental
    if not force_full_recalculation and db_manager.table_exists(table_name_to_save):
        last_processed_query = f"SELECT MAX({contest_id_col}) FROM {table_name_to_save}"
        last_processed_df = db_manager.execute_query(last_processed_query)
        if last_processed_df is not None and not last_processed_df.empty and pd.notna(last_processed_df.iloc[0,0]):
            last_saved_contest = int(last_processed_df.iloc[0,0])
            max_contest_in_data = all_data_df[contest_id_col].max() # Último concurso nos dados de entrada
            if max_contest_in_data > last_saved_contest:
                start_processing_from_contest_id = last_saved_contest + 1
                if_exists_mode = 'append'
                logger.info(f"{step_name}: Tabela '{table_name_to_save}' existente. Último concurso processado: {last_saved_contest}. Iniciando incremental de {start_processing_from_contest_id}.")
            else:
                logger.info(f"{step_name}: Tabela '{table_name_to_save}' já está atualizada até o concurso {last_saved_contest}. Nenhum novo concurso para processar.")
                return True # Nada a fazer
        else:
            logger.info(f"{step_name}: Tabela '{table_name_to_save}' existe mas está vazia ou sem {contest_id_col}. Recalculando tudo.")
            # if_exists_mode permanece 'replace'
    else:
        logger.info(f"{step_name}: {'--force-reload ativo' if force_full_recalculation else 'Tabela de destino não existe'}. Recalculando tudo para '{table_name_to_save}'.")
        # if_exists_mode permanece 'replace'

    all_contest_ids_in_data = sorted(all_data_df[contest_id_col].unique())
    
    target_contest_ids_to_calculate = [
        cid for i, cid in enumerate(all_contest_ids_in_data) 
        if cid >= start_processing_from_contest_id and i >= min_hist_contests - 1
    ]
    
    if not target_contest_ids_to_calculate:
        logger.info(f"{step_name}: Nenhum novo concurso para processar após filtros (a partir de {start_processing_from_contest_id}, mínimo histórico {min_hist_contests}).")
        return True
        
    historical_recurrence_data: List[pd.DataFrame] = []
    total_points_to_process = len(target_contest_ids_to_calculate)
    log_interval = max(1, total_points_to_process // 20) if total_points_to_process > 100 else 1
    logger.info(f"{step_name}: Processamento de recorrência (após {min_hist_contests-1} concursos iniciais ou a partir de {start_processing_from_contest_id}) para {total_points_to_process} pontos.")

    processed_points_count = 0
    for i, current_max_contest_id in enumerate(target_contest_ids_to_calculate):
        if (i + 1) % log_interval == 0 or i == 0 or i == total_points_to_process - 1:
            logger.info(f"{step_name}: Progresso - {i+1}/{total_points_to_process} (concurso de corte: {current_max_contest_id})")
        
        # df_upto_contest AINDA é todo o histórico até current_max_contest_id para o cálculo correto de gaps
        df_upto_contest = all_data_df[all_data_df[contest_id_col] <= current_max_contest_id].copy()

        if df_upto_contest.empty or len(df_upto_contest) < min_hist_contests:
            logger.debug(f"Dados insuficientes até concurso {current_max_contest_id} (linhas: {len(df_upto_contest)}).")
            continue

        try:
            draw_matrix = get_draw_matrix(df_upto_contest, config)
            if draw_matrix.empty:
                logger.warning(f"Matriz de sorteios vazia para concurso {current_max_contest_id}.")
                continue # Ou adicione um registro default como em execute_delay

            query_delays = f"SELECT {config.DEZENA_COLUMN_NAME} AS dezena, {config.CURRENT_DELAY_COLUMN_NAME} AS current_delay FROM {delays_table_name} WHERE {contest_id_col} = ?"
            current_delays_df_for_contest = db_manager.execute_query(query_delays, params=(current_max_contest_id,))

            if current_delays_df_for_contest is None or current_delays_df_for_contest.empty:
                logger.debug(f"Atrasos atuais não encontrados para concurso {current_max_contest_id}. Usando default (0).") # MUDADO PARA DEBUG
                current_delays_df_for_contest = pd.DataFrame({
                    config.DEZENA_COLUMN_NAME: config.ALL_NUMBERS, 'current_delay': 0
                })
            
            if config.DEZENA_COLUMN_NAME not in current_delays_df_for_contest.columns or \
               'current_delay' not in current_delays_df_for_contest.columns:
                logger.error(f"Colunas '{config.DEZENA_COLUMN_NAME}' ou 'current_delay' não encontradas nos atrasos do concurso {current_max_contest_id}.")
                continue

            recurrence_stats_df_raw = analyze_recurrence(draw_matrix, current_delays_df_for_contest, config)

            df_to_append = pd.DataFrame() # Inicializa df_to_append
            if recurrence_stats_df_raw is None or recurrence_stats_df_raw.empty:
                logger.warning(f"Análise de recorrência vazia para concurso {current_max_contest_id}.")
                df_to_append = pd.DataFrame({
                    contest_id_col: current_max_contest_id,
                    config.DEZENA_COLUMN_NAME: config.ALL_NUMBERS,
                    recurrence_cdf_col: pd.NA 
                })
            else:
                if config.DEZENA_COLUMN_NAME not in recurrence_stats_df_raw.columns or 'CDF_Atraso_Atual' not in recurrence_stats_df_raw.columns:
                     logger.warning(f"Colunas esperadas não em recurrence_stats_df_raw para {current_max_contest_id}.")
                     df_to_append = pd.DataFrame({
                        contest_id_col: current_max_contest_id,
                        config.DEZENA_COLUMN_NAME: config.ALL_NUMBERS,
                        recurrence_cdf_col: pd.NA
                     })
                else:
                    df_to_append = recurrence_stats_df_raw[[config.DEZENA_COLUMN_NAME, 'CDF_Atraso_Atual']].copy()
                    df_to_append.rename(columns={'CDF_Atraso_Atual': recurrence_cdf_col}, inplace=True)
                    df_to_append[contest_id_col] = current_max_contest_id
            
            df_to_append[config.DEZENA_COLUMN_NAME] = df_to_append[config.DEZENA_COLUMN_NAME].astype(int)
            cols_order = [contest_id_col, config.DEZENA_COLUMN_NAME, recurrence_cdf_col]
            
            for col_name_check in cols_order:
                if col_name_check not in df_to_append.columns:
                    if col_name_check == recurrence_cdf_col: df_to_append[col_name_check] = pd.NA
            
            df_to_append = df_to_append[cols_order]
            df_to_append[recurrence_cdf_col] = pd.to_numeric(df_to_append[recurrence_cdf_col], errors='coerce')

            historical_recurrence_data.append(df_to_append)
            processed_points_count +=1

        except Exception as e_inner:
            logger.error(f"Erro ao processar recorrência para concurso {current_max_contest_id}: {e_inner}", exc_info=True)

    if not historical_recurrence_data:
        logger.info(f"{step_name}: Nenhum novo dado de recorrência histórica foi gerado para o intervalo solicitado.")
        # Não é um erro se não havia nada novo para processar
        # (ex: start_processing_from_contest_id > último concurso nos dados, ou target_contest_ids_to_calculate ficou vazio)
        return True 

    final_historical_df = pd.concat(historical_recurrence_data, ignore_index=True)

    try:
        db_manager.save_dataframe(final_historical_df, table_name_to_save, if_exists=if_exists_mode)
        logger.info(f"Dados de recorrência CDF ({len(final_historical_df)} linhas) salvos em '{table_name_to_save}' (modo: {if_exists_mode}).")
        logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa {step_name} ao salvar dados: {e}", exc_info=True)
        return False