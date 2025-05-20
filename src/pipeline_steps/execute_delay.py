# src/pipeline_steps/execute_delay.py
import pandas as pd
import logging
from typing import Any, Dict, List, Optional

from src.config import Config 
from src.database_manager import DatabaseManager

from src.analysis.delay_analysis import (
    get_draw_matrix, 
    calculate_current_delay, 
    calculate_max_delay, 
    calculate_mean_delay
)

logger = logging.getLogger(__name__)

def run_delay_analysis(
    all_data_df: pd.DataFrame,
    db_manager: DatabaseManager,
    config: Config,
    shared_context: Dict[str, Any],
    force_full_recalculation: bool = False, # Novo parâmetro, pode ser controlado pelo --force-reload
    **kwargs 
) -> bool:
    step_name = "Delay Analysis (Historical Incremental)"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")

    # Validações de config (como antes)
    required_attrs = [
        'CONTEST_ID_COLUMN_NAME', 'DEZENA_COLUMN_NAME', 'ALL_NUMBERS',
        'ANALYSIS_DELAYS_TABLE_NAME', 'CURRENT_DELAY_COLUMN_NAME',
        'MAX_DELAY_OBSERVED_COLUMN_NAME', 'AVG_DELAY_COLUMN_NAME',
        'MIN_CONTESTS_FOR_HISTORICAL_DELAY'
    ]
    for attr in required_attrs:
        if not hasattr(config, attr):
            logger.error(f"{step_name}: Atributo de config '{attr}' não encontrado. Abortando.")
            return False
    if all_data_df.empty:
        logger.warning(f"{step_name}: 'all_data_df' vazio. Etapa pulada.")
        return True

    contest_id_col = config.CONTEST_ID_COLUMN_NAME
    dezena_col = config.DEZENA_COLUMN_NAME
    current_delay_col_name = config.CURRENT_DELAY_COLUMN_NAME
    max_delay_col_name = config.MAX_DELAY_OBSERVED_COLUMN_NAME
    avg_delay_col_name = config.AVG_DELAY_COLUMN_NAME
    table_name = config.ANALYSIS_DELAYS_TABLE_NAME
    min_hist_contests = config.MIN_CONTESTS_FOR_HISTORICAL_DELAY

    start_processing_from_contest_id = 1 # Default para processar tudo
    if_exists_mode = 'replace'

    if not force_full_recalculation and db_manager.table_exists(table_name):
        last_processed_query = f"SELECT MAX({contest_id_col}) FROM {table_name}"
        last_processed_df = db_manager.execute_query(last_processed_query)
        if last_processed_df is not None and not last_processed_df.empty and pd.notna(last_processed_df.iloc[0,0]):
            last_saved_contest = int(last_processed_df.iloc[0,0])
            start_processing_from_contest_id = last_saved_contest + 1
            if_exists_mode = 'append'
            logger.info(f"{step_name}: Tabela '{table_name}' existente. Último concurso processado: {last_saved_contest}.")
        else:
            logger.info(f"{step_name}: Tabela '{table_name}' existe mas está vazia ou não tem {contest_id_col}. Recalculando tudo.")
    else:
        logger.info(f"{step_name}: {'--force-reload ativo' if force_full_recalculation else 'Tabela não existe'}. Recalculando tudo para '{table_name}'.")

    # Seleciona apenas os concursos que precisam ser processados como "pontos finais" do cálculo histórico
    # Se for append, processaremos apenas os novos. Se for replace, todos.
    all_contest_ids_in_data = sorted(all_data_df[contest_id_col].unique())
    
    # Pontos de concurso para os quais vamos calcular e salvar o estado histórico
    target_contest_ids_to_calculate = [
        cid for i, cid in enumerate(all_contest_ids_in_data) 
        if cid >= start_processing_from_contest_id and i >= min_hist_contests - 1
    ]
    
    if not target_contest_ids_to_calculate:
        logger.info(f"{step_name}: Nenhum novo concurso para processar (a partir de {start_processing_from_contest_id} e após {min_hist_contests-1} iniciais).")
        return True

    historical_delays_data: List[pd.DataFrame] = []
    
    total_points_to_process = len(target_contest_ids_to_calculate)
    log_interval = max(1, total_points_to_process // 20) if total_points_to_process > 100 else 1
    
    logger.info(f"{step_name}: Processamento de atrasos para {total_points_to_process} pontos (concursos de corte).")

    for i, current_max_contest_id in enumerate(target_contest_ids_to_calculate):
        if (i + 1) % log_interval == 0 or i == 0 or i == total_points_to_process - 1 :
             logger.info(f"{step_name}: Progresso - {i+1}/{total_points_to_process} (concurso de corte: {current_max_contest_id})")
        
        # df_upto_contest AINDA é todo o histórico até current_max_contest_id
        # Isso é necessário para calcular max_delay e avg_delay corretamente da forma atual.
        df_upto_contest = all_data_df[all_data_df[contest_id_col] <= current_max_contest_id].copy()
        if df_upto_contest.empty or len(df_upto_contest) < min_hist_contests : 
            logger.debug(f"Dados insuficientes até concurso {current_max_contest_id} (linhas: {len(df_upto_contest)}).")
            continue

        try:
            draw_matrix_upto_contest = get_draw_matrix(df_upto_contest, config)
            if draw_matrix_upto_contest.empty:
                logger.warning(f"Matriz de sorteios vazia para concurso {current_max_contest_id}.")
                # Lógica de default para este ponto de cálculo
                default_df = pd.DataFrame({
                    contest_id_col: current_max_contest_id,
                    dezena_col: config.ALL_NUMBERS,
                    current_delay_col_name: len(df_upto_contest),
                    max_delay_col_name: len(df_upto_contest),
                    avg_delay_col_name: pd.NA
                })
                cols_order_def = [contest_id_col, dezena_col, current_delay_col_name, max_delay_col_name, avg_delay_col_name]
                historical_delays_data.append(default_df[cols_order_def])
                continue

            last_contest_in_matrix = draw_matrix_upto_contest.index.max()
            first_contest_in_matrix = draw_matrix_upto_contest.index.min()
            
            current_delay_df_raw = calculate_current_delay(draw_matrix_upto_contest, config, last_contest_in_matrix)
            max_delay_df_raw = calculate_max_delay(draw_matrix_upto_contest, config, first_contest_in_matrix, last_contest_in_matrix)
            mean_delay_df_raw = calculate_mean_delay(draw_matrix_upto_contest, config)
            
            # ... (lógica de merge e formatação como na versão anterior)
            all_dezenas_df = pd.DataFrame({dezena_col: config.ALL_NUMBERS})
            all_dezenas_df[dezena_col] = all_dezenas_df[dezena_col].astype(int)

            current_delay_df = all_dezenas_df.copy()
            if current_delay_df_raw is not None and not current_delay_df_raw.empty:
                current_delay_df_raw = current_delay_df_raw.rename(columns={'Dezena': dezena_col, 'Atraso Atual': current_delay_col_name}, errors='ignore')
                if dezena_col in current_delay_df_raw and current_delay_col_name in current_delay_df_raw:
                    current_delay_df = pd.merge(current_delay_df, current_delay_df_raw[[dezena_col, current_delay_col_name]], on=dezena_col, how='left')
            current_delay_df[current_delay_col_name] = current_delay_df[current_delay_col_name].fillna(len(draw_matrix_upto_contest)).astype(int)

            max_delay_df = all_dezenas_df.copy()
            if max_delay_df_raw is not None and not max_delay_df_raw.empty:
                max_delay_df_raw = max_delay_df_raw.rename(columns={'Dezena': dezena_col, 'Atraso Maximo': max_delay_col_name}, errors='ignore')
                if dezena_col in max_delay_df_raw and max_delay_col_name in max_delay_df_raw:
                     max_delay_df = pd.merge(max_delay_df, max_delay_df_raw[[dezena_col, max_delay_col_name]], on=dezena_col, how='left')
            
            mean_delay_df = all_dezenas_df.copy()
            if mean_delay_df_raw is not None and not mean_delay_df_raw.empty:
                mean_delay_df_raw = mean_delay_df_raw.rename(columns={'Dezena': dezena_col, 'Atraso Medio': avg_delay_col_name}, errors='ignore')
                if dezena_col in mean_delay_df_raw and avg_delay_col_name in mean_delay_df_raw:
                    mean_delay_df = pd.merge(mean_delay_df, mean_delay_df_raw[[dezena_col, avg_delay_col_name]], on=dezena_col, how='left')
            mean_delay_df[avg_delay_col_name] = pd.to_numeric(mean_delay_df[avg_delay_col_name], errors='coerce')

            merged_df = pd.merge(current_delay_df, max_delay_df, on=dezena_col, how='outer')
            merged_df = pd.merge(merged_df, mean_delay_df, on=dezena_col, how='outer')
            merged_df[contest_id_col] = current_max_contest_id
            cols_order = [contest_id_col, dezena_col, current_delay_col_name, max_delay_col_name, avg_delay_col_name]
            
            for col_name_check in cols_order:
                if col_name_check not in merged_df.columns:
                    default_val = len(draw_matrix_upto_contest) if col_name_check != avg_delay_col_name else pd.NA
                    merged_df[col_name_check] = default_val
            merged_df = merged_df[cols_order]

            merged_df[current_delay_col_name] = merged_df[current_delay_col_name].fillna(len(draw_matrix_upto_contest)).astype(int)
            merged_df[max_delay_col_name] = merged_df[max_delay_col_name].fillna(merged_df[current_delay_col_name]).astype(int)
            
            historical_delays_data.append(merged_df)
        except Exception as e_inner:
            logger.error(f"Erro ao processar atrasos para concurso {current_max_contest_id}: {e_inner}", exc_info=True)

    if not historical_delays_data:
        logger.warning(f"{step_name}: Nenhum novo dado de atraso histórico foi gerado para o intervalo solicitado.")
        return True # Não é um erro se não havia nada novo para processar

    final_df_to_save = pd.concat(historical_delays_data, ignore_index=True)
    
    try:
        if if_exists_mode == 'replace' and db_manager.table_exists(table_name):
             logger.info(f"Modo 'replace': Removendo dados antigos da tabela '{table_name}' antes de salvar.")
             # Poderia deletar ou a tabela seria substituída por to_sql.
             # Se to_sql com if_exists='replace' não apagar primeiro, você pode precisar de um DELETE.
             # Mas df.to_sql com if_exists='replace' geralmente dropa e recria.
        
        db_manager.save_dataframe(final_df_to_save, table_name, if_exists=if_exists_mode)
        logger.info(f"Dados de atraso ({len(final_df_to_save)} linhas) salvos em '{table_name}' (modo: {if_exists_mode}).")
        logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa {step_name} ao salvar dados: {e}", exc_info=True)
        return False