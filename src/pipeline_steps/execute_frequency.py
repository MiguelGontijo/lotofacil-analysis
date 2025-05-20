# src/pipeline_steps/execute_frequency.py
import pandas as pd
import logging
from typing import Any, Dict, List, Optional

from src.config import Config 
from src.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def run_frequency_analysis(
    all_data_df: pd.DataFrame,
    db_manager: DatabaseManager,
    config: Config,
    shared_context: Dict[str, Any],
    **kwargs 
) -> bool:
    step_name = "Frequency Analysis (Historical)"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")

    required_attrs = [
        'CONTEST_ID_COLUMN_NAME', 'DEZENA_COLUMN_NAME', 'ALL_NUMBERS',
        'ANALYSIS_FREQUENCY_OVERALL_TABLE_NAME', 
        'FREQUENCY_COLUMN_NAME', 'RELATIVE_FREQUENCY_COLUMN_NAME'
    ]
    for attr in required_attrs:
        if not hasattr(config, attr):
            logger.error(f"{step_name}: Atributo de config '{attr}' não encontrado. Abortando.")
            return False

    if all_data_df.empty:
        logger.warning(f"{step_name}: 'all_data_df' vazio. Etapa pulada.")
        return True

    historical_frequency_data: List[pd.DataFrame] = []
    
    contest_id_col = config.CONTEST_ID_COLUMN_NAME
    dezena_col = config.DEZENA_COLUMN_NAME
    freq_col = config.FREQUENCY_COLUMN_NAME
    rel_freq_col = config.RELATIVE_FREQUENCY_COLUMN_NAME
    
    all_contest_ids = sorted(all_data_df[contest_id_col].unique())

    if not all_contest_ids:
        logger.warning(f"{step_name}: Nenhum concurso único. Etapa pulada.")
        return True
        
    from src.analysis.frequency_analysis import calculate_frequency, calculate_relative_frequency
    
    total_contests = len(all_contest_ids)
    log_interval = max(1, total_contests // 20) if total_contests > 100 else 1

    logger.info(f"{step_name}: Processando frequências para {total_contests} concursos.")

    for i, current_max_contest_id in enumerate(all_contest_ids):
        if (i + 1) % log_interval == 0 or i == 0 or i == total_contests - 1:
            logger.info(f"{step_name}: Progresso - {i+1}/{total_contests} (concurso de corte: {current_max_contest_id})")
        
        df_upto_contest = all_data_df[all_data_df[contest_id_col] <= current_max_contest_id].copy()

        if df_upto_contest.empty:
            logger.debug(f"Nenhum dado até concurso {current_max_contest_id}.")
            continue

        try:
            abs_freq_df_raw = calculate_frequency(df_upto_contest, config) 
            
            abs_freq_df = pd.DataFrame({dezena_col: config.ALL_NUMBERS})
            if abs_freq_df_raw is not None and not abs_freq_df_raw.empty:
                rename_map_abs = {}
                if 'Dezena' in abs_freq_df_raw.columns: rename_map_abs['Dezena'] = dezena_col
                if 'Frequencia Absoluta' in abs_freq_df_raw.columns: rename_map_abs['Frequencia Absoluta'] = freq_col
                abs_freq_df_renamed = abs_freq_df_raw.rename(columns=rename_map_abs, errors='ignore')
                
                if dezena_col in abs_freq_df_renamed and freq_col in abs_freq_df_renamed:
                    abs_freq_df = pd.merge(abs_freq_df, abs_freq_df_renamed[[dezena_col, freq_col]], on=dezena_col, how='left')
                else:
                    abs_freq_df[freq_col] = 0 
            else: # Se abs_freq_df_raw for None ou vazio
                 abs_freq_df[freq_col] = 0
            
            # CORREÇÃO FutureWarning:
            abs_freq_df[freq_col] = abs_freq_df[freq_col].fillna(0)
            abs_freq_df[dezena_col] = abs_freq_df[dezena_col].astype(int)

            num_draws_considered = len(df_upto_contest[contest_id_col].unique())
            
            rel_freq_df = pd.DataFrame({dezena_col: config.ALL_NUMBERS})
            if num_draws_considered > 0:
                temp_abs_for_rel = abs_freq_df.rename(columns={
                    dezena_col: 'Dezena', freq_col: 'Frequencia Absoluta'
                })
                rel_freq_df_raw = calculate_relative_frequency(temp_abs_for_rel, num_draws_considered, config)
                if rel_freq_df_raw is not None and not rel_freq_df_raw.empty:
                    rename_map_rel = {}
                    if 'Dezena' in rel_freq_df_raw.columns: rename_map_rel['Dezena'] = dezena_col
                    if 'Frequencia Relativa' in rel_freq_df_raw.columns: rename_map_rel['Frequencia Relativa'] = rel_freq_col
                    rel_freq_df_renamed = rel_freq_df_raw.rename(columns=rename_map_rel, errors='ignore')
                    if dezena_col in rel_freq_df_renamed and rel_freq_col in rel_freq_df_renamed:
                         rel_freq_df = pd.merge(rel_freq_df, rel_freq_df_renamed[[dezena_col, rel_freq_col]], on=dezena_col, how='left')
                    else:
                        rel_freq_df[rel_freq_col] = 0.0
                else: # Se rel_freq_df_raw for None ou vazio
                    rel_freq_df[rel_freq_col] = 0.0
            else: # Se num_draws_considered == 0
                rel_freq_df[rel_freq_col] = 0.0
            
            # CORREÇÃO FutureWarning:
            rel_freq_df[rel_freq_col] = rel_freq_df[rel_freq_col].fillna(0.0)
            rel_freq_df[dezena_col] = rel_freq_df[dezena_col].astype(int)

            merged_df = pd.merge(abs_freq_df, rel_freq_df, on=dezena_col, how='outer')
            merged_df[contest_id_col] = current_max_contest_id
            
            cols_order = [contest_id_col, dezena_col, freq_col, rel_freq_col]
            for col_name_check in cols_order: # Assegura colunas antes de reordenar
                if col_name_check not in merged_df.columns:
                    if col_name_check == freq_col: merged_df[col_name_check] = 0
                    elif col_name_check == rel_freq_col: merged_df[col_name_check] = 0.0
            merged_df = merged_df[cols_order]
            
            # CORREÇÃO FutureWarning (já deve estar preenchido, mas como garantia):
            merged_df[freq_col] = merged_df[freq_col].fillna(0)
            merged_df[rel_freq_col] = merged_df[rel_freq_col].fillna(0.0)

            historical_frequency_data.append(merged_df)
        except Exception as e_inner:
            logger.error(f"Erro ao processar frequências para concurso {current_max_contest_id}: {e_inner}", exc_info=True)

    if not historical_frequency_data:
        logger.warning(f"{step_name}: Nenhum dado de frequência histórica gerado.")
        return False

    final_historical_df = pd.concat(historical_frequency_data, ignore_index=True)
    
    try:
        table_name = config.ANALYSIS_FREQUENCY_OVERALL_TABLE_NAME 
        db_manager.save_dataframe(final_historical_df, table_name, if_exists='replace')
        logger.info(f"Dados de frequência ({len(final_historical_df)} linhas) salvos em '{table_name}'.")
        logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
        return True
    except Exception as e:
        logger.error(f"Erro na etapa {step_name} ao salvar dados: {e}", exc_info=True)
        return False