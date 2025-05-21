# Arquivo: src/analysis/cycle_analysis.py
# (Conteúdo completo da minha sugestão anterior, que inclui calculate_detailed_metrics_per_closed_cycle
# e as chaves de dicionário padronizadas)

import pandas as pd
from typing import List, Dict, Tuple, Set, Optional, Any
import logging
import numpy as np

# from src.config import ALL_NUMBERS # Acessado via config
from src.analysis.chunk_analysis import (
    calculate_frequency_in_chunk, 
    get_draw_matrix_for_chunk,    
    calculate_delays_for_matrix   
)
from src.analysis.number_properties_analysis import analyze_draw_properties 

logger = logging.getLogger(__name__)

# Constantes para as chaves do dicionário de resultados
KEY_CYCLE_DETAILS_DF = 'analysis_cycle_details_df'
KEY_CYCLE_SUMMARY_DF = 'analysis_cycle_summary_df'

def identify_and_process_cycles(all_data_df: pd.DataFrame, config: Any) -> Dict[str, Optional[pd.DataFrame]]:
    logger.info("Iniciando análise completa de ciclos.")
    results: Dict[str, Optional[pd.DataFrame]] = {
        KEY_CYCLE_DETAILS_DF: None, 
        KEY_CYCLE_SUMMARY_DF: None
    }
    if all_data_df is None or all_data_df.empty:
        logger.warning("DataFrame de entrada para 'identify_and_process_cycles' está vazio ou nulo.")
        return results

    contest_col = config.CONTEST_ID_COLUMN_NAME
    ball_cols = config.BALL_NUMBER_COLUMNS
    current_all_numbers_set = set(config.ALL_NUMBERS)

    required_cols = [contest_col] + ball_cols
    missing_cols = [col for col in required_cols if col not in all_data_df.columns]
    if missing_cols:
        logger.error(f"Colunas obrigatórias ausentes no DataFrame de entrada: {missing_cols}")
        return results

    df_sorted = all_data_df.copy()
    try:
        df_sorted[contest_col] = pd.to_numeric(df_sorted[contest_col])
    except Exception as e:
        logger.error(f"Erro ao converter a coluna {contest_col} para numérico: {e}", exc_info=True)
        return results
    df_sorted = df_sorted.sort_values(by=contest_col).reset_index(drop=True)
    
    cycles_data: List[Dict[str, Any]] = []
    current_cycle_numbers_needed = current_all_numbers_set.copy()
    current_cycle_start_contest = int(df_sorted.loc[0, contest_col]) if not df_sorted.empty else 0
    cycle_count = 0
    last_processed_contest_num_for_open_cycle = int(df_sorted[contest_col].max()) if not df_sorted.empty else 0
    
    for index, row in df_sorted.iterrows():
        contest_number = int(row[contest_col])
        try:
            drawn_numbers_in_this_contest = set(int(row[col_b]) for col_b in ball_cols if pd.notna(row[col_b]))
        except ValueError: 
            logger.warning(f"Erro ao converter dezenas para o concurso {contest_number}. Pulando este concurso na análise de ciclos.")
            continue
            
        if current_cycle_numbers_needed == current_all_numbers_set and contest_number != current_cycle_start_contest:
             current_cycle_start_contest = contest_number
             
        current_cycle_numbers_needed.difference_update(drawn_numbers_in_this_contest)
        
        if not current_cycle_numbers_needed: 
            cycle_count += 1
            cycles_data.append({
                'ciclo_num': cycle_count,
                'concurso_inicio': current_cycle_start_contest,
                'concurso_fim': contest_number,
                'duracao_concursos': contest_number - current_cycle_start_contest + 1,
                'numeros_faltantes': None,
                'qtd_faltantes': 0
            })
            current_cycle_numbers_needed = current_all_numbers_set.copy()
            if index + 1 < len(df_sorted):
                current_cycle_start_contest = int(df_sorted.loc[index + 1, contest_col])
            else: 
                current_cycle_start_contest = contest_number + 1 
                
    if current_cycle_numbers_needed and current_cycle_numbers_needed != current_all_numbers_set:
        if not df_sorted.empty and current_cycle_start_contest <= last_processed_contest_num_for_open_cycle:
            cycles_data.append({
                'ciclo_num': cycle_count + 1,
                'concurso_inicio': current_cycle_start_contest,
                'concurso_fim': pd.NA, 
                'duracao_concursos': pd.NA, 
                'numeros_faltantes': ",".join(map(str, sorted(list(current_cycle_numbers_needed)))),
                'qtd_faltantes': len(current_cycle_numbers_needed)
            })
    elif cycle_count == 0 and not df_sorted.empty and current_cycle_numbers_needed and current_cycle_numbers_needed != current_all_numbers_set : 
            cycles_data.append({
                'ciclo_num': 1, 
                'concurso_inicio': current_cycle_start_contest, 
                'concurso_fim': pd.NA,
                'duracao_concursos': pd.NA,
                'numeros_faltantes': ",".join(map(str, sorted(list(current_cycle_numbers_needed)))),
                'qtd_faltantes': len(current_cycle_numbers_needed)
            })
            
    if cycles_data:
        df_cycles_detail = pd.DataFrame(cycles_data)
        for col_int in ['concurso_fim', 'duracao_concursos', 'qtd_faltantes', 'ciclo_num', 'concurso_inicio']:
            if col_int in df_cycles_detail.columns:
                df_cycles_detail[col_int] = pd.to_numeric(df_cycles_detail[col_int], errors='coerce').astype('Int64')
        
        results[KEY_CYCLE_DETAILS_DF] = df_cycles_detail
        
        if 'duracao_concursos' in df_cycles_detail.columns:
            df_closed_cycles = df_cycles_detail[df_cycles_detail['duracao_concursos'].notna()].copy()
            if not df_closed_cycles.empty: 
                df_closed_cycles['duracao_concursos'] = pd.to_numeric(df_closed_cycles['duracao_concursos']) 
                
                summary_stats_data = {
                    'total_ciclos_fechados': int(len(df_closed_cycles)),
                    'duracao_media_ciclo': float(df_closed_cycles['duracao_concursos'].mean()) if len(df_closed_cycles) > 0 else np.nan,
                    'duracao_min_ciclo': int(df_closed_cycles['duracao_concursos'].min()) if len(df_closed_cycles) > 0 else pd.NA, 
                    'duracao_max_ciclo': int(df_closed_cycles['duracao_concursos'].max()) if len(df_closed_cycles) > 0 else pd.NA, 
                    'duracao_mediana_ciclo': float(df_closed_cycles['duracao_concursos'].median()) if len(df_closed_cycles) > 0 else np.nan
                }
                df_summary = pd.DataFrame([summary_stats_data])
                for col_int_sum in ['total_ciclos_fechados', 'duracao_min_ciclo', 'duracao_max_ciclo']:
                     if col_int_sum in df_summary.columns:
                        df_summary[col_int_sum] = pd.to_numeric(df_summary[col_int_sum], errors='coerce').astype('Int64')
                
                results[KEY_CYCLE_SUMMARY_DF] = df_summary
                
    logger.info("Análise de identificação de ciclos e sumário concluída.")
    return results

def calculate_detailed_metrics_per_closed_cycle(
    all_data_df: pd.DataFrame, 
    df_ciclos_detalhe: Optional[pd.DataFrame], 
    config: Any 
) -> Dict[str, Optional[pd.DataFrame]]:
    logger.info("Iniciando cálculo de métricas detalhadas por dezena/ciclo.")
    results_data_lists: Dict[str, List[Dict[str, Any]]] = {
        "frequency": [], "mean_delay": [], "max_delay": [], 
        "final_delay": [], "rank_frequency": [], "group_metrics_cycle": []
    }
    output_dfs: Dict[str, Optional[pd.DataFrame]] = {
        config.CYCLE_METRIC_FREQUENCY_TABLE_NAME: None,
        config.CYCLE_METRIC_ATRASO_MEDIO_TABLE_NAME: None, 
        config.CYCLE_METRIC_ATRASO_MAXIMO_TABLE_NAME: None, 
        config.CYCLE_METRIC_ATRASO_FINAL_TABLE_NAME: None, 
        config.CYCLE_RANK_FREQUENCY_TABLE_NAME: None, 
        config.CYCLE_GROUP_METRICS_TABLE_NAME: None
    }

    if df_ciclos_detalhe is None or df_ciclos_detalhe.empty:
        logger.warning("DataFrame de detalhes dos ciclos (df_ciclos_detalhe) está vazio ou nulo. Não é possível calcular métricas detalhadas.")
        return output_dfs
    if all_data_df is None or all_data_df.empty:
        logger.warning("DataFrame principal (all_data_df) está vazio ou nulo. Não é possível calcular métricas detalhadas de ciclo.")
        return output_dfs

    cols_to_check_for_closed = ['concurso_fim', 'duracao_concursos', 'ciclo_num', 'concurso_inicio']
    for col_check in cols_to_check_for_closed:
        if col_check not in df_ciclos_detalhe.columns:
            logger.error(f"Coluna '{col_check}' ausente em df_ciclos_detalhe. Não é possível calcular métricas detalhadas.")
            return output_dfs
        df_ciclos_detalhe[col_check] = pd.to_numeric(df_ciclos_detalhe[col_check], errors='coerce')

    df_closed_cycles = df_ciclos_detalhe[
        df_ciclos_detalhe['concurso_fim'].notna() & 
        df_ciclos_detalhe['duracao_concursos'].notna() & 
        (df_ciclos_detalhe['duracao_concursos'] > 0)
    ].copy()
    
    if df_closed_cycles.empty:
        logger.warning("Nenhum ciclo fechado encontrado para calcular métricas detalhadas.")
        return output_dfs
    
    contest_col = config.CONTEST_ID_COLUMN_NAME
    ball_cols = config.BALL_NUMBER_COLUMNS

    for _, cycle_row in df_closed_cycles.iterrows():
        try:
            ciclo_num = int(cycle_row['ciclo_num'])
            start_contest = int(cycle_row['concurso_inicio'])
            end_contest = int(cycle_row['concurso_fim'])
        except ValueError:
            logger.warning(f"Não foi possível converter informações do ciclo para int (ciclo_num: {cycle_row.get('ciclo_num', 'N/A')}). Pulando este ciclo.")
            continue

        df_current_cycle_contests_filtered = all_data_df.copy() 
        try:
            df_current_cycle_contests_filtered[contest_col] = pd.to_numeric(df_current_cycle_contests_filtered[contest_col], errors='coerce')
            df_current_cycle_contests_filtered.dropna(subset=[contest_col], inplace=True) 
            df_current_cycle_contests_filtered[contest_col] = df_current_cycle_contests_filtered[contest_col].astype(int)
        except Exception as e_conv_detail:
            logger.error(f"Erro ao converter {contest_col} para int no processamento do ciclo {ciclo_num}: {e_conv_detail}")
            continue 
        
        mask = (df_current_cycle_contests_filtered[contest_col] >= start_contest) & (df_current_cycle_contests_filtered[contest_col] <= end_contest)
        df_current_cycle_contests = df_current_cycle_contests_filtered[mask]

        chunk_duration = end_contest - start_contest + 1 
        default_freq_val = 0
        default_delay_float_val = float(chunk_duration) if chunk_duration > 0 else np.nan 
        default_delay_int_val = int(chunk_duration) if chunk_duration > 0 else pd.NA 
        default_rank_val = pd.NA 
        default_avg_group_val = np.nan 

        if df_current_cycle_contests.empty:
            logger.warning(f"Nenhum concurso encontrado para o ciclo {ciclo_num} (entre {start_contest} e {end_contest}). Usando valores padrão para métricas.")
            for d_val in config.ALL_NUMBERS:
                results_data_lists["frequency"].append({'ciclo_num': ciclo_num, 'dezena': d_val, 'frequencia_no_ciclo': default_freq_val})
                results_data_lists["mean_delay"].append({'ciclo_num': ciclo_num, 'dezena': d_val, 'atraso_medio_no_ciclo': default_delay_float_val})
                results_data_lists["max_delay"].append({'ciclo_num': ciclo_num, 'dezena': d_val, 'atraso_maximo_no_ciclo': default_delay_int_val})
                results_data_lists["final_delay"].append({'ciclo_num': ciclo_num, 'dezena': d_val, 'atraso_final_no_ciclo': default_delay_int_val})
                results_data_lists["rank_frequency"].append({'ciclo_num': ciclo_num, 'dezena': d_val, 'frequencia_no_ciclo': default_freq_val, 'rank_freq_no_ciclo': default_rank_val})
            results_data_lists["group_metrics_cycle"].append({'ciclo_num': ciclo_num, 'avg_pares_no_ciclo': default_avg_group_val, 'avg_impares_no_ciclo': default_avg_group_val, 'avg_primos_no_ciclo': default_avg_group_val})
            continue

        freq_series = calculate_frequency_in_chunk(df_current_cycle_contests, config) 
        
        temp_cycle_freq_data_for_rank: List[Dict[str, Any]] = []
        all_dezenas_in_config = set(config.ALL_NUMBERS)
        
        for d_item in all_dezenas_in_config: 
            v_item = freq_series.get(d_item, default_freq_val) 
            results_data_lists["frequency"].append({'ciclo_num': ciclo_num, 'dezena': int(d_item), 'frequencia_no_ciclo': int(v_item)})
            temp_cycle_freq_data_for_rank.append({'dezena': int(d_item), 'frequencia_no_ciclo': int(v_item)})

        if temp_cycle_freq_data_for_rank:
            df_temp_freq = pd.DataFrame(temp_cycle_freq_data_for_rank)
            if not df_temp_freq.empty and 'frequencia_no_ciclo' in df_temp_freq.columns:
                 df_temp_freq['rank_freq_no_ciclo'] = df_temp_freq['frequencia_no_ciclo'].rank(method='dense', ascending=False).astype('Int64')
                 for _, rank_row in df_temp_freq.iterrows():
                     results_data_lists["rank_frequency"].append({
                         'ciclo_num': ciclo_num, 
                         'dezena': int(rank_row['dezena']), 
                         'frequencia_no_ciclo': int(rank_row['frequencia_no_ciclo']),
                         'rank_freq_no_ciclo': rank_row['rank_freq_no_ciclo']
                     })
            else: 
                for d_val in all_dezenas_in_config:
                    results_data_lists["rank_frequency"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'frequencia_no_ciclo': default_freq_val, 'rank_freq_no_ciclo': default_rank_val})
        else: 
            for d_val in all_dezenas_in_config:
                results_data_lists["rank_frequency"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'frequencia_no_ciclo': default_freq_val, 'rank_freq_no_ciclo': default_rank_val})
        
        draw_matrix_cycle = get_draw_matrix_for_chunk(df_current_cycle_contests, start_contest, end_contest, config) 
        if not draw_matrix_cycle.empty:
            delay_metrics_dict = calculate_delays_for_matrix(draw_matrix_cycle, start_contest, end_contest, config)
            for d_val in all_dezenas_in_config: 
                results_data_lists["mean_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_medio_no_ciclo': float(delay_metrics_dict["mean"].get(d_val, np.nan)) if pd.notna(delay_metrics_dict["mean"].get(d_val, np.nan)) else default_delay_float_val})
                results_data_lists["max_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_maximo_no_ciclo': pd.NA if pd.isna(delay_metrics_dict["max"].get(d_val)) else int(delay_metrics_dict["max"].get(d_val, default_delay_int_val))})
                results_data_lists["final_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_final_no_ciclo': pd.NA if pd.isna(delay_metrics_dict["final"].get(d_val)) else int(delay_metrics_dict["final"].get(d_val, default_delay_int_val))})
        else: 
            for d_val in all_dezenas_in_config:
                results_data_lists["mean_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_medio_no_ciclo': default_delay_float_val})
                results_data_lists["max_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_maximo_no_ciclo': default_delay_int_val})
                results_data_lists["final_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_final_no_ciclo': default_delay_int_val})
            
        actual_ball_cols_for_props = [col for col in ball_cols if col in df_current_cycle_contests.columns]
        if not actual_ball_cols_for_props :
            results_data_lists["group_metrics_cycle"].append({'ciclo_num': ciclo_num, 'avg_pares_no_ciclo': default_avg_group_val, 'avg_impares_no_ciclo': default_avg_group_val, 'avg_primos_no_ciclo': default_avg_group_val})
        else:
            cycle_contest_properties_list: List[Dict[str, Any]] = []
            for _, contest_row_in_cycle in df_current_cycle_contests.iterrows():
                try:
                    draw_in_cycle = [int(x) for x in contest_row_in_cycle[actual_ball_cols_for_props].values if pd.notna(x)]
                    if len(draw_in_cycle) == config.NUMBERS_PER_DRAW: 
                        properties = analyze_draw_properties(draw_in_cycle, config) 
                        cycle_contest_properties_list.append(properties)
                    else:
                        logger.debug(f"Concurso {contest_row_in_cycle.get(contest_col, 'N/A')} no ciclo {ciclo_num} não tem {config.NUMBERS_PER_DRAW} dezenas válidas após limpeza. Pulando propriedades.")
                except ValueError: 
                    logger.warning(f"Erro ao converter dezenas do concurso {contest_row_in_cycle.get(contest_col, 'N/A')} no ciclo {ciclo_num} para análise de propriedades."); 
                    continue
            if cycle_contest_properties_list:
                 df_cycle_props = pd.DataFrame(cycle_contest_properties_list)
                 avg_pares = df_cycle_props['pares'].mean() if 'pares' in df_cycle_props and not df_cycle_props['pares'].dropna().empty else default_avg_group_val
                 avg_impares = df_cycle_props['impares'].mean() if 'impares' in df_cycle_props and not df_cycle_props['impares'].dropna().empty else default_avg_group_val
                 avg_primos = df_cycle_props['primos'].mean() if 'primos' in df_cycle_props and not df_cycle_props['primos'].dropna().empty else default_avg_group_val

                 results_data_lists["group_metrics_cycle"].append({
                    'ciclo_num': ciclo_num,
                    'avg_pares_no_ciclo': avg_pares,
                    'avg_impares_no_ciclo': avg_impares,
                    'avg_primos_no_ciclo': avg_primos
                })
            else: 
                results_data_lists["group_metrics_cycle"].append({'ciclo_num': ciclo_num, 'avg_pares_no_ciclo': default_avg_group_val, 'avg_impares_no_ciclo': default_avg_group_val, 'avg_primos_no_ciclo': default_avg_group_val})

    # Criar DataFrames a partir das listas de dicionários e atribuir aos nomes de tabela corretos
    # Os nomes das chaves em output_dfs JÁ SÃO os nomes das tabelas definidos no config.
    if results_data_lists["frequency"]: 
        output_dfs[config.CYCLE_METRIC_FREQUENCY_TABLE_NAME] = pd.DataFrame(results_data_lists["frequency"])
    if results_data_lists["mean_delay"]: 
        output_dfs[config.CYCLE_METRIC_ATRASO_MEDIO_TABLE_NAME] = pd.DataFrame(results_data_lists["mean_delay"])
    if results_data_lists["max_delay"]: 
        output_dfs[config.CYCLE_METRIC_ATRASO_MAXIMO_TABLE_NAME] = pd.DataFrame(results_data_lists["max_delay"])
    if results_data_lists["final_delay"]: 
        output_dfs[config.CYCLE_METRIC_ATRASO_FINAL_TABLE_NAME] = pd.DataFrame(results_data_lists["final_delay"])
    if results_data_lists["rank_frequency"]: 
        output_dfs[config.CYCLE_RANK_FREQUENCY_TABLE_NAME] = pd.DataFrame(results_data_lists["rank_frequency"])
    if results_data_lists["group_metrics_cycle"]: 
        output_dfs[config.CYCLE_GROUP_METRICS_TABLE_NAME] = pd.DataFrame(results_data_lists["group_metrics_cycle"])
        
    # Aplicar conversões de tipo para as colunas dos DataFrames gerados
    for key_df, df_metric in output_dfs.items(): # key_df é o nome da tabela
        if df_metric is not None and not df_metric.empty:
            if key_df == config.CYCLE_METRIC_FREQUENCY_TABLE_NAME:
                if 'frequencia_no_ciclo' in df_metric.columns: df_metric['frequencia_no_ciclo'] = pd.to_numeric(df_metric['frequencia_no_ciclo'], errors='coerce').astype('Int64')
            elif key_df == config.CYCLE_METRIC_ATRASO_MEDIO_TABLE_NAME:
                if 'atraso_medio_no_ciclo' in df_metric.columns: df_metric['atraso_medio_no_ciclo'] = pd.to_numeric(df_metric['atraso_medio_no_ciclo'], errors='coerce').astype('float')
            elif key_df == config.CYCLE_METRIC_ATRASO_MAXIMO_TABLE_NAME:
                if 'atraso_maximo_no_ciclo' in df_metric.columns: df_metric['atraso_maximo_no_ciclo'] = pd.to_numeric(df_metric['atraso_maximo_no_ciclo'], errors='coerce').astype('Int64')
            elif key_df == config.CYCLE_METRIC_ATRASO_FINAL_TABLE_NAME:
                if 'atraso_final_no_ciclo' in df_metric.columns: df_metric['atraso_final_no_ciclo'] = pd.to_numeric(df_metric['atraso_final_no_ciclo'], errors='coerce').astype('Int64')
            elif key_df == config.CYCLE_RANK_FREQUENCY_TABLE_NAME:
                if 'rank_freq_no_ciclo' in df_metric.columns: df_metric['rank_freq_no_ciclo'] = pd.to_numeric(df_metric['rank_freq_no_ciclo'], errors='coerce').astype('Int64')
                if 'frequencia_no_ciclo' in df_metric.columns: df_metric['frequencia_no_ciclo'] = pd.to_numeric(df_metric['frequencia_no_ciclo'], errors='coerce').astype('Int64')
            elif key_df == config.CYCLE_GROUP_METRICS_TABLE_NAME:
                for col_group in ['avg_pares_no_ciclo', 'avg_impares_no_ciclo', 'avg_primos_no_ciclo']:
                    if col_group in df_metric.columns:
                        df_metric[col_group] = pd.to_numeric(df_metric[col_group], errors='coerce').astype('float')
            
            if config.CICLO_NUM_COLUMN_NAME in df_metric.columns:
                df_metric[config.CICLO_NUM_COLUMN_NAME] = pd.to_numeric(df_metric[config.CICLO_NUM_COLUMN_NAME], errors='coerce').astype('Int64')
            if config.DEZENA_COLUMN_NAME in df_metric.columns:
                 df_metric[config.DEZENA_COLUMN_NAME] = pd.to_numeric(df_metric[config.DEZENA_COLUMN_NAME], errors='coerce').astype('Int64')
            output_dfs[key_df] = df_metric

    logger.info("Cálculo de métricas detalhadas por dezena/ciclo concluído.")
    return output_dfs

# Wrappers
def identify_cycles(all_data_df: pd.DataFrame, config: Any) -> Optional[pd.DataFrame]:
    logger.info("Chamando identify_cycles (wrapper).")
    results_dict = identify_and_process_cycles(all_data_df, config)
    return results_dict.get(KEY_CYCLE_DETAILS_DF) 

def calculate_cycle_stats(df_cycles_detail: Optional[pd.DataFrame], config: Any) -> Optional[pd.DataFrame]:
    logger.info("Chamando calculate_cycle_stats (wrapper).")
    if df_cycles_detail is None or df_cycles_detail.empty:
        logger.warning("df_cycles_detail fornecido para calculate_cycle_stats está vazio ou nulo.")
        return None

    if 'duracao_concursos' not in df_cycles_detail.columns:
        logger.error("Coluna 'duracao_concursos' ausente em df_cycles_detail para calculate_cycle_stats.")
        return None
        
    df_closed_cycles = df_cycles_detail[pd.to_numeric(df_cycles_detail['duracao_concursos'], errors='coerce').notna()].copy()
    if not df_closed_cycles.empty: 
        df_closed_cycles['duracao_concursos'] = pd.to_numeric(df_closed_cycles['duracao_concursos'])
        if not df_closed_cycles.empty : 
            summary_stats = {
                'total_ciclos_fechados': int(len(df_closed_cycles)),
                'duracao_media_ciclo': float(df_closed_cycles['duracao_concursos'].mean()) if len(df_closed_cycles) > 0 else np.nan,
                'duracao_min_ciclo': int(df_closed_cycles['duracao_concursos'].min()) if len(df_closed_cycles) > 0 else pd.NA,
                'duracao_max_ciclo': int(df_closed_cycles['duracao_concursos'].max()) if len(df_closed_cycles) > 0 else pd.NA,
                'duracao_mediana_ciclo': float(df_closed_cycles['duracao_concursos'].median()) if len(df_closed_cycles) > 0 else np.nan
            }
            df_summary = pd.DataFrame([summary_stats])
            for col_int_sum in ['total_ciclos_fechados', 'duracao_min_ciclo', 'duracao_max_ciclo']:
                 if col_int_sum in df_summary.columns:
                     df_summary[col_int_sum] = pd.to_numeric(df_summary[col_int_sum], errors='coerce').astype('Int64')
            return df_summary
    else:
        logger.warning("Nenhum ciclo fechado encontrado em df_cycles_detail para calculate_cycle_stats.")
    return None