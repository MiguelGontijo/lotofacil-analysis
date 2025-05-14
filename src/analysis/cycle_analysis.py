# src/analysis/cycle_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Set, Optional, Any
import logging
import numpy as np

# from src.config import ALL_NUMBERS # Acessado via config
from src.analysis.chunk_analysis import (
    calculate_frequency_in_chunk, # Já espera config
    get_draw_matrix_for_chunk,    # Já espera config
    calculate_delays_for_matrix   # Já espera config
)
from src.analysis.number_properties_analysis import analyze_draw_properties 

logger = logging.getLogger(__name__)
# ALL_NUMBERS_SET será definido usando config.ALL_NUMBERS quando config estiver disponível

def identify_and_process_cycles(all_data_df: pd.DataFrame, config: Any) -> Dict[str, Optional[pd.DataFrame]]:
    logger.info("Iniciando análise completa de ciclos.")
    results: Dict[str, Optional[pd.DataFrame]] = {'ciclos_detalhe': None, 'ciclos_sumario_estatisticas': None}
    if all_data_df is None or all_data_df.empty: # ... (como antes)
        return results

    contest_col = config.CONTEST_ID_COLUMN_NAME
    ball_cols = config.BALL_NUMBER_COLUMNS
    current_all_numbers_set = set(config.ALL_NUMBERS)

    required_cols = [contest_col] + ball_cols
    missing_cols = [col for col in required_cols if col not in all_data_df.columns]
    if missing_cols: # ... (como antes)
        return results

    df_sorted = all_data_df.copy()
    try:
        df_sorted[contest_col] = pd.to_numeric(df_sorted[contest_col])
    except Exception as e: # ... (como antes)
        return results
    df_sorted = df_sorted.sort_values(by=contest_col).reset_index(drop=True)
    # ... (resto da lógica de identify_and_process_cycles como na sua versão mais recente/corrigida anteriormente) ...
    # ... garantindo que use config.CONTEST_ID_COLUMN_NAME, config.BALL_NUMBER_COLUMNS, config.ALL_NUMBERS ...
    cycles_data: List[Dict[str, Any]] = []
    current_cycle_numbers_needed = current_all_numbers_set.copy()
    current_cycle_start_contest = int(df_sorted.loc[0, contest_col]) if not df_sorted.empty else 0
    cycle_count = 0
    last_processed_contest_num_for_open_cycle = int(df_sorted[contest_col].max()) if not df_sorted.empty else 0
    for index, row in df_sorted.iterrows():
        contest_number = int(row[contest_col])
        try:
            drawn_numbers_in_this_contest = set(int(row[col_b]) for col_b in ball_cols if pd.notna(row[col_b]))
        except ValueError: logger.warning(f"Erro converter dezenas concurso {contest_number}."); continue
        if current_cycle_numbers_needed == current_all_numbers_set and contest_number != current_cycle_start_contest:
             current_cycle_start_contest = contest_number
        current_cycle_numbers_needed.difference_update(drawn_numbers_in_this_contest)
        if not current_cycle_numbers_needed: 
            cycle_count += 1
            cycles_data.append({'ciclo_num': cycle_count, 'concurso_inicio': current_cycle_start_contest, 'concurso_fim': contest_number, 'duracao_concursos': contest_number - current_cycle_start_contest + 1, 'numeros_faltantes': None, 'qtd_faltantes': 0})
            current_cycle_numbers_needed = current_all_numbers_set.copy()
            if index + 1 < len(df_sorted): current_cycle_start_contest = int(df_sorted.loc[index + 1, contest_col])
            else: current_cycle_start_contest = contest_number + 1
    if current_cycle_numbers_needed and current_cycle_numbers_needed != current_all_numbers_set:
        if not df_sorted.empty and current_cycle_start_contest <= last_processed_contest_num_for_open_cycle:
            cycles_data.append({'ciclo_num': cycle_count + 1, 'concurso_inicio': current_cycle_start_contest, 'concurso_fim': pd.NA, 'duracao_concursos': pd.NA, 'numeros_faltantes': ",".join(map(str, sorted(list(current_cycle_numbers_needed)))), 'qtd_faltantes': len(current_cycle_numbers_needed)})
    elif cycle_count == 0 and not df_sorted.empty and current_cycle_numbers_needed and current_cycle_numbers_needed != current_all_numbers_set : 
            cycles_data.append({'ciclo_num': 1, 'concurso_inicio': current_cycle_start_contest, 'concurso_fim': pd.NA, 'duracao_concursos': pd.NA, 'numeros_faltantes': ",".join(map(str, sorted(list(current_cycle_numbers_needed)))), 'qtd_faltantes': len(current_cycle_numbers_needed)})
    if cycles_data:
        df_cycles_detail = pd.DataFrame(cycles_data)
        for col_int in ['concurso_fim', 'duracao_concursos', 'qtd_faltantes', 'ciclo_num', 'concurso_inicio']:
            if col_int in df_cycles_detail.columns: df_cycles_detail[col_int] = pd.to_numeric(df_cycles_detail[col_int], errors='coerce').astype('Int64')
        results['ciclos_detalhe'] = df_cycles_detail
        if 'duracao_concursos' in df_cycles_detail.columns:
            df_closed_cycles = df_cycles_detail[df_cycles_detail['duracao_concursos'].notna()].copy()
            if not df_closed_cycles.empty: 
                df_closed_cycles['duracao_concursos'] = pd.to_numeric(df_closed_cycles['duracao_concursos'])
                summary_stats_data = {'total_ciclos_fechados': int(len(df_closed_cycles)), 'duracao_media_ciclo': float(df_closed_cycles['duracao_concursos'].mean()) if len(df_closed_cycles) > 0 else np.nan, 'duracao_min_ciclo': int(df_closed_cycles['duracao_concursos'].min()) if len(df_closed_cycles) > 0 else pd.NA, 'duracao_max_ciclo': int(df_closed_cycles['duracao_concursos'].max()) if len(df_closed_cycles) > 0 else pd.NA, 'duracao_mediana_ciclo': float(df_closed_cycles['duracao_concursos'].median()) if len(df_closed_cycles) > 0 else np.nan}
                df_summary = pd.DataFrame([summary_stats_data])
                for col_int_sum in ['total_ciclos_fechados', 'duracao_min_ciclo', 'duracao_max_ciclo']:
                     if col_int_sum in df_summary.columns: df_summary[col_int_sum] = df_summary[col_int_sum].astype('Int64')
                results['ciclos_sumario_estatisticas'] = df_summary
    logger.info("Análise de identificação de ciclos e sumário concluída.")
    return results

def calculate_detailed_metrics_per_closed_cycle(
    all_data_df: pd.DataFrame, 
    df_ciclos_detalhe: Optional[pd.DataFrame],
    config: Any 
) -> Dict[str, Optional[pd.DataFrame]]:
    logger.info("Iniciando cálculo de métricas detalhadas por dezena/ciclo.")
    results_data_lists: Dict[str, List[Dict[str, Any]]] = {"frequency": [], "mean_delay": [], "max_delay": [], "final_delay": [], "rank_frequency": [], "group_metrics_cycle": []}
    output_dfs: Dict[str, Optional[pd.DataFrame]] = {"ciclo_metric_frequency": None, "ciclo_metric_atraso_medio": None, "ciclo_metric_atraso_maximo": None, "ciclo_metric_atraso_final": None, "ciclo_rank_frequency": None, "ciclo_group_metrics": None}

    if df_ciclos_detalhe is None or df_ciclos_detalhe.empty: return output_dfs
    if all_data_df is None or all_data_df.empty: return output_dfs

    df_closed_cycles = df_ciclos_detalhe[df_ciclos_detalhe['concurso_fim'].notna() & df_ciclos_detalhe['duracao_concursos'].notna() & (pd.to_numeric(df_ciclos_detalhe['duracao_concursos'], errors='coerce') > 0)].copy()
    if df_closed_cycles.empty: return output_dfs
    
    contest_col = config.CONTEST_ID_COLUMN_NAME
    ball_cols = config.BALL_NUMBER_COLUMNS

    for _, cycle_row in df_closed_cycles.iterrows():
        ciclo_num = int(cycle_row['ciclo_num'])
        start_contest = int(cycle_row['concurso_inicio'])
        end_contest = int(cycle_row['concurso_fim'])

        df_current_cycle_contests_filtered = all_data_df.copy()
        try:
            df_current_cycle_contests_filtered[contest_col] = pd.to_numeric(df_current_cycle_contests_filtered[contest_col], errors='coerce')
            df_current_cycle_contests_filtered.dropna(subset=[contest_col], inplace=True)
            df_current_cycle_contests_filtered[contest_col] = df_current_cycle_contests_filtered[contest_col].astype(int)
        except Exception as e_conv_detail:
            logger.error(f"Erro ao converter {contest_col} para int no ciclo {ciclo_num}: {e_conv_detail}")
            continue
        
        mask = (df_current_cycle_contests_filtered[contest_col] >= start_contest) & (df_current_cycle_contests_filtered[contest_col] <= end_contest)
        df_current_cycle_contests = df_current_cycle_contests_filtered[mask]

        chunk_duration = end_contest - start_contest + 1
        default_freq_val = 0; default_delay_float_val = float(chunk_duration); default_delay_int_val = int(chunk_duration); default_rank_val = np.nan; default_avg_group_val = np.nan

        if df_current_cycle_contests.empty: # ... (lógica de defaults como antes) ...
            for d_val in config.ALL_NUMBERS:
                results_data_lists["frequency"].append({'ciclo_num': ciclo_num, 'dezena': d_val, 'frequencia_no_ciclo': default_freq_val})
                results_data_lists["mean_delay"].append({'ciclo_num': ciclo_num, 'dezena': d_val, 'atraso_medio_no_ciclo': default_delay_float_val})
                # ... etc ...
            results_data_lists["group_metrics_cycle"].append({'ciclo_num': ciclo_num, 'avg_pares_no_ciclo': default_avg_group_val, 'avg_impares_no_ciclo': default_avg_group_val, 'avg_primos_no_ciclo': default_avg_group_val})
            continue

        # CORRIGIDO: Passa config para as funções de chunk_analysis
        freq_series = calculate_frequency_in_chunk(df_current_cycle_contests, config) 
        # ... (lógica de rank e freq como antes) ...
        temp_cycle_freq_data_for_rank: List[Dict[str, Any]] = []
        for d_item, v_item in freq_series.items():
            results_data_lists["frequency"].append({'ciclo_num': ciclo_num, 'dezena': int(d_item), 'frequencia_no_ciclo': int(v_item)})
            temp_cycle_freq_data_for_rank.append({'dezena': int(d_item), 'frequencia_no_ciclo': int(v_item)})
        if temp_cycle_freq_data_for_rank:
            df_temp_freq = pd.DataFrame(temp_cycle_freq_data_for_rank)
            if not df_temp_freq.empty and 'frequencia_no_ciclo' in df_temp_freq.columns:
                 df_temp_freq['rank_freq_no_ciclo'] = df_temp_freq['frequencia_no_ciclo'].rank(method='dense', ascending=False).astype(int)
                 for _, rank_row in df_temp_freq.iterrows(): results_data_lists["rank_frequency"].append({'ciclo_num': ciclo_num, 'dezena': int(rank_row['dezena']), 'frequencia_no_ciclo': int(rank_row['frequencia_no_ciclo']), 'rank_freq_no_ciclo': int(rank_row['rank_freq_no_ciclo'])})
            else: 
                for d_val in config.ALL_NUMBERS: results_data_lists["rank_frequency"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'frequencia_no_ciclo': 0, 'rank_freq_no_ciclo': default_rank_val})
        else: 
            for d_val in config.ALL_NUMBERS: results_data_lists["rank_frequency"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'frequencia_no_ciclo': 0, 'rank_freq_no_ciclo': default_rank_val})
        
        draw_matrix_cycle = get_draw_matrix_for_chunk(df_current_cycle_contests, start_contest, end_contest, config) 
        if not draw_matrix_cycle.empty:
            delay_metrics_dict = calculate_delays_for_matrix(draw_matrix_cycle, start_contest, end_contest, config)
            for d_val in config.ALL_NUMBERS: 
                results_data_lists["mean_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_medio_no_ciclo': float(delay_metrics_dict["mean"].get(d_val, np.nan)) if pd.notna(delay_metrics_dict["mean"].get(d_val, np.nan)) else None})
                results_data_lists["max_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_maximo_no_ciclo': int(delay_metrics_dict["max"].get(d_val, default_delay_int_val))})
                results_data_lists["final_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_final_no_ciclo': int(delay_metrics_dict["final"].get(d_val, default_delay_int_val))})
        else: 
            for d_val in config.ALL_NUMBERS: # ... (defaults como antes)
                results_data_lists["mean_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_medio_no_ciclo': default_delay_float_val})
                # ... etc ...
            
        actual_ball_cols_for_props = [col for col in ball_cols if col in df_current_cycle_contests.columns]
        if not actual_ball_cols_for_props :
            results_data_lists["group_metrics_cycle"].append({'ciclo_num': ciclo_num, 'avg_pares_no_ciclo': default_avg_group_val, 'avg_impares_no_ciclo': default_avg_group_val, 'avg_primos_no_ciclo': default_avg_group_val})
        else:
            cycle_contest_properties_list: List[Dict[str, Any]] = []
            for _, contest_row_in_cycle in df_current_cycle_contests.iterrows():
                try:
                    draw_in_cycle = [int(contest_row_in_cycle[col]) for col in actual_ball_cols_for_props if pd.notna(contest_row_in_cycle[col])]
                    if len(draw_in_cycle) == config.NUMBERS_PER_DRAW: 
                        properties = analyze_draw_properties(draw_in_cycle, config) 
                        cycle_contest_properties_list.append(properties)
                except ValueError: logger.warning(f"Erro converter dezenas concurso {contest_row_in_cycle.get(contest_col, 'Desconhecido')} ciclo {ciclo_num}."); continue
            if cycle_contest_properties_list: # ... (lógica como antes)
                 df_cycle_props = pd.DataFrame(cycle_contest_properties_list)
                 results_data_lists["group_metrics_cycle"].append({
                    'ciclo_num': ciclo_num,
                    'avg_pares_no_ciclo': df_cycle_props['pares'].mean() if 'pares' in df_cycle_props and not df_cycle_props['pares'].empty else default_avg_group_val,
                    'avg_impares_no_ciclo': df_cycle_props['impares'].mean() if 'impares' in df_cycle_props and not df_cycle_props['impares'].empty else default_avg_group_val,
                    'avg_primos_no_ciclo': df_cycle_props['primos'].mean() if 'primos' in df_cycle_props and not df_cycle_props['primos'].empty else default_avg_group_val
                })
            else: results_data_lists["group_metrics_cycle"].append({'ciclo_num': ciclo_num, 'avg_pares_no_ciclo': default_avg_group_val, 'avg_impares_no_ciclo': default_avg_group_val, 'avg_primos_no_ciclo': default_avg_group_val})

    if results_data_lists["frequency"]: output_dfs["ciclo_metric_frequency"] = pd.DataFrame(results_data_lists["frequency"])
    if results_data_lists["mean_delay"]: output_dfs["ciclo_metric_atraso_medio"] = pd.DataFrame(results_data_lists["mean_delay"])
    if results_data_lists["max_delay"]: output_dfs["ciclo_metric_atraso_maximo"] = pd.DataFrame(results_data_lists["max_delay"])
    if results_data_lists["final_delay"]: output_dfs["ciclo_metric_atraso_final"] = pd.DataFrame(results_data_lists["final_delay"])
    if results_data_lists["rank_frequency"]: output_dfs["ciclo_rank_frequency"] = pd.DataFrame(results_data_lists["rank_frequency"])
    if results_data_lists["group_metrics_cycle"]: output_dfs["ciclo_group_metrics"] = pd.DataFrame(results_data_lists["group_metrics_cycle"])
        
    logger.info("Cálculo de métricas detalhadas por dezena/ciclo concluído.")
    return output_dfs

# Wrappers
def identify_cycles(all_data_df: pd.DataFrame, config: Any) -> Optional[pd.DataFrame]:
    logger.info("Chamando identify_cycles (wrapper).")
    results_dict = identify_and_process_cycles(all_data_df, config)
    return results_dict.get('ciclos_detalhe')

def calculate_cycle_stats(df_cycles_detail: Optional[pd.DataFrame], config: Any) -> Optional[pd.DataFrame]:
    logger.info("Chamando calculate_cycle_stats (wrapper).")
    # Esta função pode ser apenas para pegar o sumário do dict retornado por identify_and_process_cycles
    # ou recalcular com base no df_cycles_detail se necessário.
    # Se identify_and_process_cycles já retorna o sumário, o step cycle_stats pode pegar de lá.
    # Vou assumir que ele pega o sumário que já foi calculado.
    if df_cycles_detail is None or df_cycles_detail.empty: return None

    # Re-calcula o sumário a partir dos detalhes do ciclo
    # (Esta lógica está duplicada de identify_and_process_cycles, idealmente seria uma função separada)
    if 'duracao_concursos' not in df_cycles_detail.columns: return None
    df_closed_cycles = df_cycles_detail[pd.to_numeric(df_cycles_detail['duracao_concursos'], errors='coerce').notna()].copy()
    if not df_closed_cycles.empty: 
        df_closed_cycles['duracao_concursos'] = pd.to_numeric(df_closed_cycles['duracao_concursos'])
        if not df_closed_cycles.empty : 
            summary_stats = {'total_ciclos_fechados': int(len(df_closed_cycles)), 'duracao_media_ciclo': float(df_closed_cycles['duracao_concursos'].mean()) if len(df_closed_cycles) > 0 else np.nan, 'duracao_min_ciclo': int(df_closed_cycles['duracao_concursos'].min()) if len(df_closed_cycles) > 0 else pd.NA, 'duracao_max_ciclo': int(df_closed_cycles['duracao_concursos'].max()) if len(df_closed_cycles) > 0 else pd.NA, 'duracao_mediana_ciclo': float(df_closed_cycles['duracao_concursos'].median()) if len(df_closed_cycles) > 0 else np.nan}
            df_summary = pd.DataFrame([summary_stats])
            for col_int_sum in ['total_ciclos_fechados', 'duracao_min_ciclo', 'duracao_max_ciclo']:
                 if col_int_sum in df_summary.columns: df_summary[col_int_sum] = df_summary[col_int_sum].astype('Int64')
            return df_summary
    return None