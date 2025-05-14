# src/analysis/cycle_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Set, Optional, Any
import logging
import numpy as np

from src.config import ALL_NUMBERS
from src.analysis.chunk_analysis import (
    calculate_frequency_in_chunk,
    get_draw_matrix_for_chunk,
    calculate_delays_for_matrix
)
# Importar a função de análise de propriedades do sorteio para métricas de grupo
from src.analysis.number_properties_analysis import analyze_draw_properties, PRIMES_UP_TO_25
# Garantir que PRIMES_UP_TO_25 seja acessível se analyze_draw_properties depender dele como global
# Se analyze_draw_properties for auto-suficiente ou receber PRIMES_UP_TO_25 como arg, melhor.

logger = logging.getLogger(__name__)
ALL_NUMBERS_SET: Set[int] = set(ALL_NUMBERS)

# --- Funções de Identificação de Ciclo (mantidas como na última versão funcional) ---
def identify_and_process_cycles(all_data_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    logger.info("Iniciando análise completa de ciclos (Versão Refatorada).")
    if all_data_df is None or all_data_df.empty:
        logger.warning("DataFrame de entrada para análise de ciclos está vazio.")
        return {}

    required_cols = ['Concurso'] + [f'bola_{i}' for i in range(1, 16)]
    missing_cols = [col for col in required_cols if col not in all_data_df.columns]
    if missing_cols:
        logger.error(f"Colunas essenciais ausentes no DataFrame: {missing_cols}. Não é possível analisar ciclos.")
        return {}

    df_sorted = all_data_df.sort_values(by='Concurso').reset_index(drop=True)
    dezena_cols_bola = [f'bola_{i}' for i in range(1, 16)] 
    cycles_data: List[Dict[str, Any]] = []
    current_cycle_numbers_needed = ALL_NUMBERS_SET.copy()
    current_cycle_start_contest = df_sorted.loc[0, 'Concurso'] if not df_sorted.empty else 0
    cycle_count = 0
    last_processed_contest_num_for_open_cycle = df_sorted['Concurso'].max() if not df_sorted.empty else 0
    
    for index, row in df_sorted.iterrows():
        contest_number = int(row['Concurso'])
        try:
            drawn_numbers_in_this_contest = set()
            for col_b in dezena_cols_bola: 
                if pd.notna(row[col_b]): drawn_numbers_in_this_contest.add(int(row[col_b]))
            if not drawn_numbers_in_this_contest and len(dezena_cols_bola) > 0: continue
        except ValueError: 
            logger.warning(f"Erro converter dezenas concurso {contest_number}."); continue
        
        if current_cycle_numbers_needed == ALL_NUMBERS_SET and contest_number != current_cycle_start_contest:
             current_cycle_start_contest = contest_number
        
        current_cycle_numbers_needed.difference_update(drawn_numbers_in_this_contest)
        
        if not current_cycle_numbers_needed: 
            cycle_count += 1
            cycles_data.append({'ciclo_num': cycle_count, 'concurso_inicio': current_cycle_start_contest, 'concurso_fim': contest_number, 'duracao_concursos': contest_number - current_cycle_start_contest + 1, 'numeros_faltantes': None, 'qtd_faltantes': 0})
            current_cycle_numbers_needed = ALL_NUMBERS_SET.copy()
            if index + 1 < len(df_sorted): 
                current_cycle_start_contest = df_sorted.loc[index + 1, 'Concurso']
            
    if current_cycle_numbers_needed and current_cycle_numbers_needed != ALL_NUMBERS_SET:
        if not df_sorted.empty and current_cycle_start_contest <= last_processed_contest_num_for_open_cycle:
            cycles_data.append({'ciclo_num': cycle_count + 1, 'concurso_inicio': current_cycle_start_contest, 'concurso_fim': np.nan, 'duracao_concursos': np.nan, 'numeros_faltantes': ",".join(map(str, sorted(list(current_cycle_numbers_needed)))), 'qtd_faltantes': len(current_cycle_numbers_needed)})
    elif cycle_count == 0 and not df_sorted.empty and current_cycle_numbers_needed: 
        if current_cycle_numbers_needed != ALL_NUMBERS_SET: 
            cycles_data.append({'ciclo_num': 1, 'concurso_inicio': current_cycle_start_contest, 'concurso_fim': np.nan, 'duracao_concursos': np.nan, 'numeros_faltantes': ",".join(map(str, sorted(list(current_cycle_numbers_needed)))), 'qtd_faltantes': len(current_cycle_numbers_needed)})

    results = {}
    if cycles_data:
        df_cycles_detail = pd.DataFrame(cycles_data)
        if 'concurso_fim' in df_cycles_detail.columns: df_cycles_detail['concurso_fim'] = df_cycles_detail['concurso_fim'].astype('Int64')
        if 'duracao_concursos' in df_cycles_detail.columns: df_cycles_detail['duracao_concursos'] = df_cycles_detail['duracao_concursos'].astype('Int64')
        if 'numeros_faltantes' in df_cycles_detail.columns:
            df_cycles_detail['numeros_faltantes'] = df_cycles_detail['numeros_faltantes'].apply(lambda x: ",".join(map(str, sorted(list(x)))) if isinstance(x, (set, list)) else x).fillna(value=np.nan).replace([pd.NA], [None])
        results['ciclos_detalhe'] = df_cycles_detail
        if 'duracao_concursos' in df_cycles_detail.columns:
            df_closed_cycles = df_cycles_detail[pd.to_numeric(df_cycles_detail['duracao_concursos'], errors='coerce').notna()].copy()
            if not df_closed_cycles.empty and 'duracao_concursos' in df_closed_cycles.columns: 
                df_closed_cycles['duracao_concursos'] = pd.to_numeric(df_closed_cycles['duracao_concursos'])
                if not df_closed_cycles.empty:
                    summary_stats = {'total_ciclos_fechados': int(len(df_closed_cycles)), 
                                     'duracao_media_ciclo': float(df_closed_cycles['duracao_concursos'].mean()) if len(df_closed_cycles) > 0 else np.nan, 
                                     'duracao_min_ciclo': int(df_closed_cycles['duracao_concursos'].min()) if len(df_closed_cycles) > 0 else np.nan, 
                                     'duracao_max_ciclo': int(df_closed_cycles['duracao_concursos'].max()) if len(df_closed_cycles) > 0 else np.nan, 
                                     'duracao_mediana_ciclo': float(df_closed_cycles['duracao_concursos'].median()) if len(df_closed_cycles) > 0 else np.nan}
                    results['ciclos_sumario_estatisticas'] = pd.DataFrame([summary_stats])
    logger.info("Análise de identificação de ciclos e sumário concluída.")
    return results


# --- MÉTRICAS DETALHADAS POR CICLO FECHADO (MODIFICADA PARA INCLUIR MÉTRICAS DE GRUPO) ---
def calculate_detailed_metrics_per_closed_cycle(
    all_data_df: pd.DataFrame, 
    df_ciclos_detalhe: Optional[pd.DataFrame]
) -> Dict[str, Optional[pd.DataFrame]]:
    logger.info("Iniciando cálculo de métricas detalhadas (freq, atrasos, rank, grupos) por dezena/ciclo para cada ciclo fechado.")
    
    results_data_lists: Dict[str, List[Dict[str, Any]]] = {
        "frequency": [], "mean_delay": [], "max_delay": [], "final_delay": [],
        "rank_frequency": [],
        "group_metrics_cycle": [] # <<< NOVA LISTA PARA MÉTRICAS DE GRUPO POR CICLO
    }
    output_dfs: Dict[str, Optional[pd.DataFrame]] = {
        "ciclo_metric_frequency": None, "ciclo_metric_atraso_medio": None,
        "ciclo_metric_atraso_maximo": None, "ciclo_metric_atraso_final": None,
        "ciclo_rank_frequency": None,
        "ciclo_group_metrics": None # <<< NOVA ENTRADA PARA DF DE MÉTRICAS DE GRUPO
    }

    if df_ciclos_detalhe is None or df_ciclos_detalhe.empty:
        logger.warning("DataFrame 'df_ciclos_detalhe' vazio. Nenhuma métrica por ciclo calculada.")
        return output_dfs
    if all_data_df is None or all_data_df.empty:
        logger.warning("DataFrame 'all_data_df' vazio. Nenhuma métrica por ciclo calculada.")
        return output_dfs

    df_closed_cycles = df_ciclos_detalhe[
        df_ciclos_detalhe['concurso_fim'].notna() & 
        df_ciclos_detalhe['duracao_concursos'].notna() &
        (pd.to_numeric(df_ciclos_detalhe['duracao_concursos'], errors='coerce') > 0)
    ].copy()

    if df_closed_cycles.empty:
        logger.info("Nenhum ciclo fechado em 'df_ciclos_detalhe' para cálculo de métricas.")
        return output_dfs
    
    logger.debug(f"Processando métricas detalhadas para {len(df_closed_cycles)} ciclos fechados.")
    dezena_cols_ciclo = [f'bola_{i}' for i in range(1, 16)] 

    for _, cycle_row in df_closed_cycles.iterrows():
        ciclo_num = int(cycle_row['ciclo_num'])
        start_contest = int(cycle_row['concurso_inicio'])
        end_contest = int(cycle_row['concurso_fim'])

        mask = (all_data_df['Concurso'] >= start_contest) & (all_data_df['Concurso'] <= end_contest)
        df_current_cycle_contests = all_data_df[mask]

        chunk_duration = end_contest - start_contest + 1
        default_freq_val = 0
        default_delay_float_val = float(chunk_duration)
        default_delay_int_val = int(chunk_duration)
        default_rank_val = np.nan 
        default_avg_group_val = np.nan

        if df_current_cycle_contests.empty:
            logger.warning(f"Ciclo {ciclo_num} (C{start_contest}-C{end_contest}) não tem concursos. Usando defaults.")
            for d_val in ALL_NUMBERS:
                results_data_lists["frequency"].append({'ciclo_num': ciclo_num, 'dezena': d_val, 'frequencia_no_ciclo': default_freq_val})
                results_data_lists["mean_delay"].append({'ciclo_num': ciclo_num, 'dezena': d_val, 'atraso_medio_no_ciclo': default_delay_float_val})
                results_data_lists["max_delay"].append({'ciclo_num': ciclo_num, 'dezena': d_val, 'atraso_maximo_no_ciclo': default_delay_int_val})
                results_data_lists["final_delay"].append({'ciclo_num': ciclo_num, 'dezena': d_val, 'atraso_final_no_ciclo': default_delay_int_val})
                results_data_lists["rank_frequency"].append({'ciclo_num': ciclo_num, 'dezena': d_val, 'frequencia_no_ciclo': default_freq_val, 'rank_freq_no_ciclo': default_rank_val})
            results_data_lists["group_metrics"].append({'ciclo_num': ciclo_num, 'avg_pares_no_ciclo': default_avg_group_val, 'avg_impares_no_ciclo': default_avg_group_val, 'avg_primos_no_ciclo': default_avg_group_val})
            continue

        # 1. Frequência no Ciclo
        freq_series = calculate_frequency_in_chunk(df_current_cycle_contests)
        temp_cycle_freq_data_for_rank: List[Dict[str, Any]] = []
        for d, v in freq_series.items():
            results_data_lists["frequency"].append({'ciclo_num': ciclo_num, 'dezena': int(d), 'frequencia_no_ciclo': int(v)})
            temp_cycle_freq_data_for_rank.append({'dezena': int(d), 'frequencia_no_ciclo': int(v)})
        
        # 2. Rank da Frequência no Ciclo
        if temp_cycle_freq_data_for_rank:
            df_temp_freq = pd.DataFrame(temp_cycle_freq_data_for_rank)
            if not df_temp_freq.empty and 'frequencia_no_ciclo' in df_temp_freq.columns:
                 df_temp_freq['rank_freq_no_ciclo'] = df_temp_freq['frequencia_no_ciclo'].rank(method='dense', ascending=False).astype(int)
                 for _, rank_row in df_temp_freq.iterrows(): results_data_lists["rank_frequency"].append({'ciclo_num': ciclo_num, 'dezena': int(rank_row['dezena']), 'frequencia_no_ciclo': int(rank_row['frequencia_no_ciclo']), 'rank_freq_no_ciclo': int(rank_row['rank_freq_no_ciclo'])})
            else: 
                for d_val in ALL_NUMBERS: results_data_lists["rank_frequency"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'frequencia_no_ciclo': 0, 'rank_freq_no_ciclo': default_rank_val})
        else: 
            for d_val in ALL_NUMBERS: results_data_lists["rank_frequency"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'frequencia_no_ciclo': 0, 'rank_freq_no_ciclo': default_rank_val})
        
        # 3. Atrasos no Ciclo
        draw_matrix_cycle = get_draw_matrix_for_chunk(df_current_cycle_contests, start_contest, end_contest)
        if not draw_matrix_cycle.empty:
            delay_metrics_dict = calculate_delays_for_matrix(draw_matrix_cycle, start_contest, end_contest)
            for d, v in delay_metrics_dict["mean"].items(): results_data_lists["mean_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d), 'atraso_medio_no_ciclo': float(v) if pd.notna(v) else None})
            for d, v in delay_metrics_dict["max"].items(): results_data_lists["max_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d), 'atraso_maximo_no_ciclo': int(v)})
            for d, v in delay_metrics_dict["final"].items(): results_data_lists["final_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d), 'atraso_final_no_ciclo': int(v)})
        else:
            for d_val in ALL_NUMBERS:
                results_data_lists["mean_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_medio_no_ciclo': default_delay_float_val})
                results_data_lists["max_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_maximo_no_ciclo': default_delay_int_val})
                results_data_lists["final_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_final_no_ciclo': default_delay_int_val})

        # 4. Métricas de Grupo para o Ciclo (média por concurso dentro do ciclo)
        actual_dezena_cols_for_props = [col for col in dezena_cols_ciclo if col in df_current_cycle_contests.columns]
        if not actual_dezena_cols_for_props :
            results_data_lists["group_metrics"].append({'ciclo_num': ciclo_num, 'avg_pares_no_ciclo': default_avg_group_val, 'avg_impares_no_ciclo': default_avg_group_val, 'avg_primos_no_ciclo': default_avg_group_val})
        else:
            cycle_contest_properties_list: List[Dict[str, Any]] = []
            for _, contest_row_in_cycle in df_current_cycle_contests.iterrows():
                try:
                    draw_in_cycle = [int(contest_row_in_cycle[col]) for col in actual_dezena_cols_for_props if pd.notna(contest_row_in_cycle[col])]
                    if len(draw_in_cycle) == 15: 
                        properties = analyze_draw_properties(draw_in_cycle) 
                        cycle_contest_properties_list.append(properties)
                except ValueError:
                    logger.warning(f"Erro converter dezenas no concurso {contest_row_in_cycle.get('Concurso', 'Desconhecido')} do ciclo {ciclo_num} para group metrics."); 
                    continue
            
            if cycle_contest_properties_list:
                df_cycle_props = pd.DataFrame(cycle_contest_properties_list)
                results_data_lists["group_metrics"].append({
                    'ciclo_num': ciclo_num,
                    'avg_pares_no_ciclo': df_cycle_props['pares'].mean() if 'pares' in df_cycle_props and not df_cycle_props['pares'].empty else default_avg_group_val,
                    'avg_impares_no_ciclo': df_cycle_props['impares'].mean() if 'impares' in df_cycle_props and not df_cycle_props['impares'].empty else default_avg_group_val,
                    'avg_primos_no_ciclo': df_cycle_props['primos'].mean() if 'primos' in df_cycle_props and not df_cycle_props['primos'].empty else default_avg_group_val
                })
            else: 
                results_data_lists["group_metrics"].append({'ciclo_num': ciclo_num, 'avg_pares_no_ciclo': default_avg_group_val, 'avg_impares_no_ciclo': default_avg_group_val, 'avg_primos_no_ciclo': default_avg_group_val})

    # Converter listas de resultados em DataFrames
    if results_data_lists["frequency"]: output_dfs["ciclo_metric_frequency"] = pd.DataFrame(results_data_lists["frequency"])
    if results_data_lists["mean_delay"]: output_dfs["ciclo_metric_atraso_medio"] = pd.DataFrame(results_data_lists["mean_delay"])
    if results_data_lists["max_delay"]: output_dfs["ciclo_metric_atraso_maximo"] = pd.DataFrame(results_data_lists["max_delay"])
    if results_data_lists["final_delay"]: output_dfs["ciclo_metric_atraso_final"] = pd.DataFrame(results_data_lists["final_delay"])
    if results_data_lists["rank_frequency"]: output_dfs["ciclo_rank_frequency"] = pd.DataFrame(results_data_lists["rank_frequency"])
    if results_data_lists["group_metrics"]: output_dfs["ciclo_group_metrics"] = pd.DataFrame(results_data_lists["group_metrics"])
        
    logger.info("Cálculo de métricas detalhadas (incluindo rank e grupos) por dezena/ciclo para ciclos fechados concluído.")
    return output_dfs

# Funções wrapper (identify_cycles, calculate_cycle_stats, analyze_cycle_closing_data) mantidas.
def identify_cycles(all_data_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    logger.info("Chamando identify_cycles (wrapper para identify_and_process_cycles).")
    results_dict = identify_and_process_cycles(all_data_df)
    return results_dict.get('ciclos_detalhe')

def calculate_cycle_stats(df_cycles_detail: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    logger.info("Chamando calculate_cycle_stats (wrapper para lógica de sumário).")
    if df_cycles_detail is None or df_cycles_detail.empty: return None
    if 'duracao_concursos' not in df_cycles_detail.columns: return None
    df_closed_cycles = df_cycles_detail[pd.to_numeric(df_cycles_detail['duracao_concursos'], errors='coerce').notna()].copy()
    if not df_closed_cycles.empty and 'duracao_concursos' in df_closed_cycles.columns: 
        df_closed_cycles['duracao_concursos'] = pd.to_numeric(df_closed_cycles['duracao_concursos'])
        if not df_closed_cycles.empty : 
            summary_stats = {'total_ciclos_fechados': int(len(df_closed_cycles)), 'duracao_media_ciclo': float(df_closed_cycles['duracao_concursos'].mean()) if len(df_closed_cycles) > 0 else np.nan, 'duracao_min_ciclo': int(df_closed_cycles['duracao_concursos'].min()) if len(df_closed_cycles) > 0 else np.nan, 'duracao_max_ciclo': int(df_closed_cycles['duracao_concursos'].max()) if len(df_closed_cycles) > 0 else np.nan, 'duracao_mediana_ciclo': float(df_closed_cycles['duracao_concursos'].median()) if len(df_closed_cycles) > 0 else np.nan}
            return pd.DataFrame([summary_stats])
    return None

def analyze_cycle_closing_data(all_data_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    logger.info("Chamando analyze_cycle_closing_data (placeholder).")
    logger.warning("Função 'analyze_cycle_closing_data' é um placeholder e precisa de implementação real.")
    return pd.DataFrame({"analise_fechamento_stub": ["dados de fechamento não implementados"]})