# src/analysis/cycle_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Set, Optional, Any
import logging
import numpy as np

from src.config import ALL_NUMBERS
# Importar as funções REAIS e OTIMIZADAS de chunk_analysis.py
from src.analysis.chunk_analysis import (
    calculate_frequency_in_chunk, # Já estava sendo usado
    get_draw_matrix_for_chunk,    # <<< USAR ESTA
    calculate_delays_for_matrix   # <<< USAR ESTA
)

logger = logging.getLogger(__name__)
ALL_NUMBERS_SET: Set[int] = set(ALL_NUMBERS)

# --- Funções de Identificação de Ciclo (mantidas como antes) ---
def identify_and_process_cycles(all_data_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    # ... (código completo da função como na versão anterior e validada) ...
    # Nenhuma mudança necessária aqui em relação à última versão funcional desta função.
    # Apenas para garantir que está completo:
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
    dezena_cols = [f'bola_{i}' for i in range(1, 16)]
    cycles_data: List[Dict[str, Any]] = []
    current_cycle_numbers_needed = ALL_NUMBERS_SET.copy()
    current_cycle_start_contest = df_sorted.loc[0, 'Concurso'] if not df_sorted.empty else 0
    cycle_count = 0
    last_processed_contest_num_for_open_cycle = df_sorted['Concurso'].max() if not df_sorted.empty else 0
    logger.debug(f"Análise de ciclo: Iniciando loop por {len(df_sorted)} concursos.")
    for index, row in df_sorted.iterrows():
        contest_number = int(row['Concurso'])
        try:
            drawn_numbers_in_this_contest = set()
            for col in dezena_cols:
                if pd.notna(row[col]): drawn_numbers_in_this_contest.add(int(row[col]))
            if not drawn_numbers_in_this_contest and len(dezena_cols) > 0: continue
        except ValueError:
            logger.warning(f"Erro ao converter dezenas para int no concurso {contest_number}. Pulando sorteio.")
            continue
        if current_cycle_numbers_needed == ALL_NUMBERS_SET and contest_number != current_cycle_start_contest:
             current_cycle_start_contest = contest_number
        current_cycle_numbers_needed.difference_update(drawn_numbers_in_this_contest)
        if not current_cycle_numbers_needed: 
            cycle_count += 1
            closed_cycle_info = {'ciclo_num': cycle_count, 'concurso_inicio': current_cycle_start_contest, 'concurso_fim': contest_number, 'duracao_concursos': contest_number - current_cycle_start_contest + 1, 'numeros_faltantes': None, 'qtd_faltantes': 0}
            cycles_data.append(closed_cycle_info)
            logger.debug(f"Ciclo {cycle_count} FECHADO no concurso {contest_number}. Início: {current_cycle_start_contest}, Duração: {closed_cycle_info['duracao_concursos']}. Total de ciclos em cycles_data: {len(cycles_data)}")
            current_cycle_numbers_needed = ALL_NUMBERS_SET.copy()
            if index + 1 < len(df_sorted): current_cycle_start_contest = df_sorted.loc[index + 1, 'Concurso']
    logger.debug(f"Fim do loop principal. Total de ciclos fechados (cycle_count): {cycle_count}. Tamanho de cycles_data (deve ser igual a cycle_count): {len(cycles_data)}")
    if current_cycle_numbers_needed and current_cycle_numbers_needed != ALL_NUMBERS_SET:
        if not df_sorted.empty and current_cycle_start_contest <= last_processed_contest_num_for_open_cycle:
            numeros_faltantes_str = ",".join(map(str, sorted(list(current_cycle_numbers_needed))))
            open_cycle_info = {'ciclo_num': cycle_count + 1, 'concurso_inicio': current_cycle_start_contest, 'concurso_fim': np.nan, 'duracao_concursos': np.nan, 'numeros_faltantes': numeros_faltantes_str, 'qtd_faltantes': len(current_cycle_numbers_needed)}
            cycles_data.append(open_cycle_info)
            logger.debug(f"Ciclo {cycle_count + 1} EM ABERTO adicionado. Início: {current_cycle_start_contest}, Faltantes: {len(current_cycle_numbers_needed)}. Total de ciclos em cycles_data: {len(cycles_data)}")
        else: logger.info("Não há ciclo em andamento para registrar ou início de ciclo inválido.")
    elif cycle_count == 0 and not df_sorted.empty and current_cycle_numbers_needed: 
        if current_cycle_numbers_needed != ALL_NUMBERS_SET: 
            numeros_faltantes_str = ",".join(map(str, sorted(list(current_cycle_numbers_needed))))
            first_open_cycle_info = {'ciclo_num': 1, 'concurso_inicio': current_cycle_start_contest, 'concurso_fim': np.nan, 'duracao_concursos': np.nan, 'numeros_faltantes': numeros_faltantes_str, 'qtd_faltantes': len(current_cycle_numbers_needed)}
            cycles_data.append(first_open_cycle_info)
            logger.debug(f"Primeiro ciclo (1) EM ABERTO adicionado. Início: {current_cycle_start_contest}, Faltantes: {len(current_cycle_numbers_needed)}. Total de ciclos em cycles_data: {len(cycles_data)}")
        else: logger.info("Primeiro ciclo apenas iniciado, todos os números ainda faltantes.")
    results = {}
    logger.debug(f"Antes de criar DataFrame: Tamanho final de cycles_data: {len(cycles_data)}")
    if cycles_data:
        df_cycles_detail = pd.DataFrame(cycles_data)
        logger.debug(f"DataFrame df_cycles_detail criado com {len(df_cycles_detail)} linhas.")
        if 'concurso_fim' in df_cycles_detail.columns: df_cycles_detail['concurso_fim'] = df_cycles_detail['concurso_fim'].astype('Int64')
        if 'duracao_concursos' in df_cycles_detail.columns: df_cycles_detail['duracao_concursos'] = df_cycles_detail['duracao_concursos'].astype('Int64')
        if 'numeros_faltantes' in df_cycles_detail.columns:
            df_cycles_detail['numeros_faltantes'] = df_cycles_detail['numeros_faltantes'].apply(lambda x: ",".join(map(str, sorted(list(x)))) if isinstance(x, (set, list)) else x)
            df_cycles_detail['numeros_faltantes'] = df_cycles_detail['numeros_faltantes'].fillna(value=np.nan).replace([pd.NA], [None])
        results['ciclos_detalhe'] = df_cycles_detail
        logger.info(f"Detalhes de {len(df_cycles_detail)} ciclos/status de ciclo processados (DataFrame final).")
        if 'duracao_concursos' in df_cycles_detail.columns:
            df_closed_cycles = df_cycles_detail[pd.to_numeric(df_cycles_detail['duracao_concursos'], errors='coerce').notna()].copy()
            if not df_closed_cycles.empty and 'duracao_concursos' in df_closed_cycles.columns: 
                df_closed_cycles['duracao_concursos'] = pd.to_numeric(df_closed_cycles['duracao_concursos'])
                summary_stats = {'total_ciclos_fechados': int(len(df_closed_cycles)), 'duracao_media_ciclo': float(df_closed_cycles['duracao_concursos'].mean()), 'duracao_min_ciclo': int(df_closed_cycles['duracao_concursos'].min()), 'duracao_max_ciclo': int(df_closed_cycles['duracao_concursos'].max()), 'duracao_mediana_ciclo': float(df_closed_cycles['duracao_concursos'].median())}
                df_summary_stats = pd.DataFrame([summary_stats])
                results['ciclos_sumario_estatisticas'] = df_summary_stats
                logger.info("Estatísticas sumárias dos ciclos calculadas.")
            else: logger.info("Nenhum ciclo fechado para calcular estatísticas sumárias.")
        else: logger.info("Coluna 'duracao_concursos' não encontrada. Não é possível calcular estatísticas sumárias.")
    else: logger.info("Nenhum dado de ciclo gerado (lista cycles_data está vazia).")
    logger.info("Análise completa de ciclos (identificação e sumário) concluída.")
    return results


# --- NOVA FUNÇÃO PARA MÉTRICAS DETALHADAS POR CICLO FECHADO (MODIFICADA) ---
def calculate_detailed_metrics_per_closed_cycle(
    all_data_df: pd.DataFrame, 
    df_ciclos_detalhe: Optional[pd.DataFrame]
) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Calcula métricas detalhadas (frequência, atrasos) para cada dezena dentro de cada ciclo fechado.
    Reutiliza funções de chunk_analysis para os cálculos dentro do escopo de cada ciclo.
    """
    logger.info("Iniciando cálculo de métricas detalhadas por dezena para cada ciclo fechado (reutilizando lógica de chunk).")
    
    # Dicionário para armazenar listas de dicionários para cada métrica
    results_data_lists: Dict[str, List[Dict[str, Any]]] = {
        "frequency": [], "mean_delay": [], "max_delay": [], "final_delay": []
    }
    # Dicionário para os DataFrames finais
    output_dfs: Dict[str, Optional[pd.DataFrame]] = {
        "ciclo_metric_frequency": None, "ciclo_metric_atraso_medio": None,
        "ciclo_metric_atraso_maximo": None, "ciclo_metric_atraso_final": None,
    }

    if df_ciclos_detalhe is None or df_ciclos_detalhe.empty:
        logger.warning("DataFrame 'df_ciclos_detalhe' está vazio. Nenhuma métrica por ciclo será calculada.")
        return output_dfs
    if all_data_df is None or all_data_df.empty:
        logger.warning("DataFrame 'all_data_df' está vazio. Nenhuma métrica por ciclo será calculada.")
        return output_dfs

    df_closed_cycles = df_ciclos_detalhe[
        df_ciclos_detalhe['concurso_fim'].notna() & 
        df_ciclos_detalhe['duracao_concursos'].notna() &
        (pd.to_numeric(df_ciclos_detalhe['duracao_concursos'], errors='coerce') > 0) # Garante que duração é numérica e >0
    ].copy()

    if df_closed_cycles.empty:
        logger.info("Nenhum ciclo fechado encontrado em 'df_ciclos_detalhe' para cálculo de métricas.")
        return output_dfs
    
    logger.debug(f"Processando métricas para {len(df_closed_cycles)} ciclos fechados.")

    for _, cycle_row in df_closed_cycles.iterrows():
        ciclo_num = int(cycle_row['ciclo_num'])
        start_contest = int(cycle_row['concurso_inicio'])
        end_contest = int(cycle_row['concurso_fim'])

        mask = (all_data_df['Concurso'] >= start_contest) & (all_data_df['Concurso'] <= end_contest)
        df_current_cycle_contests = all_data_df[mask]

        if df_current_cycle_contests.empty:
            logger.warning(f"Ciclo {ciclo_num} (C{start_contest}-C{end_contest}) não tem concursos em all_data_df. Pulando.")
            continue

        # 1. Frequência no Ciclo (reutiliza calculate_frequency_in_chunk)
        freq_series = calculate_frequency_in_chunk(df_current_cycle_contests)
        for d, v in freq_series.items():
            results_data_lists["frequency"].append({'ciclo_num': ciclo_num, 'dezena': int(d), 'frequencia_no_ciclo': int(v)})
        
        # 2. Atrasos no Ciclo (reutiliza get_draw_matrix_for_chunk e calculate_delays_for_matrix)
        #    Estas funções são otimizadas e esperam df_chunk, start_contest, end_contest.
        draw_matrix_cycle = get_draw_matrix_for_chunk(df_current_cycle_contests, start_contest, end_contest)
        if not draw_matrix_cycle.empty:
            delay_metrics_dict = calculate_delays_for_matrix(draw_matrix_cycle, start_contest, end_contest)
            
            mean_delay_series = delay_metrics_dict["mean"]
            for d, v in mean_delay_series.items():
                results_data_lists["mean_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d), 'atraso_medio_no_ciclo': float(v) if pd.notna(v) else None})

            max_delay_series = delay_metrics_dict["max"]
            for d, v in max_delay_series.items():
                results_data_lists["max_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d), 'atraso_maximo_no_ciclo': int(v)})

            final_delay_series = delay_metrics_dict["final"]
            for d, v in final_delay_series.items():
                results_data_lists["final_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d), 'atraso_final_no_ciclo': int(v)})
        else:
            logger.warning(f"Matriz de sorteios vazia para o ciclo {ciclo_num}. Atrasos não calculados.")
            # Preenche com NaN ou defaults para este ciclo se a matriz estiver vazia
            chunk_duration = end_contest - start_contest + 1
            for d_val in ALL_NUMBERS:
                results_data_lists["mean_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_medio_no_ciclo': float(chunk_duration)})
                results_data_lists["max_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_maximo_no_ciclo': int(chunk_duration)})
                results_data_lists["final_delay"].append({'ciclo_num': ciclo_num, 'dezena': int(d_val), 'atraso_final_no_ciclo': int(chunk_duration)})


    # Converter listas de resultados em DataFrames
    if results_data_lists["frequency"]:
        output_dfs["ciclo_metric_frequency"] = pd.DataFrame(results_data_lists["frequency"])
    if results_data_lists["mean_delay"]:
        output_dfs["ciclo_metric_atraso_medio"] = pd.DataFrame(results_data_lists["mean_delay"])
    if results_data_lists["max_delay"]:
        output_dfs["ciclo_metric_atraso_maximo"] = pd.DataFrame(results_data_lists["max_delay"])
    if results_data_lists["final_delay"]:
        output_dfs["ciclo_metric_atraso_final"] = pd.DataFrame(results_data_lists["final_delay"])
        
    logger.info("Cálculo de métricas detalhadas por dezena para ciclos fechados concluído.")
    return output_dfs

# --- Funções wrapper existentes (mantidas para compatibilidade com o pipeline) ---
def identify_cycles(all_data_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    logger.info("Chamando identify_cycles (wrapper para identify_and_process_cycles).")
    results_dict = identify_and_process_cycles(all_data_df)
    return results_dict.get('ciclos_detalhe')

def calculate_cycle_stats(df_cycles_detail: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    logger.info("Chamando calculate_cycle_stats (wrapper para lógica de sumário).")
    if df_cycles_detail is None or df_cycles_detail.empty:
        logger.warning("DataFrame de detalhes de ciclo vazio para calculate_cycle_stats.")
        return None
    if 'duracao_concursos' not in df_cycles_detail.columns:
        logger.info("Coluna 'duracao_concursos' não encontrada. Não é possível calcular estatísticas sumárias.")
        return None # Adicionado para evitar erro se a coluna não existir
        
    df_closed_cycles = df_cycles_detail[pd.to_numeric(df_cycles_detail['duracao_concursos'], errors='coerce').notna()].copy()
    if not df_closed_cycles.empty and 'duracao_concursos' in df_closed_cycles.columns: 
        df_closed_cycles['duracao_concursos'] = pd.to_numeric(df_closed_cycles['duracao_concursos']) # Já deve ser numérico
        if not df_closed_cycles.empty : # Verificação adicional após a conversão
            summary_stats = {
                'total_ciclos_fechados': int(len(df_closed_cycles)),
                'duracao_media_ciclo': float(df_closed_cycles['duracao_concursos'].mean()),
                'duracao_min_ciclo': int(df_closed_cycles['duracao_concursos'].min()),
                'duracao_max_ciclo': int(df_closed_cycles['duracao_concursos'].max()),
                'duracao_mediana_ciclo': float(df_closed_cycles['duracao_concursos'].median()),
            }
            logger.info("Sumário de estatísticas de ciclo recalculado por calculate_cycle_stats.")
            return pd.DataFrame([summary_stats])
    logger.info("Nenhum ciclo fechado em df_cycles_detail para calcular estatísticas por calculate_cycle_stats.")
    return None

def analyze_cycle_closing_data(all_data_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    logger.info("Chamando analyze_cycle_closing_data (placeholder).")
    logger.warning("Função 'analyze_cycle_closing_data' é um placeholder e precisa de implementação real.")
    return pd.DataFrame({"analise_fechamento_stub": ["dados de fechamento não implementados"]})