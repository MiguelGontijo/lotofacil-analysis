# src/analysis_aggregator.py

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Set, List

# Importa constantes do config
from src.config import (
    logger, ALL_NUMBERS, AGGREGATOR_WINDOWS,
    TREND_SHORT_WINDOW, TREND_LONG_WINDOW
)
# Importa funções de análise
from src.analysis.frequency_analysis import get_cumulative_frequency, calculate_frequency as calculate_period_frequency, calculate_windowed_frequency
from src.analysis.delay_analysis import calculate_current_delay, calculate_delay_stats
from src.analysis.cycle_analysis import get_cycles_df, calculate_current_incomplete_cycle_stats, calculate_current_intra_cycle_delay #, calculate_historical_intra_cycle_delay_stats (Removido por enquanto)
# <<< Importa a nova função de análise de fechamento >>>
from src.analysis.cycle_closing_analysis import calculate_closing_number_stats
from src.analysis.number_properties_analysis import analyze_number_properties, summarize_properties
from src.database_manager import get_draw_numbers

# Fallbacks
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'AGGREGATOR_WINDOWS' not in globals(): AGGREGATOR_WINDOWS = [10, 25, 50, 100, 200, 300, 400, 500]
if 'TREND_SHORT_WINDOW' not in globals(): TREND_SHORT_WINDOW = 10
if 'TREND_LONG_WINDOW' not in globals(): TREND_LONG_WINDOW = 50


def get_consolidated_analysis(concurso_maximo: int) -> Optional[Dict[str, Any]]:
    """ Executa análises V7 (inclui stats de fechamento de ciclo). """
    logger.info(f"Agregando análises (v7) até o concurso {concurso_maximo}...")
    if concurso_maximo <= 0: logger.error("Concurso inválido."); return None

    results: Dict[str, Any] = {}; errors_summary = {}

    # --- Executa Análises ---
    # 1. Frequências (Geral e Janelas)
    results['overall_freq'] = get_cumulative_frequency(concurso_maximo=concurso_maximo)
    if results['overall_freq'] is None: errors_summary['overall_freq'] = "Falha"
    windows_to_calc = set(AGGREGATOR_WINDOWS) | {TREND_SHORT_WINDOW, TREND_LONG_WINDOW}
    for window in sorted(list(windows_to_calc)):
        key = f'recent_freq_{window}'
        results[key] = calculate_windowed_frequency(window_size=window, concurso_maximo=concurso_maximo)
        if results[key] is None: errors_summary[key] = f"Falha W{window}"

    # 2. Atrasos (Atual e Stats)
    results['current_delay'] = calculate_current_delay(concurso_maximo=concurso_maximo)
    if results['current_delay'] is None: errors_summary['current_delay'] = "Falha"
    delay_stats_df = calculate_delay_stats(concurso_maximo=concurso_maximo)
    if delay_stats_df is not None: results['delay_mean']=delay_stats_df['media_atraso']; results['delay_std_dev']=delay_stats_df['std_dev_atraso']; results['max_delay']=delay_stats_df['max_atraso']
    else: errors_summary['delay_stats'] = "Falha"; results.update({'delay_mean':None, 'delay_std_dev':None, 'max_delay':None})

    # 3. Ciclos e Métricas Derivadas
    cycles_df_until_max = get_cycles_df(concurso_maximo=concurso_maximo)
    results['cycles_completed_until_max'] = cycles_df_until_max
    last_cycle_freq = None
    if cycles_df_until_max is not None and not cycles_df_until_max.empty:
        completed_before_max = cycles_df_until_max[cycles_df_until_max['concurso_fim'] < concurso_maximo]
        if not completed_before_max.empty:
            last_cycle = completed_before_max.iloc[-1]; start_c, end_c = int(last_cycle['concurso_inicio']), int(last_cycle['concurso_fim']); logger.info(f"Calculando freq. últ. ciclo: {last_cycle['numero_ciclo']}"); last_cycle_freq = calculate_period_frequency(concurso_minimo=start_c, concurso_maximo=end_c)
        # <<< CHAMA A NOVA ANÁLISE DE FECHAMENTO >>>
        # Calcula sobre os ciclos completados ATÉ o concurso_maximo
        closing_stats_df = calculate_closing_number_stats(cycles_df_until_max)
        if closing_stats_df is not None:
            results['closing_freq'] = closing_stats_df['closing_freq']
            results['sole_closing_freq'] = closing_stats_df['sole_closing_freq']
        else:
            errors_summary['closing_stats'] = "Falha"
            results['closing_freq'] = None; results['sole_closing_freq'] = None
    else: # Se não há ciclos completos
        results['closing_freq'] = pd.Series(0, index=ALL_NUMBERS); results['sole_closing_freq'] = pd.Series(0, index=ALL_NUMBERS)

    results['last_cycle_freq'] = last_cycle_freq
    curr_cycle_start, curr_cycle_drawn, curr_cycle_freq = calculate_current_incomplete_cycle_stats(concurso_maximo)
    results['current_cycle_start'] = curr_cycle_start; results['current_cycle_drawn'] = curr_cycle_drawn; results['current_cycle_freq'] = curr_cycle_freq
    all_num_set = set(ALL_NUMBERS); results['missing_current_cycle'] = all_num_set - curr_cycle_drawn if curr_cycle_drawn is not None else (all_num_set if curr_cycle_start is not None else None)
    results['current_intra_cycle_delay'] = calculate_current_intra_cycle_delay(curr_cycle_start, concurso_maximo) if curr_cycle_start else None
    if results['current_intra_cycle_delay'] is None and curr_cycle_start is not None: errors_summary['intra_cycle_delay'] = "Falha"
    # results['avg_hist_intra_delay'] = ... (Ainda não implementado)
    # results['max_hist_intra_delay'] = ...

    # 4. Propriedades
    properties_df = analyze_number_properties(concurso_maximo=concurso_maximo)
    if properties_df is not None: results['properties_summary'] = summarize_properties(properties_df)
    else: errors_summary['properties'] = "Falha"; results['properties_summary'] = None

    # 5. Tendência de Frequência
    freq_short = results.get(f'recent_freq_{TREND_SHORT_WINDOW}')
    freq_long = results.get(f'recent_freq_{TREND_LONG_WINDOW}')
    freq_trend = None
    if freq_short is not None and freq_long is not None:
         freq_trend = pd.Series(np.where(freq_long == 0, np.where(freq_short > 0, 999, 1), freq_short / freq_long), index=ALL_NUMBERS).fillna(1)
         freq_trend = freq_trend.reindex(ALL_NUMBERS, fill_value=1.0)
    else: errors_summary['freq_trend'] = f"Faltam W{TREND_SHORT_WINDOW}/W{TREND_LONG_WINDOW}"
    results['freq_trend'] = freq_trend

    # 6. Números do Último Sorteio (N-1)
    if concurso_maximo > 0:
         results['numbers_in_last_draw'] = get_draw_numbers(concurso_maximo)
         if results['numbers_in_last_draw'] is None: errors_summary['last_draw'] = "Falha"
    else: results['numbers_in_last_draw'] = set()

    if errors_summary: logger.error(f"Erros na agregação: {errors_summary}")
    logger.info(f"Agregação de análises (v7) até {concurso_maximo} concluída.")
    return results