# src/analysis_aggregator.py

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Set, List

# Importa constantes do config
from src.config import (
    logger, ALL_NUMBERS, AGGREGATOR_WINDOWS,
    TREND_SHORT_WINDOW, TREND_LONG_WINDOW, DEFAULT_GROUP_WINDOWS # Usa DEFAULT_GROUP_WINDOWS
)
# Importa funções de análise
from src.analysis.frequency_analysis import get_cumulative_frequency, calculate_frequency as calculate_period_frequency, calculate_windowed_frequency
from src.analysis.delay_analysis import calculate_current_delay, calculate_delay_stats
from src.analysis.cycle_analysis import get_cycles_df, calculate_current_incomplete_cycle_stats, calculate_current_intra_cycle_delay #, calculate_historical_intra_cycle_delay_stats
from src.analysis.cycle_closing_analysis import calculate_closing_number_stats
from src.analysis.number_properties_analysis import analyze_number_properties, summarize_properties
# <<< Importa a nova análise de grupo (que retorna DF) >>>
from src.analysis.group_trend_analysis import calculate_group_freq_stats
from src.analysis.rank_trend_analysis import calculate_overall_rank_trend
from src.database_manager import get_draw_numbers

# Fallbacks
# ... (iguais antes) ...
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'AGGREGATOR_WINDOWS' not in globals(): AGGREGATOR_WINDOWS = [10, 25, 50, 100, 200, 300, 400, 500]
if 'TREND_SHORT_WINDOW' not in globals(): TREND_SHORT_WINDOW = 10
if 'TREND_LONG_WINDOW' not in globals(): TREND_LONG_WINDOW = 50
if 'DEFAULT_GROUP_WINDOWS' not in globals(): DEFAULT_GROUP_WINDOWS = [25, 100]


def get_consolidated_analysis(concurso_maximo: int) -> Optional[Dict[str, Any]]:
    """ Executa análises V9 (inclui group stats no formato de Series). """
    logger.info(f"Agregando análises (v9) até o concurso {concurso_maximo}...")
    if concurso_maximo <= 0: logger.error("Concurso inválido."); return None

    results: Dict[str, Any] = {}; errors_summary = {}

    # --- Executa Análises (Ordem pode importar para cache de janelas) ---
    # 1. Frequências (Geral e Janelas)
    results['overall_freq'] = get_cumulative_frequency(concurso_maximo=concurso_maximo)
    if results['overall_freq'] is None: errors_summary['overall_freq'] = "Falha"
    windows_needed = set(AGGREGATOR_WINDOWS) | {TREND_SHORT_WINDOW, TREND_LONG_WINDOW} | set(DEFAULT_GROUP_WINDOWS)
    for window in sorted(list(windows_needed)):
        key = f'recent_freq_{window}'
        if key not in results: results[key] = calculate_windowed_frequency(window_size=window, concurso_maximo=concurso_maximo)
        if results[key] is None: errors_summary[key] = f"Falha W{window}"

    # 2. Atrasos
    results['current_delay'] = calculate_current_delay(concurso_maximo=concurso_maximo)
    if results['current_delay'] is None: errors_summary['current_delay'] = "Falha"
    delay_stats_df = calculate_delay_stats(concurso_maximo=concurso_maximo)
    if delay_stats_df is not None: results['delay_mean']=delay_stats_df['media_atraso']; results['delay_std_dev']=delay_stats_df['std_dev_atraso']; results['max_delay']=delay_stats_df['max_atraso']
    else: errors_summary['delay_stats'] = "Falha"; results.update({'delay_mean':None, 'delay_std_dev':None, 'max_delay':None})

    # 3. Ciclos e Derivados
    cycles_df_until_max = get_cycles_df(concurso_maximo=concurso_maximo)
    results['cycles_completed_until_max'] = cycles_df_until_max
    last_cycle_freq = None; closing_stats_df = None
    if cycles_df_until_max is not None and not cycles_df_until_max.empty:
        completed_before_max = cycles_df_until_max[cycles_df_until_max['concurso_fim'] < concurso_maximo]
        if not completed_before_max.empty: last_cycle = completed_before_max.iloc[-1]; start_c, end_c = int(last_cycle['concurso_inicio']), int(last_cycle['concurso_fim']); last_cycle_freq = calculate_period_frequency(concurso_minimo=start_c, concurso_maximo=end_c)
        closing_stats_df = calculate_closing_number_stats(cycles_df_until_max)
    results['last_cycle_freq'] = last_cycle_freq
    if closing_stats_df is not None: results['closing_freq'] = closing_stats_df['closing_freq']; results['sole_closing_freq'] = closing_stats_df['sole_closing_freq']
    else: errors_summary['closing_stats'] = "Falha"; results.update({'closing_freq':None, 'sole_closing_freq':None})
    curr_cycle_start, curr_cycle_drawn, curr_cycle_freq = calculate_current_incomplete_cycle_stats(concurso_maximo)
    results['current_cycle_start'] = curr_cycle_start; results['current_cycle_drawn'] = curr_cycle_drawn; results['current_cycle_freq'] = curr_cycle_freq
    all_num_set = set(ALL_NUMBERS); results['missing_current_cycle'] = all_num_set - curr_cycle_drawn if curr_cycle_drawn is not None else (all_num_set if curr_cycle_start is not None else None)
    results['current_intra_cycle_delay'] = calculate_current_intra_cycle_delay(curr_cycle_start, concurso_maximo) if curr_cycle_start else None
    # ... (stats intra-ciclo histórico ainda pendente) ...

    # 4. Propriedades
    properties_df = analyze_number_properties(concurso_maximo=concurso_maximo)
    if properties_df is not None: results['properties_summary'] = summarize_properties(properties_df)
    else: errors_summary['properties'] = "Falha"; results['properties_summary'] = None

    # 5. Tendência de Frequência Individual
    freq_short = results.get(f'recent_freq_{TREND_SHORT_WINDOW}')
    freq_long = results.get(f'recent_freq_{TREND_LONG_WINDOW}')
    freq_trend = None
    if freq_short is not None and freq_long is not None: freq_trend = pd.Series(np.where(freq_long == 0, np.where(freq_short > 0, 999, 1), freq_short / freq_long), index=ALL_NUMBERS).fillna(1).reindex(ALL_NUMBERS, fill_value=1.0)
    else: errors_summary['freq_trend'] = f"Faltam W{TREND_SHORT_WINDOW}/W{TREND_LONG_WINDOW}"
    results['freq_trend'] = freq_trend

    # 6. Números do Último Sorteio (N-1)
    if concurso_maximo > 0: results['numbers_in_last_draw'] = get_draw_numbers(concurso_maximo);
    else: results['numbers_in_last_draw'] = set()
    if results.get('numbers_in_last_draw') is None and concurso_maximo > 0: errors_summary['last_draw'] = "Falha"

    # *** 7. NOVO: Tendências de Grupo (Processa o DF retornado) ***
    group_stats_df = calculate_group_freq_stats(concurso_maximo) # Usa janelas default [25, 100]
    if group_stats_df is not None:
        # Adiciona cada coluna do DF como uma Series separada no results dict
        for col in group_stats_df.columns:
            results[col] = group_stats_df[col]
    else:
        errors_summary['group_stats'] = "Falha"
        # Adiciona chaves com None para evitar KeyError no scorer
        for w in DEFAULT_GROUP_WINDOWS: results[f'group_W{w}_avg_freq'] = None # Adiciona placeholders

    # *** 8. Tendência de Rank ***
    results['rank_trend'] = calculate_overall_rank_trend(concurso_maximo) # Usa lookback padrão
    if results['rank_trend'] is None: errors_summary['rank_trend'] = "Falha"


    if errors_summary: logger.error(f"Erros na agregação: {errors_summary}")
    logger.info(f"Agregação de análises (v9) até {concurso_maximo} concluída.")
    return results