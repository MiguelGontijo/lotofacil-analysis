# src/analysis_aggregator.py

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Set, List

# Usa AGGREGATOR_WINDOWS e outras constantes do config
from src.config import (
    logger, ALL_NUMBERS, AGGREGATOR_WINDOWS,
    TREND_SHORT_WINDOW, TREND_LONG_WINDOW
)
# Importa a NOVA função otimizada e as outras
from src.analysis.frequency_analysis import (
    get_cumulative_frequency, # <<< USA ESTA PARA overall_freq
    calculate_frequency as calculate_period_frequency, # <<< Usada para ciclos
    calculate_windowed_frequency
)
from src.analysis.delay_analysis import calculate_current_delay, calculate_delay_stats
from src.analysis.cycle_analysis import get_cycles_df, calculate_current_incomplete_cycle_stats
from src.analysis.number_properties_analysis import analyze_number_properties, summarize_properties

# Fallbacks
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'AGGREGATOR_WINDOWS' not in globals(): AGGREGATOR_WINDOWS = [10, 25, 50, 100, 200]
if 'TREND_SHORT_WINDOW' not in globals(): TREND_SHORT_WINDOW = 10
if 'TREND_LONG_WINDOW' not in globals(): TREND_LONG_WINDOW = 50


def get_consolidated_analysis(concurso_maximo: int) -> Optional[Dict[str, Any]]:
    """ Executa análises V5 (usa snapshots para freq geral). """
    logger.info(f"Agregando análises (v5) até {concurso_maximo}...")
    if concurso_maximo <= 0: logger.error("Concurso inválido."); return None
    results: Dict[str, Any] = {}; errors_summary = {}

    # 1. Frequência Geral (Otimizada com Snapshots)
    results['overall_freq'] = get_cumulative_frequency(concurso_maximo=concurso_maximo) # <<< MUDANÇA AQUI
    if results['overall_freq'] is None: errors_summary['overall_freq'] = "Falha"

    # 2. Frequência Recente
    windows_to_calc = set(AGGREGATOR_WINDOWS) | {TREND_SHORT_WINDOW, TREND_LONG_WINDOW}
    for window in sorted(list(windows_to_calc)):
        key = f'recent_freq_{window}'
        results[key] = calculate_windowed_frequency(window_size=window, concurso_maximo=concurso_maximo)
        if results[key] is None: errors_summary[key] = f"Falha W{window}"

    # 3. Atrasos (Atual e Stats)
    results['current_delay'] = calculate_current_delay(concurso_maximo=concurso_maximo)
    if results['current_delay'] is None: errors_summary['current_delay'] = "Falha"
    delay_stats_df = calculate_delay_stats(concurso_maximo=concurso_maximo)
    if delay_stats_df is not None: results['delay_mean']=delay_stats_df['media_atraso']; results['delay_std_dev']=delay_stats_df['std_dev_atraso']; results['max_delay']=delay_stats_df['max_atraso']
    else: errors_summary['delay_stats'] = "Falha"; results.update({'delay_mean':None, 'delay_std_dev':None, 'max_delay':None})

    # 4. Ciclos
    cycles_df_until_max = get_cycles_df(concurso_maximo=concurso_maximo)
    results['cycles_completed_until_max'] = cycles_df_until_max
    last_cycle_freq = None
    if cycles_df_until_max is not None and not cycles_df_until_max.empty:
        completed_before_max = cycles_df_until_max[cycles_df_until_max['concurso_fim'] < concurso_maximo]
        if not completed_before_max.empty:
            last_cycle = completed_before_max.iloc[-1]; start_c, end_c = int(last_cycle['concurso_inicio']), int(last_cycle['concurso_fim']); logger.info(f"Calculando freq. último ciclo: {last_cycle['numero_ciclo']}");
            # Usa calculate_period_frequency aqui (não otimizado por snapshot, mas range é pequeno)
            last_cycle_freq = calculate_period_frequency(concurso_minimo=start_c, concurso_maximo=end_c)
    results['last_cycle_freq'] = last_cycle_freq
    curr_cycle_start, curr_cycle_drawn, curr_cycle_freq = calculate_current_incomplete_cycle_stats(concurso_maximo)
    results['current_cycle_start'] = curr_cycle_start; results['current_cycle_drawn'] = curr_cycle_drawn; results['current_cycle_freq'] = curr_cycle_freq
    all_num_set = set(ALL_NUMBERS); results['missing_current_cycle'] = all_num_set - curr_cycle_drawn if curr_cycle_drawn is not None else (all_num_set if curr_cycle_start is not None else None)
    results['current_intra_cycle_delay'] = calculate_current_intra_cycle_delay(curr_cycle_start, concurso_maximo) if curr_cycle_start else None
    if results['current_intra_cycle_delay'] is None and curr_cycle_start is not None: errors_summary['intra_cycle_delay'] = "Falha"

    # 5. Propriedades
    properties_df = analyze_number_properties(concurso_maximo=concurso_maximo)
    if properties_df is not None: results['properties_summary'] = summarize_properties(properties_df)
    else: errors_summary['properties'] = "Falha"; results['properties_summary'] = None

    # 6. Tendência de Frequência
    freq_short = results.get(f'recent_freq_{TREND_SHORT_WINDOW}')
    freq_long = results.get(f'recent_freq_{TREND_LONG_WINDOW}')
    freq_trend = None
    if freq_short is not None and freq_long is not None:
         freq_trend = pd.Series(np.where(freq_long == 0, np.where(freq_short > 0, 999, 1), freq_short / freq_long), index=ALL_NUMBERS).fillna(1)
         freq_trend = freq_trend.reindex(ALL_NUMBERS, fill_value=1.0)
    else: errors_summary['freq_trend'] = f"Faltam W{TREND_SHORT_WINDOW}/W{TREND_LONG_WINDOW}"
    results['freq_trend'] = freq_trend

    if errors_summary: logger.error(f"Erros na agregação: {errors_summary}")
    logger.info(f"Agregação de análises (v5) até {concurso_maximo} concluída.")
    return results