# src/analysis_aggregator.py

import pandas as pd
from typing import Dict, Any, Optional

from src.config import logger
from src.analysis.frequency_analysis import (
    calculate_frequency as calculate_period_frequency,
    calculate_windowed_frequency
)
from src.analysis.delay_analysis import calculate_current_delay, calculate_max_delay
# Importa a nova função de LEITURA de ciclos e a de stats do ciclo atual
from src.analysis.cycle_analysis import get_cycles_df, calculate_current_incomplete_cycle_stats
from src.analysis.number_properties_analysis import analyze_number_properties, summarize_properties

AGGREGATOR_WINDOWS = [10, 25, 50]

def get_consolidated_analysis(concurso_maximo: int) -> Optional[Dict[str, Any]]:
    """ Executa análises e retorna resultados consolidados até concurso_maximo. """
    logger.info(f"Agregando análises até o concurso {concurso_maximo}...")
    if concurso_maximo <= 0: logger.error("Concurso máximo inválido."); return None

    results: Dict[str, Any] = {}
    errors_summary = {}

    # --- Executa Análises ---
    # 1. Frequências
    results['overall_freq'] = calculate_period_frequency(concurso_maximo=concurso_maximo)
    if results['overall_freq'] is None: errors_summary['overall_freq'] = "Falha"
    for window in AGGREGATOR_WINDOWS:
        key = f'recent_freq_{window}'
        results[key] = calculate_windowed_frequency(window_size=window, concurso_maximo=concurso_maximo)
        if results[key] is None: errors_summary[key] = f"Falha W{window}"

    # 2. Atrasos
    results['current_delay'] = calculate_current_delay(concurso_maximo=concurso_maximo)
    if results['current_delay'] is None: errors_summary['current_delay'] = "Falha"
    results['max_delay'] = calculate_max_delay(concurso_maximo=concurso_maximo)
    if results['max_delay'] is None: errors_summary['max_delay'] = "Falha"

    # 3. Ciclos (Otimizado)
    # Lê os ciclos JÁ CALCULADOS da tabela 'ciclos' até o concurso máximo
    cycles_df_until_max = get_cycles_df(concurso_maximo=concurso_maximo) # <<< USA A TABELA
    results['cycles_completed_until_max'] = cycles_df_until_max

    # 3a. Frequência do Último Ciclo Completo
    last_cycle_freq = None
    if cycles_df_until_max is not None and not cycles_df_until_max.empty:
        # O último ciclo no DataFrame é o último completo *antes ou em* concurso_maximo
        # Mas a análise é feita até concurso_maximo, então precisamos do último que terminou ANTES
        completed_before_max = cycles_df_until_max[cycles_df_until_max['concurso_fim'] < concurso_maximo]
        if not completed_before_max.empty:
            last_cycle = completed_before_max.iloc[-1]
            start_c, end_c = int(last_cycle['concurso_inicio']), int(last_cycle['concurso_fim'])
            logger.info(f"Calculando freq. do último ciclo completo: {last_cycle['numero_ciclo']} ({start_c}-{end_c})")
            last_cycle_freq = calculate_period_frequency(concurso_minimo=start_c, concurso_maximo=end_c)
        else: logger.info(f"Nenhum ciclo completo ANTES de {concurso_maximo} encontrado na tabela 'ciclos'.")
    results['last_cycle_freq'] = last_cycle_freq

    # 3b. Stats do Ciclo Incompleto Atual (Usa a função otimizada)
    # Esta função agora usa get_cycles_df internamente (indiretamente via identify_cycles otimizado)
    curr_cycle_start, curr_cycle_drawn, curr_cycle_freq = calculate_current_incomplete_cycle_stats(concurso_maximo)
    results['current_cycle_start'] = curr_cycle_start
    results['current_cycle_drawn'] = curr_cycle_drawn
    results['current_cycle_freq'] = curr_cycle_freq

    # 4. Propriedades
    properties_df = analyze_number_properties(concurso_maximo=concurso_maximo)
    if properties_df is not None: results['properties_summary'] = summarize_properties(properties_df)
    else: errors_summary['properties'] = "Falha"; results['properties_summary'] = None

    if errors_summary: logger.error(f"Erros na agregação: {errors_summary}")
    logger.info(f"Agregação de análises até {concurso_maximo} concluída.")
    return results