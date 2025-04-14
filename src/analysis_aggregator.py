# src/analysis_aggregator.py

import pandas as pd
from typing import Dict, Any, Optional

from src.config import logger
from src.analysis.frequency_analysis import (
    calculate_frequency as calculate_period_frequency,
    calculate_windowed_frequency
)
from src.analysis.delay_analysis import calculate_current_delay, calculate_max_delay
from src.analysis.cycle_analysis import identify_cycles, calculate_frequency_per_cycle, calculate_current_incomplete_cycle_stats
from src.analysis.number_properties_analysis import analyze_number_properties, summarize_properties
# from src.analysis.combination_analysis import calculate_combination_frequency

DEFAULT_WINDOWS = [25] # Janela padrão para análise recente no agregador

def get_consolidated_analysis(concurso_maximo: int) -> Optional[Dict[str, Any]]:
    """
    Executa um conjunto de análises relevantes até um concurso máximo
    e retorna os resultados consolidados em um dicionário.
    """
    logger.info(f"Agregando análises até o concurso {concurso_maximo}...")
    if concurso_maximo <= 0:
        logger.error("Concurso máximo inválido para agregação.")
        return None

    results: Dict[str, Any] = {}
    errors_summary = {} # Para logar erros específicos

    # 1. Frequência Geral
    results['overall_freq'] = calculate_period_frequency(concurso_maximo=concurso_maximo)
    if results['overall_freq'] is None: errors_summary['overall_freq'] = "Falha"

    # 2. Frequência Recente (Janelas Padrão)
    for window in DEFAULT_WINDOWS:
        key = f'recent_freq_{window}'
        results[key] = calculate_windowed_frequency(window_size=window, concurso_maximo=concurso_maximo)
        if results[key] is None: errors_summary[key] = "Falha"

    # 3. Atraso Atual
    results['current_delay'] = calculate_current_delay(concurso_maximo=concurso_maximo)
    if results['current_delay'] is None: errors_summary['current_delay'] = "Falha"

    # 4. Atraso Máximo Histórico
    results['max_delay'] = calculate_max_delay(concurso_maximo=concurso_maximo)
    if results['max_delay'] is None: errors_summary['max_delay'] = "Falha"

    # 5. Ciclos
    all_cycles_df = identify_cycles() # Identifica todos os ciclos
    results['all_cycles'] = all_cycles_df # Guarda a informação completa

    # 5a. Frequência do Último Ciclo Completo
    last_cycle_freq = None
    if all_cycles_df is not None and not all_cycles_df.empty:
        completed_before_max = all_cycles_df[all_cycles_df['concurso_fim'] < concurso_maximo]
        if not completed_before_max.empty:
            last_cycle = completed_before_max.iloc[-1]
            last_cycle_num = int(last_cycle['numero_ciclo'])
            start_c, end_c = int(last_cycle['concurso_inicio']), int(last_cycle['concurso_fim'])
            logger.info(f"Calculando freq. do último ciclo completo antes de {concurso_maximo}: Ciclo {last_cycle_num}")
            last_cycle_freq = calculate_period_frequency(concurso_minimo=start_c, concurso_maximo=end_c)
        else: logger.warning(f"Nenhum ciclo completo antes de {concurso_maximo}.")
    results['last_cycle_freq'] = last_cycle_freq
    # Não considera erro se não houver ciclo anterior

    # 5b. Stats do Ciclo Incompleto Atual
    curr_cycle_start, curr_cycle_drawn, curr_cycle_freq = calculate_current_incomplete_cycle_stats(concurso_maximo)
    results['current_cycle_start'] = curr_cycle_start
    results['current_cycle_drawn'] = curr_cycle_drawn
    results['current_cycle_freq'] = curr_cycle_freq
    # Não considera erro se não houver ciclo incompleto

    # 6. Propriedades (Sumarizadas)
    properties_df = analyze_number_properties(concurso_maximo=concurso_maximo)
    if properties_df is not None:
        results['properties_summary'] = summarize_properties(properties_df)
    else:
        errors_summary['properties'] = "Falha"
        results['properties_summary'] = None

    # Log de erros
    if errors_summary:
        logger.error(f"Ocorreram erros durante a agregação das análises: {errors_summary}")
        # Decidir se retorna None ou o dicionário parcial
        # return None # Retorna None se qualquer erro for crítico

    logger.info(f"Agregação de análises até {concurso_maximo} concluída.")
    return results