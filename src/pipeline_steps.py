# src/pipeline_steps.py

import argparse
import pandas as pd
from typing import Optional

# Importações locais de configuração, análise e plotagem
from src.config import logger
from src.analysis.frequency_analysis import (
    calculate_frequency as calculate_period_frequency,
    calculate_windowed_frequency,
    calculate_cumulative_frequency_history
)
from src.analysis.combination_analysis import calculate_combination_frequency
from src.analysis.cycle_analysis import identify_cycles, run_cycle_frequency_analysis
from src.analysis.delay_analysis import calculate_current_delay, calculate_max_delay
from src.analysis.number_properties_analysis import analyze_number_properties, summarize_properties

try:
    from src.visualization.plotter import (
        plot_frequency_bar, plot_distribution_bar,
        plot_cycle_duration_hist, plot_delay_bar
        )
    PLOTTING_ENABLED_STEPS = True # Flag local para saber se pode plotar
except ImportError:
    PLOTTING_ENABLED_STEPS = False


# --- Funções de Execução para Cada Análise ---

def execute_frequency_analysis(args: argparse.Namespace, should_plot: bool):
    """ Executa e exibe todas as análises de frequência. """
    logger.info(f"Executando análises de frequência...")
    max_c = args.max_concurso
    overall_freq = calculate_period_frequency(concurso_maximo=max_c)
    if overall_freq is not None:
        print("\n--- Frequência Geral das Dezenas ---")
        print(overall_freq.to_string())
        if should_plot and PLOTTING_ENABLED_STEPS:
            plot_frequency_bar(overall_freq, "Frequência Geral", "freq_geral")

    try:
        window_sizes = [int(w.strip()) for w in args.windows.split(',') if w.strip()]
    except ValueError: window_sizes = []
    for window in window_sizes:
        window_freq = calculate_windowed_frequency(window_size=window, concurso_maximo=max_c)
        if window_freq is not None:
            print(f"\n--- Frequência nos Últimos {window} Concursos ---")
            print(window_freq.to_string())
            # if should_plot and PLOTTING_ENABLED_STEPS: plot_frequency_bar(...) # Plot opcional

    cumulative_hist = calculate_cumulative_frequency_history(concurso_maximo=max_c)
    if cumulative_hist is not None:
        print("\n--- Histórico de Frequência Acumulada (Últimos 5 Registros) ---")
        print(cumulative_hist.tail())
    logger.info("Análises de frequência concluídas.")


def execute_pair_analysis(args: argparse.Namespace):
    """ Executa e exibe a análise de pares. """
    logger.info(f"Executando análise de pares...")
    top_n = args.top_n
    max_c = args.max_concurso
    top_pairs = calculate_combination_frequency(2, top_n, max_c)
    if top_pairs:
        print(f"\n--- Top {top_n} Pares Mais Frequentes ---")
        for pair, count in top_pairs: print(f"Par: ({', '.join(map(str, pair))}) - Frequência: {count}")
    else:
         logger.warning("Não foram encontrados pares.")
    logger.info("Análise de pares concluída.")


def execute_combination_analysis(args: argparse.Namespace):
    """ Executa e exibe a análise de combinações (Trios+). """
    logger.info(f"Executando análises de combinação (Trios+)...")
    top_n = args.top_n
    max_c = args.max_concurso
    for size in [3, 4, 5, 6]:
        combo_name_map = {3:"Trios", 4:"Quartetos", 5:"Quintetos", 6:"Sextetos"}
        combo_name = combo_name_map.get(size)
        top_combos = calculate_combination_frequency(size, top_n, max_c)
        if top_combos:
            print(f"\n--- Top {top_n} {combo_name} Mais Frequentes ---")
            for combo, count in top_combos: print(f"Comb: ({', '.join(map(str, combo))}) - Freq: {count}")
        else:
            logger.warning(f"Não foram encontradas combinações para {combo_name}.")
    logger.info("Análises de combinação (Trios+) concluídas.")


def execute_cycle_identification() -> Optional[pd.DataFrame]:
    """ Executa a identificação de ciclos e retorna o DataFrame. """
    logger.info(f"Executando identificação de ciclos...")
    cycles_summary = identify_cycles()
    if cycles_summary is None: logger.error("Falha na identificação de ciclos.")
    else: logger.info("Identificação de ciclos concluída.")
    return cycles_summary


def display_cycle_summary(cycles_summary: pd.DataFrame, args: argparse.Namespace, should_plot: bool):
    """ Exibe o resumo e estatísticas dos ciclos. """
    max_c = args.max_concurso
    if not cycles_summary.empty:
        cycles_filtered = cycles_summary[cycles_summary['concurso_fim'] <= max_c].copy() if max_c else cycles_summary
        if not cycles_filtered.empty:
            print("\n--- Resumo dos Ciclos Completos ---")
            print(cycles_filtered.to_string(index=False))
            stats = cycles_filtered['duracao'].agg(['mean', 'min', 'max'])
            print(f"\nStats Ciclos (até {max_c or 'último'}): {len(cycles_filtered)} ciclos, Média {stats['mean']:.2f}, Min {stats['min']}, Max {stats['max']}")
            if should_plot and PLOTTING_ENABLED_STEPS: plot_cycle_duration_hist(cycles_filtered, "Duração dos Ciclos", "hist_duracao_ciclos")
        else: logger.info(f"Nenhum ciclo completo encontrado até o concurso {max_c}.")
    else: logger.info("Nenhum ciclo completo identificado nos dados.")


def execute_cycle_stats_analysis(cycles_summary: pd.DataFrame):
    """ Executa a análise de frequência dentro dos ciclos. """
    # A função run_cycle_frequency_analysis já imprime os resultados
    run_cycle_frequency_analysis(cycles_summary)
    logger.info("Análise de stats por ciclo concluída.")


def execute_delay_analysis(args: argparse.Namespace, should_plot: bool):
    """ Executa e exibe a análise de atraso atual. """
    logger.info(f"Executando análise de atraso atual...")
    max_c = args.max_concurso
    current_delays = calculate_current_delay(concurso_maximo=max_c)
    if current_delays is not None:
        print("\n--- Atraso Atual das Dezenas ---")
        print(current_delays.to_string())
        if should_plot and PLOTTING_ENABLED_STEPS: plot_delay_bar(current_delays, f"Atraso Atual (Ref: {max_c or 'Último'})", "barras_atraso_atual")
    else:
         logger.error("Falha ao calcular o atraso atual.")
    logger.info("Análise de atraso atual concluída.")


def execute_max_delay_analysis(args: argparse.Namespace, should_plot: bool):
    """ Executa e exibe a análise de atraso máximo histórico. """
    logger.info(f"Executando análise de atraso máximo histórico...")
    max_c = args.max_concurso
    max_delays = calculate_max_delay(concurso_maximo=max_c)
    if max_delays is not None:
        print("\n--- Atraso Máximo Histórico das Dezenas ---")
        print(max_delays.to_string())
        if should_plot and PLOTTING_ENABLED_STEPS: plot_delay_bar(max_delays, f"Atraso Máximo Histórico (até {max_c or 'Último'})", "barras_atraso_maximo")
    else:
         logger.error("Falha ao calcular o atraso máximo histórico.")
    logger.info("Análise de atraso máximo concluída.")


def execute_properties_analysis(args: argparse.Namespace, should_plot: bool):
    """ Executa e exibe a análise de propriedades dos números. """
    logger.info(f"Executando análise de propriedades...")
    max_c = args.max_concurso
    properties_df = analyze_number_properties(concurso_maximo=max_c)
    if properties_df is not None and not properties_df.empty:
        prop_summaries = summarize_properties(properties_df)
        for key, series in prop_summaries.items():
            title = key.replace('_', ' ').replace('par impar', 'Pares/Ímpares').replace('moldura miolo', 'Moldura/Miolo').title()
            print(f"\n--- Frequência {title} ---")
            print(series.to_string())
            if should_plot and PLOTTING_ENABLED_STEPS: plot_distribution_bar(series, title, f"dist_{key}")
    else:
         logger.error("Falha ao analisar as propriedades dos números.")
    logger.info("Análise de propriedades concluída.")