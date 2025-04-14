# src/pipeline_steps/execute_frequency.py

import argparse
import pandas as pd # Necessário para checar tipo

from src.config import logger
from src.analysis.frequency_analysis import (
    calculate_frequency as calculate_period_frequency,
    calculate_windowed_frequency,
    calculate_cumulative_frequency_history
)
try:
    from src.visualization.plotter import plot_frequency_bar
    PLOTTING_ENABLED_STEP = True
except ImportError:
    PLOTTING_ENABLED_STEP = False

def execute_frequency_analysis(args: argparse.Namespace, should_plot: bool):
    """ Executa e exibe todas as análises de frequência. """
    logger.info(f"Executando análises de frequência...")
    max_c = args.max_concurso
    plot_flag = should_plot and PLOTTING_ENABLED_STEP

    # --- Frequência Geral ---
    overall_freq_series = calculate_period_frequency(concurso_maximo=max_c)
    if overall_freq_series is not None:
        print("\n--- Frequência Geral das Dezenas ---")
        print(overall_freq_series.to_string())
        if plot_flag:
            plot_frequency_bar(overall_freq_series, "Frequência Geral", "freq_geral")
    else:
        logger.warning("Não foi possível calcular a frequência geral.")

    # --- Frequência por Janela ---
    try:
        window_sizes = [int(w.strip()) for w in args.windows.split(',') if w.strip()]
    except ValueError:
        logger.error(f"Formato inválido para --windows: '{args.windows}'.")
        window_sizes = []

    for window in window_sizes:
        window_freq_series = calculate_windowed_frequency(window_size=window, concurso_maximo=max_c)
        if window_freq_series is not None:
            print(f"\n--- Frequência nos Últimos {window} Concursos ---")
            print(window_freq_series.to_string())
            # if plot_flag: plot_frequency_bar(window_freq_series, f"Freq. Últimos {window}", f"freq_{window}")
        else:
            logger.warning(f"Não foi possível calcular frequência para janela de {window}.")

    # --- Frequência Acumulada ---
    cumulative_hist_df = calculate_cumulative_frequency_history(concurso_maximo=max_c)
    if cumulative_hist_df is not None:
        print("\n--- Histórico de Frequência Acumulada (Últimos 5 Registros) ---")
        # Usar to_string para formatar melhor DataFrames grandes no console
        print(cumulative_hist_df.tail().to_string(index=False))
    else:
        logger.warning("Não foi possível calcular a frequência acumulada.")

    logger.info("Análises de frequência concluídas.")