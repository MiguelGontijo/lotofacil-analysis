# src/pipeline_steps/execute_max_delay.py

import argparse
import pandas as pd # Para checar None
from src.config import logger
from src.analysis.delay_analysis import calculate_max_delay
try:
    from src.visualization.plotter import plot_delay_bar # Reutiliza a mesma função de plot
    PLOTTING_ENABLED_STEP = True
except ImportError:
    PLOTTING_ENABLED_STEP = False

def execute_max_delay_analysis(args: argparse.Namespace, should_plot: bool):
    """ Executa e exibe a análise de atraso máximo histórico. """
    logger.info(f"Executando análise de atraso máximo histórico...")
    max_c = args.max_concurso
    plot_flag = should_plot and PLOTTING_ENABLED_STEP

    # Recebe a Series de atrasos máximos
    max_delays_series = calculate_max_delay(concurso_maximo=max_c)

    if max_delays_series is not None:
        print("\n--- Atraso Máximo Histórico das Dezenas ---")
        # Verifica se a Series não está vazia antes de imprimir
        if not max_delays_series.empty:
            print(max_delays_series.to_string())
            if plot_flag:
                 plot_delay_bar(max_delays_series, f"Atraso Máximo Histórico (até {max_c or 'Último'})", "barras_atraso_maximo")
        else:
            logger.warning("Série de atraso máximo calculada está vazia.")
    else:
         logger.error("Falha ao calcular o atraso máximo histórico.")
    logger.info("Análise de atraso máximo concluída.")