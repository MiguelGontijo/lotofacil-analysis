# src/pipeline_steps/execute_delay.py

import argparse
from src.config import logger
from src.analysis.delay_analysis import calculate_current_delay
try:
    from src.visualization.plotter import plot_delay_bar
    PLOTTING_ENABLED_STEP = True
except ImportError:
    PLOTTING_ENABLED_STEP = False


def execute_delay_analysis(args: argparse.Namespace, should_plot: bool):
    """ Executa e exibe a análise de atraso atual. """
    logger.info(f"Executando análise de atraso atual...")
    max_c = args.max_concurso
    plot_flag = should_plot and PLOTTING_ENABLED_STEP

    current_delays = calculate_current_delay(concurso_maximo=max_c)
    if current_delays is not None:
        print("\n--- Atraso Atual das Dezenas ---")
        print(current_delays.to_string())
        if plot_flag:
            plot_delay_bar(current_delays, f"Atraso Atual (Ref: {max_c or 'Último'})", "barras_atraso_atual")
    else:
         logger.error("Falha ao calcular o atraso atual.")
    logger.info("Análise de atraso atual concluída.")