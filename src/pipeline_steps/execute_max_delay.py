# src/pipeline_steps/execute_max_delay.py

import argparse
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

    max_delays = calculate_max_delay(concurso_maximo=max_c)
    if max_delays is not None:
        print("\n--- Atraso Máximo Histórico das Dezenas ---")
        print(max_delays.to_string())
        if plot_flag:
             plot_delay_bar(max_delays, f"Atraso Máximo Histórico (até {max_c or 'Último'})", "barras_atraso_maximo")
    else:
         logger.error("Falha ao calcular o atraso máximo histórico.")
    logger.info("Análise de atraso máximo concluída.")