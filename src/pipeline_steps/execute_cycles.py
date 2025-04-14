# src/pipeline_steps/execute_cycles.py

import argparse
import pandas as pd
from typing import Optional

from src.config import logger
from src.analysis.cycle_analysis import identify_cycles
try:
    from src.visualization.plotter import plot_cycle_duration_hist
    PLOTTING_ENABLED_STEP = True
except ImportError:
    PLOTTING_ENABLED_STEP = False

def execute_cycle_identification() -> Optional[pd.DataFrame]:
    """ Executa a identificação de ciclos e retorna o DataFrame. """
    logger.info(f"Executando identificação de ciclos...")
    cycles_summary = identify_cycles()
    if cycles_summary is None:
        logger.error("Falha na identificação de ciclos.")
    else:
        logger.info("Identificação de ciclos concluída.")
    return cycles_summary

def display_cycle_summary(cycles_summary: pd.DataFrame, args: argparse.Namespace, should_plot: bool):
    """ Exibe o resumo e estatísticas dos ciclos. """
    max_c = args.max_concurso
    plot_flag = should_plot and PLOTTING_ENABLED_STEP

    if not cycles_summary.empty:
        # Filtra se necessário
        cycles_filtered = cycles_summary[cycles_summary['concurso_fim'] <= max_c].copy() if max_c else cycles_summary

        if not cycles_filtered.empty:
            print("\n--- Resumo dos Ciclos Completos ---")
            print(cycles_filtered.to_string(index=False))
            # Calcula e imprime estatísticas
            stats = cycles_filtered['duracao'].agg(['mean', 'min', 'max'])
            print(f"\nStats Ciclos (até {max_c or 'último'}): {len(cycles_filtered)} ciclos, Média {stats['mean']:.2f}, Min {stats['min']}, Max {stats['max']}")
            # Plota o histograma
            if plot_flag:
                plot_cycle_duration_hist(cycles_filtered, "Duração dos Ciclos", "hist_duracao_ciclos")
        else:
            logger.info(f"Nenhum ciclo completo encontrado até o concurso {max_c}.")
    else:
        logger.info("Nenhum ciclo completo identificado nos dados (DataFrame vazio).")