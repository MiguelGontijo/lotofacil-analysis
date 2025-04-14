# src/pipeline_steps/execute_cycles.py

import argparse
import pandas as pd
from typing import Optional

from src.config import logger
# Importa a função que LÊ os ciclos da tabela
from src.analysis.cycle_analysis import get_cycles_df
try:
    from src.visualization.plotter import plot_cycle_duration_hist
    PLOTTING_ENABLED_STEP = True
except ImportError:
    PLOTTING_ENABLED_STEP = False

# Esta função apenas LÊ e retorna, o orchestrator decide se chama display
def execute_cycle_identification(args: argparse.Namespace) -> Optional[pd.DataFrame]:
    """ Lê os ciclos da tabela 'ciclos' até o max_concurso especificado. """
    logger.info(f"Lendo ciclos da tabela até concurso {args.max_concurso or 'último'}...")
    # Lê os ciclos JÁ CALCULADOS da tabela
    cycles_summary = get_cycles_df(concurso_maximo=args.max_concurso)
    if cycles_summary is None:
        logger.error("Falha ao ler ciclos da tabela 'ciclos'. Execute --update-cycles?")
    elif cycles_summary.empty:
        logger.info("Nenhum ciclo encontrado na tabela 'ciclos' para o período.")
    else:
        logger.info(f"{len(cycles_summary)} ciclos lidos da tabela.")
    return cycles_summary # Retorna o DF lido (pode ser None ou vazio)

# Função para exibir o sumário (recebe o DF lido)
def display_cycle_summary(cycles_summary: pd.DataFrame, args: argparse.Namespace, should_plot: bool):
    """ Exibe o resumo e estatísticas dos ciclos lidos da tabela. """
    max_c = args.max_concurso # Usado apenas para o título/log
    plot_flag = should_plot and PLOTTING_ENABLED_STEP

    if not cycles_summary.empty:
        print("\n--- Resumo dos Ciclos Completos (da Tabela 'ciclos') ---")
        print(cycles_summary.to_string(index=False))
        try:
            stats = cycles_summary['duracao'].agg(['mean', 'min', 'max']).dropna()
            mean_str = f"{stats.get('mean', 'N/A'):.2f}" if 'mean' in stats else "N/A"
            min_str = f"{stats.get('min', 'N/A'):.0f}" if 'min' in stats else "N/A"
            max_str = f"{stats.get('max', 'N/A'):.0f}" if 'max' in stats else "N/A"
            print(f"\nStats Ciclos (lidos até {max_c or 'último'}): {len(cycles_summary)} ciclos, Média {mean_str}, Min {min_str}, Max {max_str}")
        except Exception as e: logger.error(f"Erro ao calcular stats dos ciclos: {e}")

        if plot_flag:
            plot_cycle_duration_hist(cycles_summary, "Duração dos Ciclos (Tabela 'ciclos')", "hist_duracao_ciclos_tabela")
    # else: O log já foi feito em execute_cycle_identification