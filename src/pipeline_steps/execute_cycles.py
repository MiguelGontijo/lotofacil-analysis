# src/pipeline_steps/execute_cycles.py

import argparse
import pandas as pd
from typing import Optional

from src.config import logger
# Importa apenas a função de identificação que retorna o DF
from src.analysis.cycle_analysis import identify_cycles
try:
    # Importa a função de plotagem
    from src.visualization.plotter import plot_cycle_duration_hist
    PLOTTING_ENABLED_STEP = True
except ImportError:
    PLOTTING_ENABLED_STEP = False

# Função para executar a identificação
def execute_cycle_identification() -> Optional[pd.DataFrame]:
    """ Executa a identificação de ciclos e retorna o DataFrame. """
    logger.info(f"Executando identificação de ciclos...")
    cycles_summary = identify_cycles() # Chama a função de análise
    if cycles_summary is None:
        logger.error("Falha na identificação de ciclos.")
    elif cycles_summary.empty:
        logger.info("Nenhum ciclo completo identificado.")
    else:
        logger.info(f"Identificação de ciclos concluída ({len(cycles_summary)} ciclos).")
    return cycles_summary # Retorna o DF (ou None/vazio)

# Função para exibir o sumário e plotar
def display_cycle_summary(cycles_summary: pd.DataFrame, args: argparse.Namespace, should_plot: bool):
    """ Exibe o resumo e estatísticas dos ciclos. """
    max_c = args.max_concurso
    plot_flag = should_plot and PLOTTING_ENABLED_STEP

    # Verifica se o DataFrame não é None e não está vazio
    if cycles_summary is not None and not cycles_summary.empty:
        # Filtra se necessário
        cycles_filtered = cycles_summary[cycles_summary['concurso_fim'] <= max_c].copy() if max_c else cycles_summary

        if not cycles_filtered.empty:
            print("\n--- Resumo dos Ciclos Completos ---")
            print(cycles_filtered.to_string(index=False))
            # Calcula e imprime estatísticas
            try:
                stats = cycles_filtered['duracao'].agg(['mean', 'min', 'max']).dropna()
                mean_str = f"{stats.get('mean', 'N/A'):.2f}" if 'mean' in stats else "N/A"
                min_str = f"{stats.get('min', 'N/A'):.0f}" if 'min' in stats else "N/A"
                max_str = f"{stats.get('max', 'N/A'):.0f}" if 'max' in stats else "N/A"
                print(f"\nStats Ciclos (até {max_c or 'último'}): {len(cycles_filtered)} ciclos, Média {mean_str}, Min {min_str}, Max {max_str}")
            except Exception as e:
                logger.error(f"Erro ao calcular estatísticas dos ciclos: {e}")

            # Plota o histograma
            if plot_flag:
                plot_cycle_duration_hist(cycles_filtered, "Duração dos Ciclos", "hist_duracao_ciclos")
        else:
            logger.info(f"Nenhum ciclo completo encontrado até o concurso {max_c}.")
    # else: # Log já feito em execute_cycle_identification
    #     logger.info("Nenhum ciclo completo identificado nos dados (DataFrame vazio).")