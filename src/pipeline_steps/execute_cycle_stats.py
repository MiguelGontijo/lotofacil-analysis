# src/pipeline_steps/execute_cycle_stats.py

import pandas as pd
from src.config import logger
# Importa a função que calcula E IMPRIME as stats por ciclo
from src.analysis.cycle_analysis import run_cycle_frequency_analysis

def execute_cycle_stats_analysis(cycles_summary: pd.DataFrame):
    """ Executa a análise de frequência dentro dos ciclos. """
    logger.info("Executando análise de stats por ciclo...")
    if cycles_summary is None or cycles_summary.empty:
        logger.warning("DataFrame de ciclos vazio. Análise de stats por ciclo não realizada.")
        return
    # A função importada já faz o cálculo e a impressão
    run_cycle_frequency_analysis(cycles_summary)
    # O log de conclusão já está dentro de run_cycle_frequency_analysis
    # logger.info("Análise de stats por ciclo concluída.")