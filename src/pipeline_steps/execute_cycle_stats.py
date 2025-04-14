# src/pipeline_steps/execute_cycle_stats.py

import pandas as pd
from src.config import logger
from src.analysis.cycle_analysis import run_cycle_frequency_analysis # Função principal está aqui

def execute_cycle_stats_analysis(cycles_summary: pd.DataFrame):
    """ Executa a análise de frequência dentro dos ciclos. """
    logger.info("Executando análise de stats por ciclo...")
    # A função importada já faz o cálculo e a impressão
    run_cycle_frequency_analysis(cycles_summary)
    logger.info("Análise de stats por ciclo concluída.")