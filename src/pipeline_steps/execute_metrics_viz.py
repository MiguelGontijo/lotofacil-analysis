# src/pipeline_steps/execute_metrics_viz.py

import argparse
from typing import Dict, Any, Optional
import pandas as pd

from src.config import logger, DEFAULT_GROUP_WINDOWS, DEFAULT_RANK_TREND_LOOKBACK
from src.visualization.plotter import plot_histogram, plot_group_bar_chart

# Fallbacks se config falhar
if 'DEFAULT_GROUP_WINDOWS' not in globals(): DEFAULT_GROUP_WINDOWS = [25, 100]
if 'DEFAULT_RANK_TREND_LOOKBACK' not in globals(): DEFAULT_RANK_TREND_LOOKBACK = 50

def execute_metrics_visualization(analysis_results: Optional[Dict[str, Any]]):
    """
    Gera visualizações para métricas chave calculadas pelo agregador.

    Args:
        analysis_results (Optional[Dict[str, Any]]): O dicionário retornado por
                                                      get_consolidated_analysis.
    """
    if analysis_results is None:
        logger.error("Resultados da análise não disponíveis para visualização.")
        return

    logger.info("Gerando visualizações de métricas chave...")

    # 1. Histograma do Desvio Padrão do Atraso
    delay_std_dev = analysis_results.get('delay_std_dev')
    if isinstance(delay_std_dev, pd.Series):
        plot_histogram(delay_std_dev,
                       title="Distribuição do Desvio Padrão do Atraso Histórico",
                       xlabel="Desvio Padrão do Atraso (Concursos)",
                       filename="hist_delay_std_dev")
    else: logger.warning("Dados 'delay_std_dev' não encontrados ou inválidos para plot.")

    # 2. Histograma da Frequência de Fechamento
    closing_freq = analysis_results.get('closing_freq')
    if isinstance(closing_freq, pd.Series):
         plot_histogram(closing_freq,
                       title="Distribuição da Frequência como Dezena de Fechamento",
                       xlabel="Nº de Vezes que Fechou Ciclo",
                       filename="hist_closing_freq")
    else: logger.warning("Dados 'closing_freq' não encontrados ou inválidos para plot.")

    # 3. Histograma da Tendência de Rank Geral
    rank_trend = analysis_results.get('rank_trend')
    if isinstance(rank_trend, pd.Series):
        plot_histogram(rank_trend,
                       title=f"Distribuição da Tendência de Rank (Lookback {DEFAULT_RANK_TREND_LOOKBACK})",
                       xlabel="Variação no Rank (Positivo = Melhorou)",
                       filename="hist_rank_trend")
    else: logger.warning("Dados 'rank_trend' não encontrados ou inválidos para plot.")

    # 4. Gráfico de Barras da Frequência Média por Grupo
    group_stats_df = analysis_results.get('group_freq_stats_df')
    if isinstance(group_stats_df, pd.DataFrame):
         # Plota para cada janela padrão de grupo
         for window in DEFAULT_GROUP_WINDOWS:
              plot_group_bar_chart(group_stats_df, window,
                                   title=f"Frequência Média Recente por Grupo (Últimos {window} Concursos)",
                                   filename=f"bar_group_freq_W{window}")
    else: logger.warning("Dados 'group_freq_stats_df' não encontrados ou inválidos para plot.")

    logger.info("Visualizações de métricas chave concluídas (verificar pasta 'plots').")