# src/visualization/plotter.py

import matplotlib.pyplot as plt
import seaborn as sns # Opcional para estilos
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any

from src.config import logger, PLOT_DIR

# Configurações de Plot
DEFAULT_FIGSIZE = (10, 6)
DEFAULT_DPI = 100

def _setup_plot_style():
    """ Configura estilo visual dos gráficos (opcional). """
    try:
        sns.set_theme(style="whitegrid") # Usa tema do Seaborn se disponível
        logger.debug("Estilo Seaborn aplicado aos gráficos.")
    except NameError:
        logger.debug("Seaborn não disponível, usando estilo padrão Matplotlib.")
        plt.style.use('ggplot') # Ou outro estilo matplotlib

def _save_plot(filename: str, tight_layout: bool = True):
    """ Salva o gráfico atual em um arquivo no diretório de plots. """
    if not filename: return
    # Garante que o diretório de plots exista
    PLOT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = PLOT_DIR / f"{filename}.png"
    try:
        if tight_layout:
            plt.tight_layout()
        plt.savefig(filepath, dpi=DEFAULT_DPI, bbox_inches='tight')
        logger.info(f"Gráfico salvo em: {filepath}")
    except Exception as e:
        logger.error(f"Erro ao salvar gráfico '{filepath}': {e}")
    finally:
        plt.close() # Fecha a figura para liberar memória

def setup_plotting():
    """ Função chamada no início para configurar algo se necessário. """
    _setup_plot_style()
    logger.info("Setup de plotagem inicializado.")

# --- Funções de Plot Específicas ---

def plot_backtest_summary(summary_data: Dict[int | str, int], title: str, filename: str):
    """
    (Ainda não implementada - Causa o Warning na inicialização)
    Plota o resumo dos acertos de um backtest.
    """
    # TODO: Implementar lógica para gerar gráfico de barras com acertos (11 a 15)
    logger.warning(f"Função plot_backtest_summary ainda não implementada. Gráfico '{filename}' não gerado.")
    pass


def plot_histogram(data: pd.Series, title: str, xlabel: str, filename: str, bins: int = 15):
    """
    Plota um histograma para uma Series de dados.

    Args:
        data (pd.Series): Os dados a serem plotados.
        title (str): Título do gráfico.
        xlabel (str): Rótulo do eixo X.
        filename (str): Nome do arquivo para salvar (sem extensão).
        bins (int): Número de barras no histograma.
    """
    if data is None or data.empty:
        logger.warning(f"Dados vazios ou nulos para plotar histograma '{title}'.")
        return

    plt.figure(figsize=DEFAULT_FIGSIZE)
    # Remove NaNs antes de plotar
    data_cleaned = data.dropna()
    if data_cleaned.empty:
         logger.warning(f"Todos os dados eram nulos para histograma '{title}'.")
         plt.close(); return

    plt.hist(data_cleaned, bins=bins, edgecolor='black')
    plt.title(title, fontsize=14)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel("Frequência", fontsize=12)
    plt.grid(axis='y', alpha=0.75)
    _save_plot(filename)


def plot_group_bar_chart(group_stats_df: pd.DataFrame, window: int, title: str, filename: str):
    """
    Plota um gráfico de barras comparando a frequência média por grupo para uma janela.

    Args:
        group_stats_df (pd.DataFrame): DataFrame indexado por grupo com colunas W{X}_avg_freq.
        window (int): A janela (ex: 25, 100) a ser plotada.
        title (str): Título do gráfico.
        filename (str): Nome do arquivo para salvar.
    """
    col_name = f'W{window}_avg_freq'
    if group_stats_df is None or group_stats_df.empty or col_name not in group_stats_df.columns:
        logger.warning(f"Dados de stats de grupo para janela {window} inválidos ou vazios para plotar '{title}'.")
        return

    data_to_plot = group_stats_df[col_name].sort_values(ascending=False)

    plt.figure(figsize=DEFAULT_FIGSIZE)
    bars = plt.bar(data_to_plot.index, data_to_plot.values)
    plt.title(title, fontsize=14)
    plt.xlabel("Grupo de Dezenas", fontsize=12)
    plt.ylabel(f"Frequência Média (Últimos {window} Conc.)", fontsize=12)
    plt.xticks(rotation=0, ha='center') # Garante que nomes dos grupos fiquem legíveis
    # Adiciona valor em cima das barras
    plt.bar_label(bars, fmt='%.2f')
    _save_plot(filename)