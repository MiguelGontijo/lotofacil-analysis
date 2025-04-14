# src/visualization/plotter.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import logging
from src.config import logger

PLOT_OUTPUT_DIR = Path(__file__).parent.parent.parent / 'plots'

def setup_plotting():
    sns.set_theme(style="whitegrid")
    PLOT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def _save_plot(filename: str, title: str):
    full_path = PLOT_OUTPUT_DIR / f"{filename}.png"
    try:
        plt.tight_layout()
        plt.savefig(full_path)
        logger.info(f"Plot salvo: {full_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar o plot '{filename}.png': {e}")
    finally:
        plt.close()

def plot_frequency_bar(freq_series: pd.Series, title: str, filename: str):
    if freq_series is None or freq_series.empty: return
    logger.debug(f"Gerando plot de frequência: {title}")
    plt.figure(figsize=(12, 6))
    # --- AJUSTE AQUI ---
    # Usando hue=freq_series.index para colorir por dezena e legend=False
    bars = sns.barplot(x=freq_series.index, y=freq_series.values,
                       hue=freq_series.index, palette="viridis", legend=False)
    # --- FIM AJUSTE ---
    plt.title(title, fontsize=16)
    plt.xlabel("Dezena", fontsize=12)
    plt.ylabel("Frequência", fontsize=12)
    plt.xticks(ticks=range(len(freq_series.index)), labels=freq_series.index, rotation=0)
    for bar in bars.patches:
         bars.annotate(f"{bar.get_height():.0f}", (bar.get_x() + bar.get_width() / 2., bar.get_height()),
                       ha='center', va='center', size=8, xytext=(0, 5), textcoords='offset points')
    _save_plot(filename, title)

def plot_distribution_bar(dist_series: pd.Series, title: str, filename: str):
    if dist_series is None or dist_series.empty: return
    logger.debug(f"Gerando plot de distribuição: {title}")
    plt.figure(figsize=(10, 6))
    # --- AJUSTE AQUI ---
    # Usando hue=dist_series.index para colorir por categoria e legend=False
    bars = sns.barplot(x=dist_series.index, y=dist_series.values,
                       hue=dist_series.index, palette="magma", order=dist_series.index, legend=False)
    # --- FIM AJUSTE ---
    plt.title(title, fontsize=16)
    plt.xlabel("Distribuição", fontsize=12)
    plt.ylabel("Número de Ocorrências", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    for bar in bars.patches:
         bars.annotate(f"{bar.get_height():.0f}", (bar.get_x() + bar.get_width() / 2., bar.get_height()),
                       ha='center', va='center', size=9, xytext=(0, 5), textcoords='offset points')
    _save_plot(filename, title)

def plot_cycle_duration_hist(cycles_df: pd.DataFrame, title: str, filename: str):
    if cycles_df is None or cycles_df.empty or 'duracao' not in cycles_df.columns: return
    logger.debug(f"Gerando histograma de duração de ciclos: {title}")
    plt.figure(figsize=(10, 6))
    min_d, max_d = cycles_df['duracao'].min(), cycles_df['duracao'].max()
    bins = range(min_d, max_d + 2)
    # Histplot não usa 'palette' da mesma forma que barplot, então não precisa de 'hue' aqui
    ax = sns.histplot(data=cycles_df, x='duracao', bins=bins, kde=False, palette="rocket", discrete=True)
    plt.title(title, fontsize=16)
    plt.xlabel("Duração do Ciclo (em concursos)", fontsize=12)
    plt.ylabel("Número de Ciclos", fontsize=12)
    plt.xticks(range(min_d, max_d + 1))
    for p in ax.patches:
        if p.get_height() > 0:
            ax.annotate(f'{p.get_height():.0f}', (p.get_x() + p.get_width() / 2., p.get_height()),
                        ha='center', va='center', size=9, xytext=(0, 5), textcoords='offset points')
    _save_plot(filename, title)

def plot_delay_bar(delay_series: pd.Series, title: str, filename: str):
    if delay_series is None or delay_series.empty: return
    delay_series_cleaned = delay_series.dropna()
    if delay_series_cleaned.empty: return
    logger.debug(f"Gerando plot de barras de atraso: {title}")
    plt.figure(figsize=(12, 6))
    x_values = delay_series_cleaned.index.astype(int) # Usado para x e hue
    y_values = delay_series_cleaned.values.astype(int)
    # --- AJUSTE AQUI ---
    # Usando hue=x_values para colorir por dezena e legend=False
    bars = sns.barplot(x=x_values, y=y_values, hue=x_values, palette="coolwarm", legend=False)
    # --- FIM AJUSTE ---
    plt.title(title, fontsize=16)
    plt.xlabel("Dezena", fontsize=12)
    plt.ylabel("Atraso Atual (em concursos)", fontsize=12)
    plt.xticks(ticks=range(len(x_values)), labels=x_values, rotation=0)
    for bar in bars.patches:
         bars.annotate(f"{bar.get_height():.0f}", (bar.get_x() + bar.get_width() / 2., bar.get_height()),
                       ha = 'center', va = 'center', size=8, xytext = (0, 5), textcoords = 'offset points')
    _save_plot(filename, title)

# Chama setup ao importar o módulo
setup_plotting()