# src/visualization/plotter.py

import matplotlib.pyplot as plt
import seaborn as sns # Opcional
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any

from src.config import logger, PLOT_DIR, ALL_NUMBERS # Importa ALL_NUMBERS se usado abaixo

# Configurações de Plot
DEFAULT_FIGSIZE = (10, 6)
DEFAULT_DPI = 100

def _setup_plot_style():
    """ Configura estilo visual (mantido). """
    try: sns.set_theme(style="whitegrid"); logger.debug("Estilo Seaborn aplicado.")
    except NameError: logger.debug("Seaborn não disponível."); plt.style.use('ggplot')
    except Exception as e: logger.warning(f"Erro ao configurar estilo de plot: {e}")


def _save_plot(filename: str, tight_layout: bool = True):
    """ Salva o gráfico atual (mantido). """
    if not filename: return
    PLOT_DIR.mkdir(parents=True, exist_ok=True); filepath = PLOT_DIR / f"{filename}.png"
    try:
        if tight_layout: plt.tight_layout()
        plt.savefig(filepath, dpi=DEFAULT_DPI, bbox_inches='tight'); logger.info(f"Gráfico salvo: {filepath}")
    except Exception as e: logger.error(f"Erro salvar gráfico '{filepath}': {e}")
    finally: plt.close()


def setup_plotting():
    """ Função inicial de setup (mantido). """
    _setup_plot_style(); logger.info("Setup de plotagem inicializado.")


# --- Funções de Plot Específicas ---

# <<< PLACEHOLDER ADICIONADO >>>
def plot_backtest_summary(summary_data: Dict[int | str, int], title: str, filename: str):
    """ Plota o resumo dos acertos de um backtest (PLACEHOLDER). """
    logger.warning(f"Função plot_backtest_summary ainda não implementada. Gráfico '{filename}' não gerado.")
    # Cria uma figura vazia apenas para não dar erro se for chamada
    plt.figure(figsize=(5, 1)); plt.text(0.5, 0.5, 'Plot não implementado', ha='center', va='center'); plt.axis('off');
    _save_plot(filename + "_placeholder", tight_layout=False) # Salva um placeholder
    # pass # Ou simplesmente não faz nada


def plot_histogram(data: Optional[pd.Series], title: str, xlabel: str, filename: str, bins: int = 15):
    """ Plota um histograma para uma Series (com checagem de None). """
    if data is None or data.empty: logger.warning(f"Dados vazios/nulos para hist '{title}'."); return
    plt.figure(figsize=DEFAULT_FIGSIZE)
    data_cleaned = data.dropna()
    if data_cleaned.empty: logger.warning(f"Dados nulos/vazios após dropna para hist '{title}'."); plt.close(); return
    try:
        plt.hist(data_cleaned, bins=bins, edgecolor='black'); plt.title(title, fontsize=14); plt.xlabel(xlabel, fontsize=12); plt.ylabel("Frequência", fontsize=12); plt.grid(axis='y', alpha=0.75); _save_plot(filename)
    except Exception as e: logger.error(f"Erro ao gerar histograma '{title}': {e}"); plt.close()


def plot_group_bar_chart(group_stats_df: Optional[pd.DataFrame], window: int, title: str, filename: str):
    """ Plota gráfico de barras para stats de grupo (com checagem de None). """
    col_name = f'W{window}_avg_freq'
    if group_stats_df is None or group_stats_df.empty or col_name not in group_stats_df.columns: logger.warning(f"Dados stats grupo W{window} inválidos p/ plot '{title}'."); return
    data_to_plot = group_stats_df[col_name].sort_values(ascending=False)
    plt.figure(figsize=DEFAULT_FIGSIZE)
    try:
        bars = plt.bar(data_to_plot.index, data_to_plot.values); plt.title(title, fontsize=14); plt.xlabel("Grupo de Dezenas", fontsize=12); plt.ylabel(f"Frequência Média (Últimos {window} Conc.)", fontsize=12); plt.xticks(rotation=0, ha='center'); plt.bar_label(bars, fmt='%.2f'); _save_plot(filename)
    except Exception as e: logger.error(f"Erro ao gerar bar chart grupo '{title}': {e}"); plt.close()