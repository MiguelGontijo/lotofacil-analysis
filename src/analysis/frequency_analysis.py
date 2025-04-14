# src/analysis/frequency_analysis.py

import pandas as pd
from typing import Optional

from src.database_manager import read_data_from_db
from src.config import logger, NEW_BALL_COLUMNS

# Define ALL_NUMBERS localmente
ALL_NUMBERS = list(range(1, 26))
BASE_COLS = ['concurso'] + NEW_BALL_COLUMNS


def _get_data_for_period(concurso_minimo: Optional[int] = None,
                         concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """ Busca os dados base (concurso e bolas) para um período específico. """
    # (Código da função auxiliar permanece o mesmo)
    df = read_data_from_db(columns=BASE_COLS,
                             concurso_minimo=concurso_minimo,
                             concurso_maximo=concurso_maximo)
    if df is None or df.empty:
        period_str = f"[{concurso_minimo or 'início'} - {concurso_maximo or 'fim'}]"
        logger.warning(f"Nenhum dado base encontrado para o período {period_str}.")
        return None
    if not all(col in df.columns for col in NEW_BALL_COLUMNS):
        logger.error("Dados lidos não contêm colunas de bolas esperadas.")
        return None
    return df


def calculate_frequency(concurso_minimo: Optional[int] = None,
                        concurso_maximo: Optional[int] = None) -> Optional[pd.Series]:
    """
    Calcula a frequência de sorteio de cada dezena para um período específico.
    RETORNA: Series com frequência ou None.
    """
    period_str = f"[{concurso_minimo or 'início'} - {concurso_maximo or 'fim'}]"
    logger.info(f"Calculando frequência no período {period_str}...")
    df = _get_data_for_period(concurso_minimo, concurso_maximo)
    if df is None:
        return None

    melted_balls = df[NEW_BALL_COLUMNS].melt(value_name='number')['number'].dropna().astype(int)
    frequency = melted_balls.value_counts()
    frequency = frequency.reindex(ALL_NUMBERS, fill_value=0)
    frequency.sort_index(inplace=True)
    logger.info(f"Cálculo de frequência no período {period_str} concluído.")
    return frequency # <<< RETORNA A SERIES


def calculate_windowed_frequency(window_size: int, concurso_maximo: Optional[int] = None) -> Optional[pd.Series]:
    """
    Calcula a frequência de sorteio nas últimas 'window_size' dezenas.
    RETORNA: Series com frequência da janela ou None.
    """
    logger.info(f"Calculando frequência na janela de {window_size} concursos até {concurso_maximo or 'último'}...")
    df_all = _get_data_for_period(concurso_maximo=concurso_maximo)
    if df_all is None: return None

    actual_max_concurso = df_all['concurso'].max()
    if pd.isna(actual_max_concurso): return None # Checagem de segurança
    actual_max_concurso = int(actual_max_concurso)

    effective_max_concurso = actual_max_concurso
    if concurso_maximo:
        if concurso_maximo > actual_max_concurso:
             logger.warning(f"Max concurso {concurso_maximo} > último disponível {actual_max_concurso}.")
        effective_max_concurso = min(int(concurso_maximo), actual_max_concurso) # Garante que não passe do limite

    concurso_minimo_janela = effective_max_concurso - window_size + 1
    df_window = df_all[df_all['concurso'] >= concurso_minimo_janela].copy()

    if df_window.empty:
        logger.warning(f"Nenhum dado na janela [{concurso_minimo_janela} - {effective_max_concurso}].")
        return pd.Series(0, index=ALL_NUMBERS, name='frequency')

    logger.info(f"Analisando {len(df_window)} concursos na janela [{concurso_minimo_janela} - {effective_max_concurso}]")

    melted_balls = df_window[NEW_BALL_COLUMNS].melt(value_name='number')['number'].dropna().astype(int)
    frequency = melted_balls.value_counts()
    frequency = frequency.reindex(ALL_NUMBERS, fill_value=0)
    frequency.sort_index(inplace=True)
    logger.info(f"Cálculo de frequência na janela de {window_size} concluído.")
    return frequency # <<< RETORNA A SERIES


def calculate_cumulative_frequency_history(concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Calcula a frequência acumulada histórica.
    RETORNA: DataFrame com histórico ou None.
    """
    logger.info(f"Calculando histórico de frequência acumulada até {concurso_maximo or 'último'}...")
    df = read_data_from_db(columns=['concurso', 'data_sorteio'] + NEW_BALL_COLUMNS,
                             concurso_maximo=concurso_maximo)
    if df is None or df.empty: return None

    melted = df.melt(id_vars=['concurso'], value_vars=NEW_BALL_COLUMNS, value_name='number')
    melted = melted[['concurso', 'number']].dropna()
    melted['number'] = melted['number'].astype(int)

    counts_pivot = pd.pivot_table(melted, index='concurso', columns='number', aggfunc='size', fill_value=0)
    counts_pivot = counts_pivot.reindex(columns=ALL_NUMBERS, fill_value=0)
    cumulative_freq = counts_pivot.cumsum(axis=0)
    cumulative_freq.columns = [f'cum_freq_{i}' for i in ALL_NUMBERS]

    if 'data_sorteio' in df.columns:
         df_dates = df[['concurso', 'data_sorteio']].drop_duplicates(subset=['concurso']).set_index('concurso')
         cumulative_freq = df_dates.join(cumulative_freq, how='right')
         cumulative_freq.reset_index(inplace=True) # Garante concurso como coluna

    logger.info("Cálculo do histórico de frequência acumulada concluído.")
    return cumulative_freq # <<< RETORNA O DATAFRAME