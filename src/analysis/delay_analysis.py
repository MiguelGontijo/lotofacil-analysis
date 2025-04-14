# src/analysis/delay_analysis.py

import pandas as pd
import numpy as np # Para std dev
from typing import Optional, Dict, List, Set, Tuple # Adicionado Tuple

from src.database_manager import read_data_from_db
from src.config import logger, NEW_BALL_COLUMNS

ALL_NUMBERS: List[int] = list(range(1, 26))
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS


def calculate_current_delay(concurso_maximo: Optional[int] = None) -> Optional[pd.Series]:
    """ Calcula o atraso atual das dezenas. Retorna Series ou None. """
    # (Código idêntico ao da versão anterior correta)
    logger.info(f"Calculando atraso atual até {concurso_maximo or 'último'}...")
    df = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo);
    if df is None or df.empty: return None
    if not all(col in df.columns for col in NEW_BALL_COLUMNS): return None
    effective_max_concurso_val = df['concurso'].max();
    if pd.isna(effective_max_concurso_val): return None
    effective_max_concurso = int(effective_max_concurso_val)
    logger.info(f"Ref. atraso: Concurso {effective_max_concurso}")
    last_seen: Dict[int, int] = {}; delays: Dict[int, object] = {}
    for index, row in df.iloc[::-1].iterrows():
        current_concurso_scan_val = row['concurso'];
        if pd.isna(current_concurso_scan_val): continue
        current_concurso_scan = int(current_concurso_scan_val)
        drawn_numbers: Set[int] = set(int(num) for num in row[NEW_BALL_COLUMNS].dropna().values)
        for number in ALL_NUMBERS:
            if number not in last_seen and number in drawn_numbers: last_seen[number] = current_concurso_scan
        if len(last_seen) == len(ALL_NUMBERS): break
    for number in ALL_NUMBERS:
        last_seen_concurso = last_seen.get(number)
        if last_seen_concurso is not None: delays[number] = effective_max_concurso - last_seen_concurso
        else: logger.warning(f"Dezena {number} não encontrada. Atraso NA."); delays[number] = pd.NA
    delay_series = pd.Series(delays, name='Atraso Atual').sort_index();
    try: delay_series = delay_series.astype('Int64')
    except: pass
    logger.info("Cálculo de atraso atual concluído."); return delay_series


def calculate_max_delay(concurso_maximo: Optional[int] = None) -> Optional[pd.Series]:
    """ Calcula o atraso máximo histórico. Retorna Series ou None. """
    # (Código idêntico ao da versão anterior correta)
    logger.info(f"Calculando atraso máximo histórico até {concurso_maximo or 'último'}...")
    df = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo);
    if df is None or df.empty: return None
    if not all(col in df.columns for col in NEW_BALL_COLUMNS): return None
    effective_max_concurso_val = df['concurso'].max(); first_concurso_val = df['concurso'].min()
    if pd.isna(effective_max_concurso_val) or pd.isna(first_concurso_val): return None
    effective_max_concurso = int(effective_max_concurso_val); first_concurso = int(first_concurso_val)
    last_seen_concurso: Dict[int, int] = {n: first_concurso - 1 for n in ALL_NUMBERS}; max_delay: Dict[int, int] = {n: 0 for n in ALL_NUMBERS}
    logger.info(f"Analisando concursos de {first_concurso} a {effective_max_concurso}...")
    for index, row in df.iterrows():
        current_concurso_val = row['concurso'];
        if pd.isna(current_concurso_val): continue
        current_concurso = int(current_concurso_val)
        drawn_numbers: Set[int] = set(int(num) for num in row[NEW_BALL_COLUMNS].dropna().values)
        for n in ALL_NUMBERS:
            if n in drawn_numbers:
                if last_seen_concurso[n] >= first_concurso: current_delay = current_concurso - last_seen_concurso[n] - 1; max_delay[n] = max(max_delay[n], current_delay)
                last_seen_concurso[n] = current_concurso
    logger.debug("Verificando atraso final...")
    for n in ALL_NUMBERS:
         if last_seen_concurso[n] >= first_concurso: final_delay = effective_max_concurso - last_seen_concurso[n]; max_delay[n] = max(max_delay[n], final_delay)
         else: logger.warning(f"Dezena {n} nunca vista."); max_delay[n] = effective_max_concurso - first_concurso + 1
    max_delay_series = pd.Series(max_delay, name='Atraso Máximo Histórico').sort_index().astype(int)
    logger.info("Cálculo de atraso máximo histórico concluído."); return max_delay_series


# --- NOVA FUNÇÃO ---
def calculate_delay_stats(concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Calcula estatísticas de atraso (intervalo entre aparições) para cada dezena.

    Args:
        concurso_maximo (Optional[int]): O último concurso a considerar.

    Returns:
        Optional[pd.DataFrame]: DataFrame com colunas 'media_atraso', 'std_dev_atraso',
                                'max_atraso_calculado' (igual a calculate_max_delay),
                                indexado por dezena (1-25), ou None se erro.
    """
    logger.info(f"Calculando estatísticas de atraso (média, std dev) até {concurso_maximo or 'último'}...")
    df = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo)
    if df is None or df.empty: return None
    if not all(col in df.columns for col in NEW_BALL_COLUMNS): return None

    effective_max_concurso = int(df['concurso'].max())
    first_concurso = int(df['concurso'].min())

    # Dicionário para armazenar a lista de atrasos de cada número
    delays_list: Dict[int, List[int]] = {n: [] for n in ALL_NUMBERS}
    last_seen_concurso: Dict[int, int] = {n: first_concurso - 1 for n in ALL_NUMBERS}

    logger.info(f"Analisando concursos de {first_concurso} a {effective_max_concurso} para stats de atraso...")
    for index, row in df.iterrows():
        current_concurso = int(row['concurso'])
        drawn_numbers = set(int(num) for num in row[NEW_BALL_COLUMNS].dropna().values)
        for n in ALL_NUMBERS:
            if n in drawn_numbers:
                if last_seen_concurso[n] >= first_concurso:
                     # Atraso = intervalo entre aparições
                     current_delay = current_concurso - last_seen_concurso[n] - 1
                     delays_list[n].append(current_delay)
                last_seen_concurso[n] = current_concurso

    # Calcula o atraso final para incluir na média/std dev?
    # Pode enviesar a média/std dev. Melhor não incluir por enquanto.
    # Ou incluir apenas se quisermos o Max Delay como parte do DF retornado.

    results_data = []
    for n in ALL_NUMBERS:
        delays = delays_list[n]
        if len(delays) > 1: # Precisa de pelo menos 2 atrasos para calcular std dev
            mean_delay = np.mean(delays)
            std_dev_delay = np.std(delays, ddof=1) # ddof=1 para sample std dev
            max_delay_calc = np.max(delays) if delays else 0
        elif len(delays) == 1: # Se só apareceu 1x (após a primeira), só temos 1 atraso
             mean_delay = delays[0]
             std_dev_delay = 0 # Ou NaN? Vamos usar 0
             max_delay_calc = delays[0]
        else: # Se nunca apareceu (depois da primeira vez) ou só apareceu 1 vez
            mean_delay = np.nan
            std_dev_delay = np.nan
            max_delay_calc = 0 # Ou o atraso máximo histórico? Vamos pegar do calculate_max_delay depois

        # Recalcula o max_delay incluindo o final para consistência
        max_delay_final = 0
        if last_seen_concurso[n] >= first_concurso:
             final_delay_val = effective_max_concurso - last_seen_concurso[n]
             max_delay_final = max(max_delay_calc, final_delay_val)
        elif not delays: # Nunca visto
             max_delay_final = effective_max_concurso - first_concurso + 1


        results_data.append({
            'dezena': n,
            'media_atraso': mean_delay,
            'std_dev_atraso': std_dev_delay,
            'max_atraso': max_delay_final # Inclui o max_delay completo
        })

    results_df = pd.DataFrame(results_data).set_index('dezena')
    logger.info("Cálculo de estatísticas de atraso concluído.")
    return results_df