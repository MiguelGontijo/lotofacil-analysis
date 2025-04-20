# src/analysis/cycle_analysis.py

import sqlite3
import pandas as pd
import numpy as np
from typing import Optional, List
import logging # Certifique-se que logging está importado

# Importa constantes e logger
try:
    from src.config import DB_PATH, DRAW_TABLE_NAME, CYCLES_TABLE_NAME, ALL_NUMBERS, LOGGER_NAME
    from src.database_manager import read_data_from_db # Importa read_data_from_db
except ImportError:
    # Fallbacks para ambiente de teste ou execução isolada
    DB_PATH = 'lotofacil.db'
    DRAW_TABLE_NAME = 'sorteios'
    CYCLES_TABLE_NAME = 'ciclos'
    ALL_NUMBERS = list(range(1, 26))
    LOGGER_NAME = __name__ # Ou um nome padrão
    def read_data_from_db(*args, **kwargs): return None # Mock simples se não encontrado

logger = logging.getLogger(LOGGER_NAME)


def get_cycles_df(concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """ Busca dados dos ciclos do banco de dados. """
    try:
        conn = sqlite3.connect(DB_PATH)
        query = f"SELECT * FROM {CYCLES_TABLE_NAME}"
        if concurso_maximo is not None:
            query += f" WHERE concurso_fim <= {concurso_maximo}"
        query += " ORDER BY numero_ciclo"
        df = pd.read_sql_query(query, conn)
        conn.close()
        logger.info(f"{len(df)} registros de ciclos lidos" + (f" até concurso {concurso_maximo}." if concurso_maximo else "."))
        return df
    except Exception as e:
        logger.error(f"Erro ao ler tabela de ciclos: {e}")
        return None


def _calculate_intra_cycle_delays(cycle_draws_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the intra-cycle delay for each number within a single cycle.
    Delay = contests since last seen *within this cycle*. 0 if present.
    Increments from 1 at the start if not present in the first draw.
    """
    if cycle_draws_df is None or cycle_draws_df.empty:
        return pd.DataFrame()

    all_numbers = set(range(1, 26))
    # Ensure ball columns exist
    ball_cols = [col for col in cycle_draws_df.columns if col.startswith('bola')]
    if not ball_cols:
         logger.warning("Nenhuma coluna 'bolaX' encontrada nos dados do ciclo.")
         return pd.DataFrame()

    melted_draws = cycle_draws_df.melt(
        id_vars=['concurso', 'data_sorteio'],
        value_vars=ball_cols, # Use dynamic ball columns
        value_name='numero'
    ).dropna()
    # Convert 'numero' to int after melt, handling potential errors
    melted_draws['numero'] = pd.to_numeric(melted_draws['numero'], errors='coerce').astype('Int64') # Use nullable Int64
    melted_draws.dropna(subset=['numero'], inplace=True)


    # Pivot to get presence matrix (concurso x numero)
    presence_matrix = pd.pivot_table(
        melted_draws, index='concurso', columns='numero', aggfunc='size', fill_value=0
    ).applymap(lambda x: 1 if x > 0 else 0)

    # Reindex to include all contests and numbers, fill missing numbers with 0 (not present)
    all_contests_in_df = cycle_draws_df['concurso'].unique()
    all_contests_in_df.sort() # Ensure contests are sorted
    presence_matrix = presence_matrix.reindex(index=all_contests_in_df, columns=list(all_numbers), fill_value=0)
    # presence_matrix = presence_matrix.sort_index() # Already sorted above


    delay_matrix = pd.DataFrame(0, index=presence_matrix.index, columns=presence_matrix.columns)
    last_seen_in_cycle = {} # Store contest number where number was last seen

    for contest in presence_matrix.index:
        for number in presence_matrix.columns:
            if presence_matrix.loc[contest, number] == 1:
                delay_matrix.loc[contest, number] = 0
                last_seen_in_cycle[number] = contest
            else:
                if number in last_seen_in_cycle:
                    # Calculate delay since last seen IN THIS CYCLE
                    delay_matrix.loc[contest, number] = contest - last_seen_in_cycle[number]
                else:
                    # Number not seen yet in this cycle. Delay is contests since cycle start.
                    cycle_start_contest = presence_matrix.index.min()
                    delay_matrix.loc[contest, number] = contest - cycle_start_contest + 1

    return delay_matrix


def calculate_historical_intra_cycle_delay_stats(
    cycles_df: pd.DataFrame, history_limit: int = 10
) -> Optional[pd.DataFrame]:
    """
    Calculates historical (avg, max, std) intra-cycle delay stats for each number
    based on the last 'history_limit' completed cycles.
    """
    logger.info(f"Calculando stats históricos de atraso intra-ciclo (Últimos {history_limit} ciclos)...")
    if cycles_df is None or cycles_df.empty:
        logger.warning("DataFrame de ciclos vazio fornecido.")
        return None
    if not all(col in cycles_df.columns for col in ['numero_ciclo', 'concurso_inicio', 'concurso_fim']):
        logger.error("DataFrame de ciclos não contém as colunas necessárias ('numero_ciclo', 'concurso_inicio', 'concurso_fim').")
        return None

    # Get the N most recent cycles
    recent_cycles = cycles_df.nlargest(history_limit, 'numero_ciclo')
    if recent_cycles.empty:
        logger.warning("Nenhum ciclo encontrado para calcular stats históricos de atraso intra-ciclo.")
        return None

    logger.info(f"Analisando {len(recent_cycles)} ciclos recentes para atraso intra-ciclo...")
    all_delays_by_number = {num: [] for num in range(1, 26)}

    # Iterate through the selected cycles
    for _, cycle_info in recent_cycles.iterrows():
        cycle_num = cycle_info['numero_ciclo']
        min_c = int(cycle_info['concurso_inicio'])
        max_c = int(cycle_info['concurso_fim'])

        # Read draw data for this specific cycle
        # Make sure read_data_from_db is correctly imported or available in scope
        cycle_draws_df = read_data_from_db(
            table_name=DRAW_TABLE_NAME,
            concurso_minimo=min_c,
            concurso_maximo=max_c
        )

        if cycle_draws_df is None or cycle_draws_df.empty:
            logger.warning(f"Nenhum dado de sorteio encontrado para o ciclo {cycle_num} ({min_c}-{max_c}). Pulando.")
            continue

        # Calculate delays for this single cycle
        intra_cycle_delays = _calculate_intra_cycle_delays(cycle_draws_df) # Calls the helper

        # Append delays for each number to the global list
        if not intra_cycle_delays.empty:
            for number in all_delays_by_number.keys():
                if number in intra_cycle_delays.columns:
                     # Collect ALL delay values for this number in this cycle
                    all_delays_by_number[number].extend(intra_cycle_delays[number].tolist())

    # Now calculate stats over the collected delays for all relevant cycles
    results = []
    for number, delays in all_delays_by_number.items():
        if not delays: # No data collected for this number
            results.append({
                'numero': number,
                'avg_hist_intra_delay': np.nan,
                'max_hist_intra_delay': np.nan,
                'std_hist_intra_delay': np.nan
            })
            continue

        # ======================================================
        # ADICIONE O LOG AQUI DENTRO DO LOOP, ANTES DOS CÁLCULOS:
        if number == 1:
            logger.info(f"DEBUG: Lista de atrasos calculada para Dezena 1: {delays}")
        # ======================================================

        # Calculate statistics
        avg_delay = np.mean(delays)
        max_delay = np.max(delays)
        # Check for sufficient data points for standard deviation
        std_delay = np.std(delays, ddof=1) if len(delays) > 1 else 0.0 # Use 0.0 for std dev if only 1 element

        results.append({
            'numero': number,
            'avg_hist_intra_delay': avg_delay,
            'max_hist_intra_delay': max_delay,
            'std_hist_intra_delay': std_delay
        })

    if not results:
        logger.warning("Nenhuma estatística de atraso intra-ciclo foi calculada.")
        return None

    hist_stats_df = pd.DataFrame(results)
    # Convert max delay to integer if possible, handle NaN
    hist_stats_df['max_hist_intra_delay'] = pd.to_numeric(hist_stats_df['max_hist_intra_delay'], errors='coerce').astype('Int64')
    hist_stats_df.set_index('numero', inplace=True)
    logger.info("Stats históricos de atraso intra-ciclo (limitado) concluídos.")
    return hist_stats_df

# Você pode adicionar outras funções de análise de ciclo aqui, se necessário.