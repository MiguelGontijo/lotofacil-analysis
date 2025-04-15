# src/table_updater.py

import pandas as pd
import sqlite3
import numpy as np
from typing import List, Optional, Dict, Set

# Importa constantes e logger do config
from src.config import (
    logger, ALL_NUMBERS, DEFAULT_SNAPSHOT_INTERVALS,
    DATABASE_PATH, NEW_BALL_COLUMNS, TABLE_NAME # Usa TABLE_NAME para ler sorteios
)

# Fallbacks
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'DEFAULT_SNAPSHOT_INTERVALS' not in globals(): DEFAULT_SNAPSHOT_INTERVALS = [10, 25, 50, 100, 200, 300, 400, 500]
if 'NEW_BALL_COLUMNS' not in globals(): NEW_BALL_COLUMNS = [f'b{i}' for i in range(1,16)]

# Importa funções do DB Manager
from src.database_manager import (
    read_data_from_db, get_last_freq_snapshot_contest, save_freq_snapshot,
    get_closest_freq_snapshot, create_freq_snap_table, FREQ_SNAP_TABLE_NAME
)

# Define BASE_COLS localmente usando NEW_BALL_COLUMNS
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS


def update_freq_geral_snap_table(intervals: List[int] = DEFAULT_SNAPSHOT_INTERVALS,
                                 force_rebuild: bool = False):
    """ Calcula e salva snapshots da frequência geral acumulada incrementalmente. """
    logger.info(f"Iniciando atualização snapshots de frequência geral...")
    create_freq_snap_table()

    last_snapshot_contest = 0
    current_freq_counts = pd.Series(0, index=ALL_NUMBERS)

    if force_rebuild:
        logger.warning(f"REBUILD: Apagando snapshots de '{FREQ_SNAP_TABLE_NAME}'.")
        try:
            with sqlite3.connect(DATABASE_PATH) as conn: conn.execute(f"DELETE FROM {FREQ_SNAP_TABLE_NAME};")
        except sqlite3.Error as e: logger.error(f"Erro limpar '{FREQ_SNAP_TABLE_NAME}': {e}"); return
    else:
        last_snapshot_contest_val = get_last_freq_snapshot_contest()
        if last_snapshot_contest_val is not None:
            last_snapshot_contest = last_snapshot_contest_val
            snap_info = get_closest_freq_snapshot(last_snapshot_contest)
            if snap_info: _, current_freq_counts = snap_info; logger.info(f"Continuando do snapshot {last_snapshot_contest}.")
            else: logger.warning(f"Snapshot {last_snapshot_contest} não encontrado? Recalculando."); last_snapshot_contest = 0
        else: logger.info(f"Nenhum snapshot. Calculando do início."); last_snapshot_contest = 0

    start_processing_from = last_snapshot_contest + 1
    # Lê da tabela principal 'sorteios'
    df_new_draws = read_data_from_db(table_name=TABLE_NAME, columns=BASE_COLS, concurso_minimo=start_processing_from)

    if df_new_draws is None or df_new_draws.empty:
        logger.info(f"Nenhum sorteio novo após {last_snapshot_contest}."); return

    max_contest_in_data_val = df_new_draws['concurso'].max()
    if pd.isna(max_contest_in_data_val): logger.error("max_contest inválido."); return
    max_contest_in_data = int(max_contest_in_data_val)

    logger.info(f"Processando {len(df_new_draws)} sorteios ({start_processing_from} a {max_contest_in_data})...")

    snapshot_points_to_save = set()
    for interval in intervals:
        if interval <= 0: continue
        first_multiple = ((start_processing_from + interval - 1) // interval) * interval
        snapshot_points_to_save.update(range(first_multiple, max_contest_in_data + 1, interval))

    sorted_snapshot_points = sorted(list(snapshot_points_to_save))
    snapshot_idx = 0; processed_count = 0; snapshots_saved_count = 0

    # Garante que current_freq_counts seja de inteiros
    current_freq_counts = current_freq_counts.astype(int)

    for index, row in df_new_draws.iterrows():
        current_concurso_val = row['concurso']
        if pd.isna(current_concurso_val): continue
        current_concurso = int(current_concurso_val)
        drawn_numbers = {int(num) for num in row[NEW_BALL_COLUMNS].dropna().values}

        for num in drawn_numbers:
            if num in current_freq_counts.index: current_freq_counts[num] += 1
            # else: current_freq_counts[num] = 1 # Não deve acontecer

        processed_count += 1
        if snapshot_idx < len(sorted_snapshot_points) and current_concurso == sorted_snapshot_points[snapshot_idx]:
            save_freq_snapshot(current_concurso, current_freq_counts.copy()) # Salva cópia
            snapshots_saved_count += 1
            snapshot_idx += 1
            if snapshots_saved_count % 50 == 0: logger.info(f"{snapshots_saved_count}/{len(sorted_snapshot_points)} snapshots salvos...")
        if processed_count % 500 == 0: logger.info(f"Processados {processed_count}/{len(df_new_draws)} sorteios...")

    logger.info(f"Atualização/Reconstrução da tabela '{FREQ_SNAP_TABLE_NAME}' concluída. {snapshots_saved_count} snapshots salvos/atualizados.")