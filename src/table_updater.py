# src/table_updater.py

import pandas as pd
import sqlite3
import numpy as np # Numpy pode ser útil para cálculos futuros aqui
from typing import List, Optional, Dict, Set

# Importa logger, constantes e ALL_NUMBERS do config
from src.config import logger, ALL_NUMBERS, DEFAULT_SNAPSHOT_INTERVALS, DATABASE_PATH, NEW_BALL_COLUMNS

# Define ALL_NUMBERS localmente COMO FALLBACK (embora deva vir do config)
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'DEFAULT_SNAPSHOT_INTERVALS' not in globals(): DEFAULT_SNAPSHOT_INTERVALS = [10, 25, 50, 100, 200, 300, 400, 500]

# Importa funções do DB Manager (SEM BASE_COLS)
from src.database_manager import (
    read_data_from_db, get_last_freq_snapshot_contest, save_freq_snapshot,
    get_closest_freq_snapshot, create_freq_snap_table, FREQ_SNAP_TABLE_NAME
)

# Define BASE_COLS localmente usando NEW_BALL_COLUMNS do config
# Garante que NEW_BALL_COLUMNS existe (deve vir do config)
if 'NEW_BALL_COLUMNS' not in globals(): NEW_BALL_COLUMNS = [f'b{i}' for i in range(1,16)] # Fallback
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS


def update_freq_geral_snap_table(intervals: List[int] = DEFAULT_SNAPSHOT_INTERVALS,
                                 force_rebuild: bool = False):
    """ Calcula e salva snapshots da frequência geral acumulada incrementalmente. """
    logger.info(f"Iniciando atualização snapshots de frequência...")
    create_freq_snap_table() # Garante que a tabela exista

    last_snapshot_contest = 0
    # Usa ALL_NUMBERS local/importado
    current_freq_counts = pd.Series(0, index=ALL_NUMBERS)

    if force_rebuild:
        logger.warning("REBUILD: Apagando snapshots de frequência.")
        try:
            with sqlite3.connect(DATABASE_PATH) as conn: conn.execute(f"DELETE FROM {FREQ_SNAP_TABLE_NAME};")
        except sqlite3.Error as e: logger.error(f"Erro limpar '{FREQ_SNAP_TABLE_NAME}': {e}"); return
    else:
        last_snapshot_contest_val = get_last_freq_snapshot_contest()
        if last_snapshot_contest_val is not None:
            last_snapshot_contest = last_snapshot_contest_val
            snap_info = get_closest_freq_snapshot(last_snapshot_contest)
            if snap_info:
                _, current_freq_counts = snap_info # Pega a Series do último snapshot
                logger.info(f"Continuando a partir do snapshot {last_snapshot_contest}.")
            else:
                logger.warning(f"Snapshot {last_snapshot_contest} não encontrado? Recalculando do início.")
                last_snapshot_contest = 0 # Força recalcular do início
        else:
             logger.info(f"Nenhum snapshot. Calculando do início.")
             last_snapshot_contest = 0

    start_processing_from = last_snapshot_contest + 1
    # Usa BASE_COLS definido localmente
    df_new_draws = read_data_from_db(columns=BASE_COLS, concurso_minimo=start_processing_from)

    if df_new_draws is None or df_new_draws.empty:
        logger.info(f"Nenhum sorteio novo após {last_snapshot_contest}. Snapshots atualizados.")
        return

    # Usa ALL_NUMBERS local/importado
    if not ALL_NUMBERS: logger.error("ALL_NUMBERS não está definido!"); return

    max_contest_in_data_val = df_new_draws['concurso'].max()
    if pd.isna(max_contest_in_data_val): logger.error("Não foi possível determinar max_contest."); return
    max_contest_in_data = int(max_contest_in_data_val)

    logger.info(f"Processando {len(df_new_draws)} sorteios ({start_processing_from} a {max_contest_in_data})...")

    snapshot_points_to_save = set()
    for interval in intervals:
        first_multiple = ((start_processing_from + interval - 1) // interval) * interval
        snapshot_points_to_save.update(range(first_multiple, max_contest_in_data + 1, interval))

    sorted_snapshot_points = sorted(list(snapshot_points_to_save))
    snapshot_idx = 0; processed_count = 0

    # Itera sobre os novos sorteios
    for index, row in df_new_draws.iterrows():
        current_concurso_val = row['concurso']
        if pd.isna(current_concurso_val): continue
        current_concurso = int(current_concurso_val)

        # Usa NEW_BALL_COLUMNS importado
        drawn_numbers = {int(num) for num in row[NEW_BALL_COLUMNS].dropna().values}

        # Incrementa contagem cumulativa
        for num in drawn_numbers:
            # Usa ALL_NUMBERS local/importado para checar índice
            if num in current_freq_counts.index: current_freq_counts[num] += 1
            else: current_freq_counts[num] = 1

        processed_count += 1
        # Salva snapshot se o concurso atual for um ponto definido
        if snapshot_idx < len(sorted_snapshot_points) and current_concurso == sorted_snapshot_points[snapshot_idx]:
            # Passa a cópia para garantir tipo correto e evitar modificar original
            save_freq_snapshot(current_concurso, current_freq_counts.copy().astype(int))
            snapshot_idx += 1
            if snapshot_idx % 50 == 0: logger.info(f"{snapshot_idx}/{len(sorted_snapshot_points)} snapshots salvos...")

        # Log de progresso geral
        if processed_count % 500 == 0: logger.info(f"Processados {processed_count}/{len(df_new_draws)} sorteios para snapshots...")


    logger.info(f"Atualização/Reconstrução da tabela '{FREQ_SNAP_TABLE_NAME}' concluída. {snapshot_idx} snapshots salvos/atualizados.")