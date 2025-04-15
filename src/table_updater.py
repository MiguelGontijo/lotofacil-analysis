# src/table_updater.py

import pandas as pd
import sqlite3
import numpy as np
from typing import List, Optional, Dict, Set

# Importa constantes e logger do config
# Garante que ALL_NUMBERS e NEW_BALL_COLUMNS sejam importados
from src.config import (
    logger, ALL_NUMBERS, DEFAULT_SNAPSHOT_INTERVALS,
    DATABASE_PATH, NEW_BALL_COLUMNS
)

# Fallbacks (caso as constantes não sejam importadas corretamente do config)
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'DEFAULT_SNAPSHOT_INTERVALS' not in globals(): DEFAULT_SNAPSHOT_INTERVALS = [10, 25, 50, 100, 200, 300, 400, 500]
if 'NEW_BALL_COLUMNS' not in globals(): NEW_BALL_COLUMNS = [f'b{i}' for i in range(1,16)]

# Importa funções do DB Manager (SEM importar BASE_COLS daqui)
from src.database_manager import (
    read_data_from_db, get_last_freq_snapshot_contest, save_freq_snapshot,
    get_closest_freq_snapshot, create_freq_snap_table, FREQ_SNAP_TABLE_NAME
    # BASE_COLS NÃO é importado daqui
)

# Define BASE_COLS localmente usando NEW_BALL_COLUMNS (importado do config)
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS


def update_freq_geral_snap_table(intervals: List[int] = DEFAULT_SNAPSHOT_INTERVALS,
                                 force_rebuild: bool = False):
    """ Calcula e salva snapshots da frequência geral acumulada incrementalmente. """
    logger.info(f"Iniciando atualização snapshots de frequência geral...")
    # Usa constante FREQ_SNAP_TABLE_NAME importada do config ou definida no db_manager
    create_freq_snap_table() # Garante que a tabela exista

    last_snapshot_contest = 0
    # Usa ALL_NUMBERS local ou do config
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
            if snap_info:
                _, current_freq_counts = snap_info # Pega a Series do último snapshot
                logger.info(f"Continuando a partir do snapshot {last_snapshot_contest}.")
            else:
                logger.warning(f"Snapshot {last_snapshot_contest} não encontrado? Recalculando do início.")
                last_snapshot_contest = 0 # Força recalcular do início
        else:
             logger.info(f"Nenhum snapshot encontrado. Calculando do início.")
             last_snapshot_contest = 0

    start_processing_from = last_snapshot_contest + 1
    # Usa BASE_COLS definido localmente
    df_new_draws = read_data_from_db(columns=BASE_COLS, concurso_minimo=start_processing_from)

    if df_new_draws is None or df_new_draws.empty:
        logger.info(f"Nenhum novo sorteio encontrado após o concurso {last_snapshot_contest}. Snapshots atualizados.")
        return

    # Usa ALL_NUMBERS local ou do config
    if not ALL_NUMBERS: logger.error("ALL_NUMBERS não está definido!"); return

    max_contest_in_data_val = df_new_draws['concurso'].max()
    if pd.isna(max_contest_in_data_val): logger.error("Não foi possível determinar max_contest."); return
    max_contest_in_data = int(max_contest_in_data_val)

    logger.info(f"Processando {len(df_new_draws)} sorteios ({start_processing_from} a {max_contest_in_data})...")

    # Gera a lista de pontos onde salvar snapshots no range de dados novos
    snapshot_points_to_save = set()
    for interval in intervals:
        # Garante que interval é positivo para evitar loop infinito ou erro
        if interval <= 0: continue
        first_multiple_in_range = ((start_processing_from + interval - 1) // interval) * interval
        snapshot_points_to_save.update(range(first_multiple_in_range, max_contest_in_data + 1, interval))

    sorted_snapshot_points = sorted(list(snapshot_points_to_save))
    snapshot_idx = 0; processed_count = 0
    snapshots_saved_count = 0

    # Itera sobre os novos sorteios
    for index, row in df_new_draws.iterrows():
        current_concurso_val = row['concurso']
        if pd.isna(current_concurso_val): continue
        current_concurso = int(current_concurso_val)

        # Usa NEW_BALL_COLUMNS importado
        drawn_numbers = {int(num) for num in row[NEW_BALL_COLUMNS].dropna().values}

        # Incrementa contagem cumulativa
        for num in drawn_numbers:
            if num in current_freq_counts.index: current_freq_counts[num] += 1
            # else: current_freq_counts[num] = 1 # Não deve acontecer

        processed_count += 1
        # Salva snapshot se o concurso atual for um ponto definido
        if snapshot_idx < len(sorted_snapshot_points) and current_concurso == sorted_snapshot_points[snapshot_idx]:
            save_freq_snapshot(current_concurso, current_freq_counts.copy().astype(int)) # Salva cópia
            snapshots_saved_count += 1
            snapshot_idx += 1
            if snapshots_saved_count % 50 == 0: logger.info(f"{snapshots_saved_count}/{len(sorted_snapshot_points)} snapshots salvos...")

        if processed_count % 500 == 0: logger.info(f"Processados {processed_count}/{len(df_new_draws)} sorteios para snapshots...")

    logger.info(f"Atualização/Reconstrução da tabela '{FREQ_SNAP_TABLE_NAME}' concluída. {snapshots_saved_count} snapshots salvos/atualizados neste lote.")