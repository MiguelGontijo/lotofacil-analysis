# src/table_updater.py

import pandas as pd
import sqlite3
import numpy as np
from typing import List, Optional, Dict, Set

# Importa constantes e logger
from src.config import (
    logger, ALL_NUMBERS, DEFAULT_SNAPSHOT_INTERVALS, DATABASE_PATH,
    NEW_BALL_COLUMNS, TABLE_NAME
)

# Fallbacks
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'DEFAULT_SNAPSHOT_INTERVALS' not in globals(): DEFAULT_SNAPSHOT_INTERVALS = [10, 25, 50, 100, 200, 300, 400, 500]
if 'NEW_BALL_COLUMNS' not in globals(): NEW_BALL_COLUMNS = [f'b{i}' for i in range(1,16)]
if 'TABLE_NAME' not in globals(): TABLE_NAME = 'sorteios'

# Importa funções do DB Manager
from src.database_manager import (
    read_data_from_db, get_last_freq_snapshot_contest, save_freq_snapshot,
    get_closest_freq_snapshot, create_freq_snap_table, FREQ_SNAP_TABLE_NAME,
    create_chunk_stats_final_table, save_chunk_final_stats_row, # <<< Novas
    get_last_contest_in_chunk_stats_final, get_chunk_final_stats_table_name # <<< Novas
)

BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS


# --- update_freq_geral_snap_table ---
# (Código idêntico ao da última versão correta)
def update_freq_geral_snap_table(intervals: List[int] = DEFAULT_SNAPSHOT_INTERVALS, force_rebuild: bool = False):
    logger.info(f"Iniciando atualização snapshots de frequência geral...")
    create_freq_snap_table(); last_snapshot_contest = 0; current_freq_counts = pd.Series(0, index=ALL_NUMBERS)
    if force_rebuild:
        logger.warning(f"REBUILD: Apagando snapshots de '{FREQ_SNAP_TABLE_NAME}'.");
        try:
            with sqlite3.connect(DATABASE_PATH) as conn: conn.execute(f"DELETE FROM {FREQ_SNAP_TABLE_NAME};")
        except sqlite3.Error as e: logger.error(f"Erro limpar '{FREQ_SNAP_TABLE_NAME}': {e}"); return
    else:
        last_snapshot_contest_val = get_last_freq_snapshot_contest()
        if last_snapshot_contest_val is not None:
            last_snapshot_contest = last_snapshot_contest_val; snap_info = get_closest_freq_snapshot(last_snapshot_contest)
            if snap_info: _, current_freq_counts = snap_info; logger.info(f"Continuando do snapshot {last_snapshot_contest}.")
            else: logger.warning(f"Snapshot {last_snapshot_contest} não encontrado? Recalculando."); last_snapshot_contest = 0
        else: logger.info(f"Nenhum snapshot. Calculando do início."); last_snapshot_contest = 0
    start_processing_from = last_snapshot_contest + 1
    df_new_draws = read_data_from_db(table_name=TABLE_NAME, columns=BASE_COLS, concurso_minimo=start_processing_from)
    if df_new_draws is None or df_new_draws.empty: logger.info(f"Nenhum sorteio novo após {last_snapshot_contest}."); return
    max_contest_in_data_val = df_new_draws['concurso'].max();
    if pd.isna(max_contest_in_data_val): logger.error("max_contest inválido."); return
    max_contest_in_data = int(max_contest_in_data_val)
    logger.info(f"Processando {len(df_new_draws)} sorteios ({start_processing_from} a {max_contest_in_data})...")
    snapshot_points_to_save = set();
    for interval in intervals:
        if interval <= 0: continue
        first_multiple = ((start_processing_from + interval - 1) // interval) * interval
        snapshot_points_to_save.update(range(first_multiple, max_contest_in_data + 1, interval))
    sorted_snapshot_points = sorted(list(snapshot_points_to_save)); snapshot_idx = 0; processed_count = 0; snapshots_saved_count = 0
    current_freq_counts = current_freq_counts.astype(int)
    for index, row in df_new_draws.iterrows():
        current_concurso_val = row['concurso'];
        if pd.isna(current_concurso_val): continue
        current_concurso = int(current_concurso_val)
        drawn_numbers = {int(num) for num in row[NEW_BALL_COLUMNS].dropna().values}
        for num in drawn_numbers:
            if num in current_freq_counts.index: current_freq_counts[num] += 1
        processed_count += 1
        if snapshot_idx < len(sorted_snapshot_points) and current_concurso == sorted_snapshot_points[snapshot_idx]:
            save_freq_snapshot(current_concurso, current_freq_counts.copy())
            snapshots_saved_count += 1; snapshot_idx += 1
            if snapshots_saved_count % 50 == 0: logger.info(f"{snapshots_saved_count}/{len(sorted_snapshot_points)} snapshots salvos...")
        if processed_count % 500 == 0: logger.info(f"Processados {processed_count}/{len(df_new_draws)} sorteios...")
    logger.info(f"Atualização/Reconstrução de '{FREQ_SNAP_TABLE_NAME}' concluída. {snapshots_saved_count} snapshots.")


# --- NOVA FUNÇÃO PARA STATS FINAIS DE CHUNK ---
def update_chunk_final_stats_table(interval_size: int, force_rebuild: bool = False):
    """
    Calcula e salva as estatísticas FINAIS (freq, rank) de cada bloco completo.
    Args:
        interval_size (int): Tamanho do bloco.
        force_rebuild (bool): Se True, reconstrói do zero.
    """
    table_name = get_chunk_final_stats_table_name(interval_size)
    logger.info(f"Iniciando atualização/rebuild da tabela '{table_name}'...")

    create_chunk_stats_final_table(interval_size) # Garante que a tabela exista

    last_processed_chunk_end = 0 # Começa do zero se for rebuild ou tabela vazia
    if force_rebuild:
        logger.warning(f"REBUILD: Apagando dados de '{table_name}'.")
        try:
            with sqlite3.connect(DATABASE_PATH) as conn: conn.execute(f"DELETE FROM {table_name};")
        except sqlite3.Error as e: logger.error(f"Erro ao limpar '{table_name}': {e}"); return
    else:
        # Descobre o último concurso final de chunk já processado e salvo
        last_processed_val = get_last_contest_in_chunk_stats_final(interval_size)
        if last_processed_val is not None:
            last_processed_chunk_end = last_processed_val
            logger.info(f"Último chunk final processado para intervalo {interval_size}: {last_processed_chunk_end}")
        else:
             logger.info(f"Nenhum chunk final encontrado para intervalo {interval_size}. Calculando do início.")

    # Determina a partir de qual concurso ler os sorteios
    # Precisamos ler desde o início do PRIMEIRO chunk incompleto
    start_processing_from = last_processed_chunk_end + 1

    df_new_draws = read_data_from_db(table_name=TABLE_NAME, columns=BASE_COLS, concurso_minimo=start_processing_from)

    if df_new_draws is None or df_new_draws.empty:
        logger.info(f"Nenhum sorteio novo encontrado após {last_processed_chunk_end}. Tabela '{table_name}' atualizada.")
        return

    max_contest_in_data = int(df_new_draws['concurso'].max())
    logger.info(f"Processando {len(df_new_draws)} sorteios ({start_processing_from} a {max_contest_in_data}) para chunks de {interval_size}...")

    current_chunk_counts = pd.Series(0, index=ALL_NUMBERS).astype(int)
    current_chunk_start_contest = ((start_processing_from - 1) // interval_size) * interval_size + 1
    chunks_saved_count = 0

    # Itera sobre os novos sorteios
    for index, row in df_new_draws.iterrows():
        current_concurso_val = row['concurso']
        if pd.isna(current_concurso_val): continue
        current_concurso = int(current_concurso_val)
        drawn_numbers = {int(num) for num in row[NEW_BALL_COLUMNS].dropna().values}

        # Verifica se este concurso inicia um novo chunk (exceto o primeiríssimo)
        is_start_of_new_chunk = (current_concurso - 1) % interval_size == 0
        if is_start_of_new_chunk and current_concurso != start_processing_from:
            # Este é o início de um NOVO chunk, o anterior acabou no concurso `current_concurso - 1`.
            # Mas só salvamos quando o chunk REALMENTE termina (múltiplo do intervalo)
            # A lógica de salvar está no if abaixo. Aqui só precisamos resetar.
            logger.debug(f"Resetando contagem para novo chunk {interval_size} no concurso {current_concurso}")
            current_chunk_counts = pd.Series(0, index=ALL_NUMBERS).astype(int)
            current_chunk_start_contest = current_concurso

        # Incrementa contagem CUMULATIVA dentro do chunk atual
        for num in drawn_numbers:
            if num in current_chunk_counts.index:
                current_chunk_counts[num] += 1

        # Verifica se este concurso é o FIM de um chunk
        if current_concurso % interval_size == 0:
            logger.debug(f"Fim do chunk {interval_size} detectado no concurso {current_concurso}. Calculando e salvando stats...")
            # Calcula o rank baseado na frequência acumulada deste chunk
            ranks = current_chunk_counts.rank(method='min', ascending=False, pct=False).astype(int)
            # Salva a frequência final E o rank final deste chunk
            save_chunk_final_stats_row(interval_size, current_concurso, current_chunk_counts.copy(), ranks.copy())
            chunks_saved_count += 1
            if chunks_saved_count % 50 == 0: logger.info(f"{chunks_saved_count} chunks finais de {interval_size} salvos...")

    logger.info(f"Atualização/Reconstrução da tabela '{table_name}' concluída. {chunks_saved_count} chunks finais salvos/atualizados.")