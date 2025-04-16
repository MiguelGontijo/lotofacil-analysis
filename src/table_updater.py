# src/table_updater.py

import pandas as pd
import sqlite3
from typing import List, Optional, Dict, Set

# Importa constantes e logger
from src.config import (
    logger, ALL_NUMBERS, DEFAULT_SNAPSHOT_INTERVALS, DATABASE_PATH,
    NEW_BALL_COLUMNS, TABLE_NAME # Usa TABLE_NAME
)

# Fallbacks
if 'ALL_NUMBERS' not in globals(): ALL_NUMBERS = list(range(1, 26))
if 'DEFAULT_SNAPSHOT_INTERVALS' not in globals(): DEFAULT_SNAPSHOT_INTERVALS = [10, 25, 50, 100, 200, 300, 400, 500]
if 'NEW_BALL_COLUMNS' not in globals(): NEW_BALL_COLUMNS = [f'b{i}' for i in range(1,16)]

# Importa funções do DB Manager (incluindo as novas de chunk)
from src.database_manager import (
    read_data_from_db, get_last_freq_snapshot_contest, save_freq_snapshot,
    get_closest_freq_snapshot, create_freq_snap_table, FREQ_SNAP_TABLE_NAME,
    create_chunk_freq_table, save_chunk_freq_row, # <<< Novas para chunk detail
    get_chunk_table_name # <<< Nova para nome da tabela
)

# Define BASE_COLS localmente
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS


def update_freq_geral_snap_table(intervals: List[int] = DEFAULT_SNAPSHOT_INTERVALS,
                                 force_rebuild: bool = False):
    """ Calcula e salva snapshots da frequência geral acumulada incrementalmente. """
    # (Código idêntico ao da última versão correta)
    logger.info(f"Iniciando atualização snapshots de frequência geral...")
    create_freq_snap_table(); last_snapshot_contest = 0; current_freq_counts = pd.Series(0, index=ALL_NUMBERS)
    if force_rebuild:
        logger.warning(f"REBUILD: Apagando snapshots de '{FREQ_SNAP_TABLE_NAME}'.")
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
    current_freq_counts = current_freq_counts.astype(int) # Garante tipo
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
    logger.info(f"Atualização/Reconstrução de '{FREQ_SNAP_TABLE_NAME}' concluída. {snapshots_saved_count} snapshots salvos/atualizados.")


# --- NOVA FUNÇÃO ---
def rebuild_chunk_freq_detail_table(interval_size: int):
    """
    Reconstrói COMPLETAMENTE a tabela de frequência detalhada por chunk
    para um intervalo específico. (Processo LENTO!)

    Args:
        interval_size (int): O tamanho do bloco/chunk (ex: 10).
    """
    table_name = get_chunk_table_name(interval_size)
    logger.info(f"Iniciando reconstrução completa da tabela '{table_name}'...")

    # 1. Cria/Limpa a tabela de chunk
    create_chunk_freq_table(interval_size)
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            conn.execute(f"DELETE FROM {table_name};")
            logger.warning(f"Dados antigos de '{table_name}' apagados para reconstrução.")
    except sqlite3.Error as e:
        logger.error(f"Erro ao limpar tabela '{table_name}': {e}")
        return

    # 2. Lê TODOS os sorteios da tabela principal
    df_all_draws = read_data_from_db(table_name=TABLE_NAME, columns=BASE_COLS)
    if df_all_draws is None or df_all_draws.empty:
        logger.error("Não foi possível ler dados da tabela de sorteios para reconstruir chunks.")
        return

    logger.info(f"Processando {len(df_all_draws)} sorteios para popular '{table_name}'...")

    # 3. Itera e calcula/salva a frequência cumulativa DENTRO de cada chunk
    current_chunk_counts = pd.Series(0, index=ALL_NUMBERS).astype(int)
    current_chunk_start_contest = -1 # Indica que ainda não começou o primeiro chunk

    processed_count = 0
    for index, row in df_all_draws.iterrows():
        current_concurso_val = row['concurso']
        if pd.isna(current_concurso_val): continue
        current_concurso = int(current_concurso_val)
        drawn_numbers = {int(num) for num in row[NEW_BALL_COLUMNS].dropna().values}

        # Verifica se um novo chunk começou
        # O primeiro chunk começa no concurso 1 (ou o primeiro do dataset)
        # Novos chunks começam quando (concurso - 1) é múltiplo do intervalo
        is_new_chunk = (current_chunk_start_contest == -1) or \
                       ((current_concurso - current_chunk_start_contest) >= interval_size)

        if is_new_chunk:
            current_chunk_counts = pd.Series(0, index=ALL_NUMBERS).astype(int) # Zera contagem
            current_chunk_start_contest = current_concurso # Marca início do novo chunk
            logger.debug(f"Iniciando novo chunk de {interval_size} no concurso {current_concurso}")

        # Incrementa contagem para os números sorteados neste concurso
        for num in drawn_numbers:
            if num in current_chunk_counts.index:
                current_chunk_counts[num] += 1

        # Salva a linha com a contagem CUMULATIVA ATÉ ESTE PONTO DENTRO DO CHUNK
        save_chunk_freq_row(current_concurso, current_chunk_counts.copy(), interval_size)

        processed_count += 1
        if processed_count % 500 == 0:
             logger.info(f"Processados {processed_count}/{len(df_all_draws)} sorteios para '{table_name}'...")

    logger.info(f"Reconstrução completa da tabela '{table_name}' concluída.")

# Função de update incremental (MAIS COMPLEXA) - AINDA NÃO IMPLEMENTADA
# def update_chunk_freq_detail_table(interval_size: int):
#     logger.info(f"Iniciando atualização incremental da tabela '{get_chunk_table_name(interval_size)}'...")
#     # 1. Descobre último concurso na tabela de chunk
#     # 2. Descobre em qual chunk ele estava e pega a contagem dele
#     # 3. Lê novos sorteios do BD principal
#     # 4. Continua a lógica de iteração e salvamento...
#     logger.warning("Atualização incremental para chunk detail ainda não implementada.")
#     pass