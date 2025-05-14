# src/analysis/delay_analysis.py
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any 
import logging

logger = logging.getLogger(__name__)

def get_draw_matrix(all_data_df: pd.DataFrame, config: Any) -> pd.DataFrame:
    # Usa config.CONTEST_ID_COLUMN_NAME, config.BALL_NUMBER_COLUMNS, config.ALL_NUMBERS
    if all_data_df.empty or config.CONTEST_ID_COLUMN_NAME not in all_data_df.columns:
        logger.warning(f"DataFrame de entrada para get_draw_matrix está vazio ou sem coluna '{config.CONTEST_ID_COLUMN_NAME}'.")
        return pd.DataFrame(columns=config.ALL_NUMBERS) 

    df_for_matrix = all_data_df.copy()
    try:
        df_for_matrix[config.CONTEST_ID_COLUMN_NAME] = pd.to_numeric(df_for_matrix[config.CONTEST_ID_COLUMN_NAME])
    except Exception as e:
        logger.error(f"Não foi possível converter a coluna '{config.CONTEST_ID_COLUMN_NAME}' para numérico em get_draw_matrix: {e}")
        return pd.DataFrame(columns=config.ALL_NUMBERS)
        
    df_sorted = df_for_matrix.sort_values(by=config.CONTEST_ID_COLUMN_NAME).set_index(config.CONTEST_ID_COLUMN_NAME)
    
    actual_ball_cols = [col for col in config.BALL_NUMBER_COLUMNS if col in df_sorted.columns]
    if not actual_ball_cols:
        logger.error(f"Nenhuma coluna de bola ({config.BALL_NUMBER_COLUMNS}) encontrada em all_data_df para get_draw_matrix.")
        return pd.DataFrame(columns=config.ALL_NUMBERS, index=df_sorted.index)

    draw_matrix_list = []
    for contest_id, row in df_sorted.iterrows():
        drawn_numbers_for_row = [int(n) for n in row[actual_ball_cols].dropna().unique()]
        drawn_numbers = set(drawn_numbers_for_row)
        contest_presence = {number: 1 if number in drawn_numbers else 0 for number in config.ALL_NUMBERS}
        draw_matrix_list.append(contest_presence)
    
    if not draw_matrix_list:
        logger.warning("Nenhum dado processado para a matriz de sorteios.")
        return pd.DataFrame(columns=config.ALL_NUMBERS, index=df_sorted.index if not df_sorted.empty else None)

    draw_matrix_df = pd.DataFrame(draw_matrix_list, index=df_sorted.index)
    
    missing_num_cols = set(config.ALL_NUMBERS) - set(draw_matrix_df.columns)
    for col in missing_num_cols:
        draw_matrix_df[col] = 0
    
    return draw_matrix_df[config.ALL_NUMBERS].astype(int)

def calculate_current_delay(all_data_df: pd.DataFrame, config: Any) -> pd.DataFrame:
    logger.info("Calculando atraso atual das dezenas.")
    if all_data_df.empty:
        logger.warning("DataFrame vazio para calculate_current_delay.")
        return pd.DataFrame({'Dezena': [], 'Atraso Atual': []})
    draw_matrix = get_draw_matrix(all_data_df, config)
    if draw_matrix.empty:
        logger.warning("Matriz de sorteios vazia, não é possível calcular o atraso atual.")
        return pd.DataFrame({'Dezena': [], 'Atraso Atual': []})
    current_delays = {}
    max_contest_in_matrix = draw_matrix.index.max() if not draw_matrix.empty else 0
    for dezena_val in config.ALL_NUMBERS:
        if dezena_val not in draw_matrix.columns:
            current_delays[dezena_val] = len(draw_matrix) if max_contest_in_matrix == 0 else max_contest_in_matrix
            continue
        series_dezena = draw_matrix[dezena_val]
        last_occurrence_indices = series_dezena[series_dezena == 1].index
        last_occurrence_idx = last_occurrence_indices.max() if not last_occurrence_indices.empty else pd.NA
        if pd.isna(last_occurrence_idx):
            current_delays[dezena_val] = len(draw_matrix)
        else:
            current_delays[dezena_val] = max_contest_in_matrix - last_occurrence_idx
    delay_df = pd.DataFrame(list(current_delays.items()), columns=['Dezena', 'Atraso Atual']).astype({'Dezena': int, 'Atraso Atual': int})
    return delay_df.sort_values(by=['Atraso Atual', 'Dezena'], ascending=[False, True])

def calculate_max_delay(all_data_df: pd.DataFrame, config: Any) -> pd.DataFrame:
    logger.info("Calculando atraso máximo das dezenas.")
    if all_data_df.empty: return pd.DataFrame({'Dezena': [], 'Atraso Maximo': []})
    draw_matrix = get_draw_matrix(all_data_df, config)
    if draw_matrix.empty: return pd.DataFrame({'Dezena': [], 'Atraso Maximo': []})
    max_delays = {}
    contest_ids_sorted = draw_matrix.index.sort_values().tolist()
    if not contest_ids_sorted:
        for dezena_val in config.ALL_NUMBERS: max_delays[dezena_val] = 0
        return pd.DataFrame(list(max_delays.items()), columns=['Dezena', 'Atraso Maximo'])
    first_contest_overall = contest_ids_sorted[0]
    last_contest_overall = contest_ids_sorted[-1]
    for dezena_val in config.ALL_NUMBERS:
        if dezena_val not in draw_matrix.columns: max_delays[dezena_val] = len(draw_matrix); continue
        series_dezena = draw_matrix[dezena_val]
        occurrences = series_dezena[series_dezena == 1].index.sort_values().tolist()
        if not occurrences: max_delays[dezena_val] = len(draw_matrix); continue
        gaps = [occurrences[0] - first_contest_overall]
        for i in range(len(occurrences) - 1): gaps.append(occurrences[i+1] - occurrences[i] - 1)
        gaps.append(last_contest_overall - occurrences[-1])
        max_delays[dezena_val] = max(gaps) if gaps else 0
    delay_df = pd.DataFrame(list(max_delays.items()), columns=['Dezena', 'Atraso Maximo']).astype({'Dezena':int, 'Atraso Maximo':int})
    return delay_df.sort_values(by=['Atraso Maximo', 'Dezena'], ascending=[False, True])

def calculate_mean_delay(all_data_df: pd.DataFrame, config: Any) -> pd.DataFrame:
    logger.info("Calculando atraso médio das dezenas (gaps entre ocorrências).")
    if all_data_df.empty: return pd.DataFrame({'Dezena': [], 'Atraso Medio': []})
    draw_matrix = get_draw_matrix(all_data_df, config)
    if draw_matrix.empty: return pd.DataFrame({'Dezena': [], 'Atraso Medio': []})
    mean_delays = {}
    for dezena_val in config.ALL_NUMBERS:
        if dezena_val not in draw_matrix.columns: mean_delays[dezena_val] = np.nan; continue
        series_dezena = draw_matrix[dezena_val]
        occurrences = series_dezena[series_dezena == 1].index.sort_values().tolist()
        if len(occurrences) < 2: mean_delays[dezena_val] = np.nan; continue
        gaps_between_occurrences = []
        for i in range(len(occurrences) - 1): gaps_between_occurrences.append(occurrences[i+1] - occurrences[i] - 1)
        if gaps_between_occurrences: mean_delays[dezena_val] = np.mean(gaps_between_occurrences)
        else: mean_delays[dezena_val] = np.nan
    delay_df = pd.DataFrame(list(mean_delays.items()), columns=['Dezena', 'Atraso Medio']).astype({'Dezena':int})
    return delay_df.sort_values(by=['Atraso Medio', 'Dezena'], ascending=[False, True])