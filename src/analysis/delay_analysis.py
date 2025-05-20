# src/analysis/delay_analysis.py
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional 
import logging

logger = logging.getLogger(__name__)

# get_draw_matrix permanece como está, pois será chamada uma vez por iteração no execute_delay.py
def get_draw_matrix(all_data_df: pd.DataFrame, config: Any) -> pd.DataFrame:
    logger.debug("Interno: get_draw_matrix iniciando.")
    if all_data_df.empty or config.CONTEST_ID_COLUMN_NAME not in all_data_df.columns:
        logger.warning("DataFrame de entrada para get_draw_matrix está vazio ou sem coluna de concurso.")
        return pd.DataFrame(columns=config.ALL_NUMBERS) 

    df_for_matrix = all_data_df.copy()
    try:
        df_for_matrix[config.CONTEST_ID_COLUMN_NAME] = pd.to_numeric(df_for_matrix[config.CONTEST_ID_COLUMN_NAME])
    except Exception as e:
        logger.error(f"Não foi possível converter '{config.CONTEST_ID_COLUMN_NAME}' para numérico: {e}")
        return pd.DataFrame(columns=config.ALL_NUMBERS)
        
    df_sorted = df_for_matrix.sort_values(by=config.CONTEST_ID_COLUMN_NAME).set_index(config.CONTEST_ID_COLUMN_NAME)
    
    actual_ball_cols = [col for col in config.BALL_NUMBER_COLUMNS if col in df_sorted.columns]
    if not actual_ball_cols:
        logger.error(f"Nenhuma coluna de bola encontrada em all_data_df para get_draw_matrix.")
        return pd.DataFrame(columns=config.ALL_NUMBERS, index=df_sorted.index if not df_sorted.empty else None)

    draw_matrix_list = []
    for contest_id_loop, row in df_sorted.iterrows(): # Renomeado contest_id para evitar conflito
        try:
            drawn_numbers_for_row = [int(n) for n in row[actual_ball_cols].dropna().unique()]
            drawn_numbers = set(drawn_numbers_for_row)
            contest_presence = {number: 1 if number in drawn_numbers else 0 for number in config.ALL_NUMBERS}
            draw_matrix_list.append(contest_presence)
        except ValueError:
            logger.warning(f"Valor não numérico encontrado nas dezenas do concurso {contest_id_loop}. Adicionando linha de zeros na matriz.")
            contest_presence = {number: 0 for number in config.ALL_NUMBERS} 
            draw_matrix_list.append(contest_presence) # Garante que a lista tenha o mesmo tamanho do índice
            continue 
    
    if not draw_matrix_list: # Deve ser raro se df_sorted não for vazio
        logger.warning("Nenhum dado processado para a matriz de sorteios.")
        return pd.DataFrame(columns=config.ALL_NUMBERS, index=df_sorted.index if not df_sorted.empty else None)

    # Se draw_matrix_list foi populado, seu tamanho deve corresponder a df_sorted.index
    draw_matrix_df = pd.DataFrame(draw_matrix_list, index=df_sorted.index) 
    
    missing_num_cols = set(config.ALL_NUMBERS) - set(draw_matrix_df.columns)
    for col in missing_num_cols:
        draw_matrix_df[col] = 0
    
    logger.debug("Interno: get_draw_matrix concluído.")
    return draw_matrix_df[config.ALL_NUMBERS].astype(int)


# MODIFICADO: Recebe draw_matrix e last_contest_id_in_matrix
def calculate_current_delay(draw_matrix: pd.DataFrame, config: Any, last_contest_id_in_matrix: int) -> pd.DataFrame:
    logger.debug("Interno: Iniciando calculate_current_delay (com matriz pré-calculada).")
    if draw_matrix.empty:
        logger.warning("Matriz de sorteios vazia em calculate_current_delay.")
        return pd.DataFrame({'Dezena': config.ALL_NUMBERS, 'Atraso Atual': last_contest_id_in_matrix if last_contest_id_in_matrix else 0})

    current_delays = {}
    for dezena_val in config.ALL_NUMBERS:
        if dezena_val not in draw_matrix.columns:
            current_delays[dezena_val] = len(draw_matrix) 
            continue
            
        series_dezena = draw_matrix[dezena_val]
        last_occurrence_indices = series_dezena[series_dezena == 1].index
        
        if not last_occurrence_indices.empty:
            last_occurrence_idx = last_occurrence_indices.max()
            current_delays[dezena_val] = last_contest_id_in_matrix - last_occurrence_idx
        else:
            current_delays[dezena_val] = len(draw_matrix)
            
    delay_df = pd.DataFrame(list(current_delays.items()), columns=['Dezena', 'Atraso Atual'])
    delay_df['Dezena'] = delay_df['Dezena'].astype(int)
    delay_df['Atraso Atual'] = delay_df['Atraso Atual'].astype(int)
    logger.debug(f"Atraso atual calculado para {len(delay_df)} dezenas.")
    return delay_df.sort_values(by=['Atraso Atual', 'Dezena'], ascending=[False, True])


# MODIFICADO: Recebe draw_matrix, first_contest_id, last_contest_id
def calculate_max_delay(draw_matrix: pd.DataFrame, config: Any, first_contest_id_in_matrix: int, last_contest_id_in_matrix: int) -> pd.DataFrame:
    logger.debug("Interno: Iniciando calculate_max_delay (com matriz pré-calculada).")
    if draw_matrix.empty:
        logger.warning("Matriz de sorteios vazia em calculate_max_delay.")
        return pd.DataFrame({'Dezena': config.ALL_NUMBERS, 'Atraso Maximo': len(draw_matrix) if not draw_matrix.empty else 0})

    max_delays = {}
    for dezena_val in config.ALL_NUMBERS:
        if dezena_val not in draw_matrix.columns:
            max_delays[dezena_val] = len(draw_matrix)
            continue
            
        series_dezena = draw_matrix[dezena_val]
        occurrences = sorted(series_dezena[series_dezena == 1].index.tolist())
        
        if not occurrences:
            max_delays[dezena_val] = len(draw_matrix)
            continue
        
        gaps = [occurrences[0] - first_contest_id_in_matrix] 
        for i in range(len(occurrences) - 1):
            gaps.append(occurrences[i+1] - occurrences[i] - 1)
        gaps.append(last_contest_id_in_matrix - occurrences[-1])
        
        max_delays[dezena_val] = max(gaps) if gaps else 0
        
    delay_df = pd.DataFrame(list(max_delays.items()), columns=['Dezena', 'Atraso Maximo'])
    delay_df['Dezena'] = delay_df['Dezena'].astype(int)
    delay_df['Atraso Maximo'] = delay_df['Atraso Maximo'].astype(int)
    logger.debug(f"Atraso máximo calculado para {len(delay_df)} dezenas.")
    return delay_df.sort_values(by=['Atraso Maximo', 'Dezena'], ascending=[False, True])


# MODIFICADO: Recebe draw_matrix
def calculate_mean_delay(draw_matrix: pd.DataFrame, config: Any) -> pd.DataFrame:
    logger.debug("Interno: Iniciando calculate_mean_delay (com matriz pré-calculada).")
    if draw_matrix.empty:
        logger.warning("Matriz de sorteios vazia em calculate_mean_delay.")
        return pd.DataFrame({'Dezena': config.ALL_NUMBERS, 'Atraso Medio': np.nan})

    mean_delays = {}
    for dezena_val in config.ALL_NUMBERS:
        if dezena_val not in draw_matrix.columns:
            mean_delays[dezena_val] = np.nan
            continue
            
        series_dezena = draw_matrix[dezena_val]
        occurrences = sorted(series_dezena[series_dezena == 1].index.tolist())
        
        if len(occurrences) < 2:
            mean_delays[dezena_val] = np.nan
            continue
            
        gaps_between_occurrences = []
        for i in range(len(occurrences) - 1):
            gaps_between_occurrences.append(occurrences[i+1] - occurrences[i] - 1)
            
        if gaps_between_occurrences:
            mean_delays[dezena_val] = np.mean(gaps_between_occurrences)
        else:
            mean_delays[dezena_val] = np.nan
            
    delay_df = pd.DataFrame(list(mean_delays.items()), columns=['Dezena', 'Atraso Medio'])
    delay_df['Dezena'] = delay_df['Dezena'].astype(int)
    delay_df['Atraso Medio'] = pd.to_numeric(delay_df['Atraso Medio'], errors='coerce').round(4)
    logger.debug(f"Atraso médio calculado para {len(delay_df)} dezenas.")
    return delay_df.sort_values(by=['Atraso Medio', 'Dezena'], ascending=[False, True])