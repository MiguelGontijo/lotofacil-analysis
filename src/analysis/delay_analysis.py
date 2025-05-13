# src/analysis/delay_analysis.py
import pandas as pd
import numpy as np # Pode ser útil para cálculos numéricos
from typing import Dict, List, Tuple, Any # Adicionado Tuple, Any
import logging # ADICIONADO

from src.config import ALL_NUMBERS # ALL_NUMBERS é uma lista de 1 a 25

logger = logging.getLogger(__name__) # Logger específico para este módulo

def get_draw_matrix(all_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria uma matriz booleana (ou 0/1) indicando a presença de cada dezena em cada concurso.
    Linhas: Concursos (ordenados)
    Colunas: Dezenas (1 a 25)
    Valores: 1 se a dezena saiu, 0 caso contrário.
    """
    if all_data_df.empty or 'Concurso' not in all_data_df.columns:
        logger.warning("DataFrame de entrada para get_draw_matrix está vazio ou sem coluna 'Concurso'.")
        return pd.DataFrame()

    dezena_cols = [f'bola_{i}' for i in range(1, 16)]
    actual_dezena_cols = [col for col in dezena_cols if col in all_data_df.columns]
    if not actual_dezena_cols:
        logger.error("Nenhuma coluna de bola encontrada em all_data_df para get_draw_matrix.")
        return pd.DataFrame()

    # Garante que os concursos estão ordenados
    df_sorted = all_data_df.sort_values(by='Concurso').set_index('Concurso')
    
    draw_matrix_list = []
    for contest_id, row in df_sorted.iterrows():
        drawn_numbers = set(row[actual_dezena_cols].dropna().astype(int))
        contest_presence = {number: 1 if number in drawn_numbers else 0 for number in ALL_NUMBERS}
        contest_presence['Concurso'] = contest_id # Mantém o ID do concurso para referência, se necessário
        draw_matrix_list.append(contest_presence)
    
    if not draw_matrix_list:
        logger.warning("Nenhum dado processado para a matriz de sorteios.")
        return pd.DataFrame()

    # Cria DataFrame e define o índice se Concurso foi adicionado
    # Se Concurso não for necessário como coluna, pode ser o índice diretamente.
    draw_matrix_df = pd.DataFrame(draw_matrix_list)
    if 'Concurso' in draw_matrix_df.columns:
        draw_matrix_df = draw_matrix_df.set_index('Concurso')
    
    # Garante que todas as colunas de dezenas (ALL_NUMBERS) existam
    for number in ALL_NUMBERS:
        if number not in draw_matrix_df.columns:
            draw_matrix_df[number] = 0 # Adiciona coluna com zeros se alguma dezena nunca saiu

    return draw_matrix_df[ALL_NUMBERS] # Retorna apenas as colunas das dezenas, indexadas por Concurso


def calculate_current_delay(all_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o atraso atual (gap) de cada dezena.
    O atraso atual é o número de concursos desde a última vez que a dezena foi sorteada.
    """
    logger.info("Calculando atraso atual das dezenas.")
    if all_data_df.empty:
        logger.warning("DataFrame vazio para calculate_current_delay.")
        return pd.DataFrame({'Dezena': [], 'Atraso Atual': []})

    draw_matrix = get_draw_matrix(all_data_df)
    if draw_matrix.empty:
        logger.warning("Matriz de sorteios vazia, não é possível calcular o atraso atual.")
        return pd.DataFrame({'Dezena': [], 'Atraso Atual': []})

    current_delays = {}
    for dezena in draw_matrix.columns: # As colunas são as dezenas (ALL_NUMBERS)
        series_dezena = draw_matrix[dezena]
        # Encontra o índice (Concurso) da última ocorrência (valor 1)
        last_occurrence_idx = series_dezena[series_dezena == 1].index.max()
        
        if pd.isna(last_occurrence_idx): # Dezena nunca saiu
            current_delays[dezena] = len(draw_matrix) # Atraso é o número total de concursos
        else:
            # O atraso é o número total de concursos MENOS o índice do concurso da última ocorrência.
            # Assumindo que os índices de draw_matrix são os números dos concursos (1-based).
            # Se draw_matrix.index.max() é o último concurso, então:
            current_delays[dezena] = draw_matrix.index.max() - last_occurrence_idx
            
    delay_df = pd.DataFrame(list(current_delays.items()), columns=['Dezena', 'Atraso Atual'])
    delay_df['Dezena'] = delay_df['Dezena'].astype(int)
    delay_df['Atraso Atual'] = delay_df['Atraso Atual'].astype(int)
    logger.info("Atraso atual calculado.")
    return delay_df.sort_values(by='Atraso Atual', ascending=False)


def calculate_max_delay(all_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o atraso máximo (maior gap) que cada dezena já teve.
    """
    logger.info("Calculando atraso máximo das dezenas.")
    if all_data_df.empty:
        logger.warning("DataFrame vazio para calculate_max_delay.")
        return pd.DataFrame({'Dezena': [], 'Atraso Maximo': []})

    draw_matrix = get_draw_matrix(all_data_df) # Colunas são as dezenas
    if draw_matrix.empty:
        logger.warning("Matriz de sorteios vazia, não é possível calcular o atraso máximo.")
        return pd.DataFrame({'Dezena': [], 'Atraso Maximo': []})

    max_delays = {}
    for dezena in draw_matrix.columns:
        series_dezena = draw_matrix[dezena]
        # Identifica os concursos onde a dezena saiu (valor 1)
        occurrences = series_dezena[series_dezena == 1].index.tolist()
        
        if not occurrences: # Dezena nunca saiu
            max_delays[dezena] = len(draw_matrix)
            continue
        
        # Calcula gaps entre ocorrências
        gaps = []
        # Gap inicial (do concurso 0 até a primeira ocorrência)
        # Se os índices são 1-based, o gap é occurrences[0] - 1
        gaps.append(occurrences[0] - 1) 
        
        for i in range(len(occurrences) - 1):
            gaps.append(occurrences[i+1] - occurrences[i] - 1)
            
        # Gap final (da última ocorrência até o último concurso)
        gaps.append(draw_matrix.index.max() - occurrences[-1])
        
        max_delays[dezena] = max(gaps) if gaps else 0 # Se só saiu uma vez e no primeiro concurso, gaps pode ser vazio.
        
    delay_df = pd.DataFrame(list(max_delays.items()), columns=['Dezena', 'Atraso Maximo'])
    delay_df['Dezena'] = delay_df['Dezena'].astype(int)
    delay_df['Atraso Maximo'] = delay_df['Atraso Maximo'].astype(int)
    logger.info("Atraso máximo calculado.")
    return delay_df.sort_values(by='Atraso Maximo', ascending=False)


def calculate_mean_delay(all_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula o atraso médio (média dos gaps) de cada dezena.
    """
    logger.info("Calculando atraso médio das dezenas.")
    if all_data_df.empty:
        logger.warning("DataFrame vazio para calculate_mean_delay.")
        return pd.DataFrame({'Dezena': [], 'Atraso Medio': []})

    draw_matrix = get_draw_matrix(all_data_df) # Colunas são as dezenas
    if draw_matrix.empty:
        logger.warning("Matriz de sorteios vazia, não é possível calcular o atraso médio.")
        return pd.DataFrame({'Dezena': [], 'Atraso Medio': []})

    mean_delays = {}
    for dezena in draw_matrix.columns:
        series_dezena = draw_matrix[dezena]
        occurrences = series_dezena[series_dezena == 1].index.tolist()
        
        if len(occurrences) < 2: # Precisa de pelo menos duas ocorrências para ter um gap entre elas
            # Se nunca saiu, ou saiu uma vez, o conceito de atraso médio entre aparições é menos definido.
            # Poderíamos retornar NaN, 0, ou o atraso total se nunca saiu.
            # Para simplificar, se não há gaps entre ocorrências, o atraso médio é 0 ou indefinido.
            mean_delays[dezena] = np.nan # Ou 0, ou len(draw_matrix) se nunca saiu
            continue
            
        gaps_between_occurrences = []
        for i in range(len(occurrences) - 1):
            gaps_between_occurrences.append(occurrences[i+1] - occurrences[i] - 1)
        
        mean_delays[dezena] = np.mean(gaps_between_occurrences) if gaps_between_occurrences else np.nan
        
    delay_df = pd.DataFrame(list(mean_delays.items()), columns=['Dezena', 'Atraso Medio'])
    delay_df['Dezena'] = delay_df['Dezena'].astype(int)
    # Atraso médio pode ser float
    logger.info("Atraso médio calculado.")
    return delay_df.sort_values(by='Atraso Medio', ascending=False)