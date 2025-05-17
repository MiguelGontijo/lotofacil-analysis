# src/analysis/temporal_trend_analysis.py
import pandas as pd
import numpy as np # Adicionado para np.nan
import logging
from typing import List, Any # Any para o objeto config

logger = logging.getLogger(__name__)

def get_full_draw_matrix(all_draws_df: pd.DataFrame, config: Any) -> pd.DataFrame:
    """
    Cria uma matriz binária de ocorrências de dezenas por concurso.

    Args:
        all_draws_df (pd.DataFrame): DataFrame com todos os sorteios.
                                     Deve conter config.CONTEST_ID_COLUMN_NAME e
                                     config.DRAWN_NUMBERS_COLUMN_NAME (lista de dezenas).
        config (Any): Objeto de configuração.

    Returns:
        pd.DataFrame: Matriz com Concursos como índice, Dezenas (1-25) como colunas,
                      e valores 0 (não ocorreu) ou 1 (ocorreu).
                      Retorna um DataFrame vazio se a entrada for inválida.
    """
    logger.debug("Iniciando criação da matriz completa de sorteios (ocorrências).")
    if all_draws_df.empty:
        logger.warning("DataFrame de entrada para get_full_draw_matrix está vazio.")
        return pd.DataFrame()

    contest_col = config.CONTEST_ID_COLUMN_NAME
    drawn_numbers_col = config.DRAWN_NUMBERS_COLUMN_NAME

    if contest_col not in all_draws_df.columns:
        logger.error(f"Coluna '{contest_col}' não encontrada em all_draws_df.")
        return pd.DataFrame()
    if drawn_numbers_col not in all_draws_df.columns:
        logger.error(f"Coluna '{drawn_numbers_col}' não encontrada em all_draws_df.")
        return pd.DataFrame()

    try:
        df_copy = all_draws_df[[contest_col, drawn_numbers_col]].copy()
        df_copy[contest_col] = pd.to_numeric(df_copy[contest_col], errors='coerce')
        df_copy.dropna(subset=[contest_col], inplace=True)
        df_copy[contest_col] = df_copy[contest_col].astype(int)
        df_copy.set_index(contest_col, inplace=True)
        df_copy.sort_index(inplace=True) # Garante que os concursos estão ordenados
    except Exception as e:
        logger.error(f"Erro ao processar coluna de concurso '{contest_col}': {e}", exc_info=True)
        return pd.DataFrame()

    all_contests_idx = df_copy.index
    draw_matrix = pd.DataFrame(0, index=all_contests_idx, columns=config.ALL_NUMBERS)
    draw_matrix.index.name = contest_col

    for contest_id, row in df_copy.iterrows():
        drawn_numbers = row[drawn_numbers_col]
        if isinstance(drawn_numbers, list):
            for number in drawn_numbers:
                if number in draw_matrix.columns:
                    draw_matrix.loc[contest_id, number] = 1
        else:
            logger.warning(f"Dados em '{drawn_numbers_col}' para o concurso {contest_id} não são uma lista: {drawn_numbers}")
            
    logger.debug(f"Matriz completa de sorteios criada com {draw_matrix.shape[0]} concursos e {draw_matrix.shape[1]} dezenas.")
    return draw_matrix


def calculate_moving_average_frequency(
    draw_matrix: pd.DataFrame, 
    windows: List[int], 
    config: Any
) -> pd.DataFrame:
    """
    Calcula a média móvel da frequência de ocorrência para cada dezena e cada janela especificada.
    """
    logger.info(f"Iniciando cálculo da média móvel de frequência para janelas: {windows}")
    if draw_matrix.empty:
        logger.warning("Matriz de sorteios para calculate_moving_average_frequency está vazia.")
        return pd.DataFrame(columns=['Concurso', 'Dezena', 'Janela', 'MA_Frequencia'])
    if not windows:
        logger.warning("Nenhuma janela especificada para cálculo da média móvel de frequência.")
        return pd.DataFrame(columns=['Concurso', 'Dezena', 'Janela', 'MA_Frequencia'])

    all_ma_results = []
    
    if not pd.api.types.is_numeric_dtype(draw_matrix.index):
        try:
            draw_matrix.index = pd.to_numeric(draw_matrix.index)
        except Exception as e:
            logger.error(f"Índice da draw_matrix (Concurso) não é numérico e não pôde ser convertido: {e}")
            return pd.DataFrame(columns=['Concurso', 'Dezena', 'Janela', 'MA_Frequencia'])
    
    draw_matrix = draw_matrix.sort_index()

    for dezena_col in draw_matrix.columns:
        if dezena_col not in config.ALL_NUMBERS:
            logger.warning(f"Coluna {dezena_col} na draw_matrix não está em config.ALL_NUMBERS. Pulando.")
            continue
            
        occurrence_series = draw_matrix[dezena_col]
        
        for window_size in windows:
            if window_size <= 0:
                logger.warning(f"Tamanho de janela inválido {window_size} para a dezena {dezena_col}. Pulando.")
                continue
            
            ma_series = occurrence_series.rolling(window=window_size, min_periods=1).mean()
            
            temp_df = ma_series.reset_index()
            temp_df.columns = ['Concurso', 'MA_Frequencia']
            temp_df['Dezena'] = dezena_col
            temp_df['Janela'] = window_size
            all_ma_results.append(temp_df)

    if not all_ma_results:
        logger.warning("Nenhum resultado de média móvel de frequência foi gerado.")
        return pd.DataFrame(columns=['Concurso', 'Dezena', 'Janela', 'MA_Frequencia'])

    final_df = pd.concat(all_ma_results, ignore_index=True)
    final_df = final_df[['Concurso', 'Dezena', 'Janela', 'MA_Frequencia']]
    
    logger.info(f"Cálculo da média móvel de frequência concluído. {len(final_df)} registros gerados.")
    return final_df

# --- NOVAS FUNÇÕES PARA MÉDIA MÓVEL DE ATRASO ---

def get_historical_delay_matrix(draw_matrix: pd.DataFrame, config: Any) -> pd.DataFrame:
    """
    Calcula o atraso atual histórico para cada dezena em cada concurso.

    Args:
        draw_matrix (pd.DataFrame): Matriz de ocorrências (Concursos x Dezenas, valores 0/1),
                                     com índice de Concurso ordenado.
        config (Any): Objeto de configuração (para CONTEST_ID_COLUMN_NAME).

    Returns:
        pd.DataFrame: Matriz com Concursos como índice, Dezenas como colunas,
                      e valores de atraso atual histórico.
    """
    logger.debug("Iniciando cálculo da matriz de atraso atual histórico.")
    if draw_matrix.empty:
        logger.warning("Matriz de sorteios para get_historical_delay_matrix está vazia.")
        return pd.DataFrame()

    # Assume que draw_matrix.index são os IDs dos concursos e estão ordenados.
    historical_delay_df = pd.DataFrame(index=draw_matrix.index, columns=draw_matrix.columns)
    historical_delay_df.index.name = config.CONTEST_ID_COLUMN_NAME # ou draw_matrix.index.name
    
    first_contest_id_overall = draw_matrix.index.min()

    for dezena in draw_matrix.columns:
        last_occurrence_contest = 0 # Considera 0 como "antes do primeiro concurso"
        
        # Se a dezena nunca ocorre em todo o histórico, o atraso será sempre crescente
        # desde o primeiro concurso.
        if draw_matrix[dezena].sum() == 0:
            for contest_id in draw_matrix.index:
                historical_delay_df.loc[contest_id, dezena] = contest_id - first_contest_id_overall +1 # Ou apenas contest_id
            continue

        for contest_id in draw_matrix.index:
            if last_occurrence_contest == 0: # Ainda não encontrou a primeira ocorrência
                 current_delay = contest_id - first_contest_id_overall + 1
            else:
                 current_delay = contest_id - last_occurrence_contest
            
            historical_delay_df.loc[contest_id, dezena] = current_delay
            
            if draw_matrix.loc[contest_id, dezena] == 1: # Se a dezena ocorreu neste concurso
                last_occurrence_contest = contest_id    # Atualiza o último concurso de ocorrência

    logger.debug("Matriz de atraso atual histórico calculada.")
    return historical_delay_df.astype(int)


def calculate_moving_average_delay(
    historical_delay_matrix: pd.DataFrame, 
    windows: List[int], 
    config: Any # Usado para config.ALL_NUMBERS, se necessário para validar colunas
) -> pd.DataFrame:
    """
    Calcula a média móvel do atraso atual histórico para cada dezena e cada janela.

    Args:
        historical_delay_matrix (pd.DataFrame): Matriz de atraso atual histórico
                                                (Concursos x Dezenas).
        windows (List[int]): Lista de tamanhos de janela para a média móvel.
        config (Any): Objeto de configuração.

    Returns:
        pd.DataFrame: DataFrame em formato longo com as colunas:
                      'Concurso', 'Dezena', 'Janela', 'MA_Atraso'.
    """
    logger.info(f"Iniciando cálculo da média móvel de atraso para janelas: {windows}")
    if historical_delay_matrix.empty:
        logger.warning("Matriz de atraso histórico para calculate_moving_average_delay está vazia.")
        return pd.DataFrame(columns=['Concurso', 'Dezena', 'Janela', 'MA_Atraso'])
    if not windows:
        logger.warning("Nenhuma janela especificada para cálculo da média móvel de atraso.")
        return pd.DataFrame(columns=['Concurso', 'Dezena', 'Janela', 'MA_Atraso'])

    all_ma_results = []
    
    # Garante que o índice (Concurso) é numérico e ordenado (já deve ser pela função anterior)
    if not pd.api.types.is_numeric_dtype(historical_delay_matrix.index):
        try:
            historical_delay_matrix.index = pd.to_numeric(historical_delay_matrix.index)
        except Exception as e:
            logger.error(f"Índice da historical_delay_matrix (Concurso) não é numérico e não pôde ser convertido: {e}")
            return pd.DataFrame(columns=['Concurso', 'Dezena', 'Janela', 'MA_Atraso'])

    historical_delay_matrix = historical_delay_matrix.sort_index()

    for dezena_col in historical_delay_matrix.columns:
        if hasattr(config, 'ALL_NUMBERS') and dezena_col not in config.ALL_NUMBERS:
             logger.warning(f"Coluna {dezena_col} na historical_delay_matrix não está em config.ALL_NUMBERS. Pulando.")
             continue

        delay_series = historical_delay_matrix[dezena_col].astype(float) # .rolling().mean() espera float
        
        for window_size in windows:
            if window_size <= 0:
                logger.warning(f"Tamanho de janela inválido {window_size} para a dezena {dezena_col}. Pulando.")
                continue
            
            ma_series = delay_series.rolling(window=window_size, min_periods=1).mean()
            
            temp_df = ma_series.reset_index()
            temp_df.columns = ['Concurso', 'MA_Atraso']
            temp_df['Dezena'] = dezena_col
            temp_df['Janela'] = window_size
            all_ma_results.append(temp_df)

    if not all_ma_results:
        logger.warning("Nenhum resultado de média móvel de atraso foi gerado.")
        return pd.DataFrame(columns=['Concurso', 'Dezena', 'Janela', 'MA_Atraso'])

    final_df = pd.concat(all_ma_results, ignore_index=True)
    final_df = final_df[['Concurso', 'Dezena', 'Janela', 'MA_Atraso']]
    
    logger.info(f"Cálculo da média móvel de atraso concluído. {len(final_df)} registros gerados.")
    return final_df