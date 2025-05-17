# src/analysis/temporal_trend_analysis.py
import pandas as pd
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

    # Garante que os IDs dos concursos sejam numéricos e usados como índice
    try:
        # Cria uma cópia para evitar SettingWithCopyWarning se all_draws_df for uma view
        df_copy = all_draws_df[[contest_col, drawn_numbers_col]].copy()
        df_copy[contest_col] = pd.to_numeric(df_copy[contest_col], errors='coerce')
        df_copy.dropna(subset=[contest_col], inplace=True)
        df_copy[contest_col] = df_copy[contest_col].astype(int)
        df_copy.set_index(contest_col, inplace=True)
    except Exception as e:
        logger.error(f"Erro ao processar coluna de concurso '{contest_col}': {e}", exc_info=True)
        return pd.DataFrame()

    all_contests_idx = df_copy.index # Todos os IDs de concursos válidos
    
    # Inicializa a matriz com zeros
    draw_matrix = pd.DataFrame(0, index=all_contests_idx, columns=config.ALL_NUMBERS)
    draw_matrix.index.name = contest_col # Preserva o nome do índice

    for contest_id, row in df_copy.iterrows():
        drawn_numbers = row[drawn_numbers_col]
        if isinstance(drawn_numbers, list): # Certifica-se que é uma lista
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

    Args:
        draw_matrix (pd.DataFrame): Matriz de ocorrências (Concursos x Dezenas, valores 0/1).
        windows (List[int]): Lista de tamanhos de janela para a média móvel.
        config (Any): Objeto de configuração (usado para config.ALL_NUMBERS e config.CONTEST_ID_COLUMN_NAME).

    Returns:
        pd.DataFrame: DataFrame em formato longo com as colunas:
                      'Concurso', 'Dezena', 'Janela', 'MA_Frequencia'.
                      Retorna um DataFrame vazio se a entrada for inválida ou não houver janelas.
    """
    logger.info(f"Iniciando cálculo da média móvel de frequência para janelas: {windows}")
    if draw_matrix.empty:
        logger.warning("Matriz de sorteios para calculate_moving_average_frequency está vazia.")
        return pd.DataFrame(columns=['Concurso', 'Dezena', 'Janela', 'MA_Frequencia'])
    if not windows:
        logger.warning("Nenhuma janela especificada para cálculo da média móvel de frequência.")
        return pd.DataFrame(columns=['Concurso', 'Dezena', 'Janela', 'MA_Frequencia'])

    all_ma_results = []
    
    # Garante que o índice da draw_matrix (Concurso) seja numérico e ordenado
    # A função get_full_draw_matrix já deve cuidar disso, mas uma verificação extra pode ser útil
    # se a draw_matrix vier de outra fonte.
    if not pd.api.types.is_numeric_dtype(draw_matrix.index):
        try:
            draw_matrix.index = pd.to_numeric(draw_matrix.index)
        except Exception as e:
            logger.error(f"Índice da draw_matrix (Concurso) não é numérico e não pôde ser convertido: {e}")
            return pd.DataFrame(columns=['Concurso', 'Dezena', 'Janela', 'MA_Frequencia'])
    
    draw_matrix = draw_matrix.sort_index()


    for dezena_col in draw_matrix.columns: # Itera sobre as colunas de dezenas
        if dezena_col not in config.ALL_NUMBERS: # Segurança extra
            logger.warning(f"Coluna {dezena_col} na draw_matrix não está em config.ALL_NUMBERS. Pulando.")
            continue
            
        occurrence_series = draw_matrix[dezena_col]
        
        for window_size in windows:
            if window_size <= 0:
                logger.warning(f"Tamanho de janela inválido {window_size} para a dezena {dezena_col}. Pulando.")
                continue
            
            # min_periods=1 garante que a média seja calculada desde o início,
            # mesmo que a janela completa ainda não esteja disponível.
            ma_series = occurrence_series.rolling(window=window_size, min_periods=1).mean()
            
            # Transforma a série resultante em um DataFrame para adicionar ao resultado final
            temp_df = ma_series.reset_index() # Concurso se torna uma coluna
            temp_df.columns = ['Concurso', 'MA_Frequencia']
            temp_df['Dezena'] = dezena_col
            temp_df['Janela'] = window_size
            all_ma_results.append(temp_df)

    if not all_ma_results:
        logger.warning("Nenhum resultado de média móvel foi gerado.")
        return pd.DataFrame(columns=['Concurso', 'Dezena', 'Janela', 'MA_Frequencia'])

    final_df = pd.concat(all_ma_results, ignore_index=True)
    final_df = final_df[['Concurso', 'Dezena', 'Janela', 'MA_Frequencia']] # Reordena colunas
    
    logger.info(f"Cálculo da média móvel de frequência concluído. {len(final_df)} registros gerados.")
    return final_df