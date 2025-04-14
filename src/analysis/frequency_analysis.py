# src/analysis/frequency_analysis.py

import pandas as pd
from typing import Optional, Dict

# Importações locais
from src.database_manager import read_data_from_db
from src.config import logger, NEW_BALL_COLUMNS # NEW_BALL_COLUMNS são as colunas b1 a b15

# Colunas necessárias do banco de dados para análise de frequência
BASE_COLS = ['concurso'] + NEW_BALL_COLUMNS
ALL_NUMBERS = list(range(1, 26)) # Lista de todas as dezenas possíveis (1 a 25)

def _get_base_data(concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Função auxiliar para buscar os dados base (concurso e bolas) do banco de dados.

    Args:
        concurso_maximo (Optional[int]): O concurso máximo a ser incluído.

    Returns:
        Optional[pd.DataFrame]: DataFrame com colunas 'concurso' e 'b1' a 'b15',
                                 ou None se não houver dados ou ocorrer erro.
    """
    df = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo)

    if df is None or df.empty:
        logger.warning("Nenhum dado base encontrado no banco de dados para os critérios fornecidos.")
        return None

    # Verifica se todas as colunas de bolas esperadas estão presentes
    if not all(col in df.columns for col in NEW_BALL_COLUMNS):
        logger.error("Dados lidos do banco não contêm todas as colunas de bolas esperadas (b1 a b15).")
        return None

    return df

def calculate_overall_frequency(concurso_maximo: Optional[int] = None) -> Optional[pd.Series]:
    """
    Calcula a frequência de sorteio de cada dezena (1-25) em todos os concursos
    até o concurso_maximo (se especificado).

    Args:
        concurso_maximo (Optional[int]): O último concurso a ser considerado.

    Returns:
        Optional[pd.Series]: Uma Series com a contagem de cada dezena (índice 1-25),
                             ordenada pelo número da dezena, ou None.
    """
    logger.info(f"Calculando frequência geral até o concurso {concurso_maximo or 'último'}...")
    df = _get_base_data(concurso_maximo)
    if df is None:
        return None

    # Transforma as colunas de bolas em uma única série (long format)
    melted_balls = df[NEW_BALL_COLUMNS].melt(value_name='number')['number']

    # Conta a frequência de cada número
    frequency = melted_balls.value_counts()

    # Garante que todas as dezenas (1-25) estejam presentes, mesmo com contagem 0
    frequency = frequency.reindex(ALL_NUMBERS, fill_value=0)

    # Ordena pelo número da dezena (índice)
    frequency.sort_index(inplace=True)

    logger.info("Cálculo de frequência geral concluído.")
    return frequency

def calculate_windowed_frequency(window_size: int, concurso_maximo: Optional[int] = None) -> Optional[pd.Series]:
    """
    Calcula a frequência de sorteio de cada dezena (1-25) nos últimos 'window_size'
    concursos até o concurso_maximo (se especificado).

    Args:
        window_size (int): O número de concursos na janela deslizante.
        concurso_maximo (Optional[int]): O último concurso da janela. Se None, usa o último disponível.

    Returns:
        Optional[pd.Series]: Uma Series com a contagem de cada dezena na janela,
                             ordenada pelo número da dezena, ou None.
    """
    logger.info(f"Calculando frequência na janela de {window_size} concursos até {concurso_maximo or 'último'}...")
    df_all = _get_base_data(concurso_maximo) # Pega todos os dados até o máximo
    if df_all is None:
        return None

    # Determina o concurso máximo real presente nos dados
    actual_max_concurso = df_all['concurso'].max()
    if concurso_maximo and concurso_maximo > actual_max_concurso:
        logger.warning(f"Concurso máximo solicitado ({concurso_maximo}) é maior que o último disponível ({actual_max_concurso}). Usando {actual_max_concurso}.")
        effective_max_concurso = actual_max_concurso
    elif concurso_maximo:
        effective_max_concurso = concurso_maximo
    else:
        effective_max_concurso = actual_max_concurso

    # Calcula o concurso inicial da janela
    concurso_minimo = effective_max_concurso - window_size + 1

    # Filtra o DataFrame para a janela desejada
    df_window = df_all[df_all['concurso'] >= concurso_minimo].copy() # .copy() para evitar SettingWithCopyWarning

    if df_window.empty:
        logger.warning(f"Nenhum dado encontrado na janela de {window_size} concursos terminando em {effective_max_concurso}.")
        # Retorna uma série com zeros para todas as dezenas
        return pd.Series(0, index=ALL_NUMBERS, name='frequency')


    logger.info(f"Analisando {len(df_window)} concursos na janela [{concurso_minimo} - {effective_max_concurso}]")

    # Transforma as colunas de bolas em uma única série (long format)
    melted_balls = df_window[NEW_BALL_COLUMNS].melt(value_name='number')['number']

    # Conta a frequência de cada número na janela
    frequency = melted_balls.value_counts()

    # Garante que todas as dezenas (1-25) estejam presentes, mesmo com contagem 0
    frequency = frequency.reindex(ALL_NUMBERS, fill_value=0)

    # Ordena pelo número da dezena (índice)
    frequency.sort_index(inplace=True)

    logger.info(f"Cálculo de frequência na janela de {window_size} concluído.")
    return frequency


def calculate_cumulative_frequency_history(concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Calcula a frequência acumulada de cada dezena (1-25) para cada concurso
    realizado até o concurso_maximo (se especificado).

    Args:
        concurso_maximo (Optional[int]): O último concurso a ser considerado.

    Returns:
        Optional[pd.DataFrame]: DataFrame indexado por 'concurso', com colunas
                                 'cum_freq_1' a 'cum_freq_25' mostrando a contagem
                                 acumulada de cada dezena até aquele concurso. Retorna None se não houver dados.
    """
    logger.info(f"Calculando histórico de frequência acumulada até o concurso {concurso_maximo or 'último'}...")
    # Precisamos da data também para potentially adicionar de volta
    df = read_data_from_db(columns=['concurso', 'data_sorteio'] + NEW_BALL_COLUMNS,
                             concurso_maximo=concurso_maximo)

    if df is None or df.empty:
        logger.warning("Nenhum dado base encontrado para calcular frequência acumulada.")
        return None

    # 1. Melt: Transforma b1...b15 em linhas
    melted = df.melt(id_vars=['concurso'], value_vars=NEW_BALL_COLUMNS, value_name='number')
    melted = melted[['concurso', 'number']].dropna() # Remove NaNs e a coluna 'variable'
    melted['number'] = melted['number'].astype(int) # Garante que números das bolas são inteiros

    # 2. Pivot: Cria uma matriz onde 1 indica que a dezena foi sorteada no concurso
    # Usamos 'size' como função de agregação e lidamos com duplicatas (embora não devam existir por concurso/bola)
    counts_pivot = pd.pivot_table(melted, index='concurso', columns='number', aggfunc='size', fill_value=0)

    # 3. Reindex: Garante que todas as colunas de 1 a 25 existam
    counts_pivot = counts_pivot.reindex(columns=ALL_NUMBERS, fill_value=0)

    # 4. Cumsum: Calcula a soma acumulada ao longo dos concursos (linhas)
    cumulative_freq = counts_pivot.cumsum(axis=0)

    # 5. Renomear colunas
    cumulative_freq.columns = [f'cum_freq_{i}' for i in ALL_NUMBERS]

    # 6. Opcional: Adicionar data de volta
    if 'data_sorteio' in df.columns:
         # Define 'concurso' como índice no df original para alinhar com cumulative_freq
         df_dates = df[['concurso', 'data_sorteio']].set_index('concurso')
         # Junta as datas com as frequências acumuladas
         cumulative_freq = df_dates.join(cumulative_freq, how='right') # 'right' para manter todos os concursos da freq. acumulada
         # Reseta o índice para ter 'concurso' como coluna novamente, se desejado
         # cumulative_freq.reset_index(inplace=True)


    logger.info("Cálculo do histórico de frequência acumulada concluído.")
    return cumulative_freq