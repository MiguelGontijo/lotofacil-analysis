# src/analysis/delay_analysis.py

import pandas as pd
from typing import Optional, Dict

# Importações locais
# Precisamos ler os dados de forma ordenada DESC para esta lógica
# Vamos adicionar um parâmetro 'order_by' na função de leitura ou criar uma específica?
# Por simplicidade agora, vamos ler ASC e pegar o último. Uma otimização seria ler DESC.
from src.database_manager import read_data_from_db
from src.config import logger, NEW_BALL_COLUMNS

# Colunas necessárias
BASE_COLS = ['concurso'] + NEW_BALL_COLUMNS
ALL_NUMBERS = list(range(1, 26))

def calculate_current_delay(concurso_maximo: Optional[int] = None) -> Optional[pd.Series]:
    """
    Calcula o atraso atual (número de concursos sem ser sorteada) para cada
    dezena (1-25), até um concurso máximo especificado.

    Args:
        concurso_maximo (Optional[int]): O concurso de referência para calcular o atraso.
                                         Se None, usa o último concurso disponível no BD.

    Returns:
        Optional[pd.Series]: Uma Series com o atraso de cada dezena (índice 1-25),
                             ordenada pelo número da dezena, ou None se erro/sem dados.
                             Atraso 0 significa que a dezena saiu no último concurso analisado.
    """
    logger.info(f"Calculando atraso atual das dezenas até o concurso {concurso_maximo or 'último'}...")

    # Lê todos os dados até o concurso máximo, ordenados ASC por padrão
    df = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo)

    if df is None or df.empty:
        logger.warning("Nenhum dado encontrado para calcular atrasos.")
        return None

    # Verifica colunas de bolas
    if not all(col in df.columns for col in NEW_BALL_COLUMNS):
        logger.error("Dados lidos do banco não contêm todas as colunas de bolas esperadas (b1 a b15).")
        return None

    # Determina o concurso de referência (o último concurso nos dados lidos)
    effective_max_concurso = df['concurso'].max()
    logger.info(f"Concurso de referência para cálculo do atraso: {effective_max_concurso}")

    # Encontra o último concurso em que cada número apareceu
    last_seen = {}
    # Iteramos do mais recente para o mais antigo para otimizar a busca
    for index, row in df.iloc[::-1].iterrows(): # iloc[::-1] inverte o DataFrame
        current_concurso_scan = row['concurso']
        drawn_numbers = set(int(num) for num in row[NEW_BALL_COLUMNS].dropna().values)

        for number in ALL_NUMBERS:
            if number not in last_seen: # Só registra se ainda não achou
                if number in drawn_numbers:
                    last_seen[number] = current_concurso_scan

        # Otimização: Se já achou todos, pode parar
        if len(last_seen) == 25:
            logger.debug(f"Última ocorrência de todas as dezenas encontrada até o concurso {current_concurso_scan}.")
            break

    # Calcula o atraso para cada número
    delays = {}
    for number in ALL_NUMBERS:
        last_seen_concurso = last_seen.get(number)
        if last_seen_concurso is not None:
            # Atraso é a diferença entre o concurso de referência e o último que saiu
            delays[number] = effective_max_concurso - last_seen_concurso
        else:
            # Se nunca foi visto no período analisado, o atraso é o número total de concursos analisados?
            # Ou podemos retornar um valor especial como -1 ou NaN? Vamos usar NaN por enquanto.
            # Ou considerar o atraso como o número total de concursos analisados. Adotaremos NaN.
            logger.warning(f"Dezena {number} não encontrada no período analisado (até {effective_max_concurso}). Atraso indefinido.")
            delays[number] = pd.NA # Ou float('nan') se preferir tipo numérico

    # Converte para Series e ordena pelo número da dezena
    delay_series = pd.Series(delays, name='Atraso Atual').sort_index()
    # Tenta converter para Int64 se possível (se não houver NAs)
    try:
        delay_series = delay_series.astype('Int64')
    except pd.errors.IntCastingNaNError:
        logger.debug("Atrasos contêm valores NA, mantendo tipo float/object.")


    logger.info("Cálculo de atraso atual concluído.")
    return delay_series