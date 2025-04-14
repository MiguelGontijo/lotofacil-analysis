# src/analysis/cycle_analysis.py

import pandas as pd
from typing import List, Dict, Optional

# Importações locais
from src.database_manager import read_data_from_db
from src.config import logger, NEW_BALL_COLUMNS

# Colunas necessárias do banco de dados
BASE_COLS = ['concurso'] + NEW_BALL_COLUMNS
ALL_NUMBERS_SET = set(range(1, 26)) # Conjunto com todas as 25 dezenas

def identify_cycles() -> Optional[pd.DataFrame]:
    """
    Identifica os ciclos completos da Lotofácil, onde um ciclo se encerra
    quando todas as 25 dezenas foram sorteadas pelo menos uma vez desde
    o início do ciclo.

    Returns:
        Optional[pd.DataFrame]: Um DataFrame com informações de cada ciclo completo
                                 (numero_ciclo, concurso_inicio, concurso_fim, duracao),
                                 ou None se ocorrer erro ou não houver dados.
                                 Retorna DataFrame vazio se nenhum ciclo completo for encontrado.
    """
    logger.info("Iniciando identificação de ciclos da Lotofácil...")

    # Ler todos os dados, ordenados por concurso
    df = read_data_from_db(columns=BASE_COLS) # Lê todas as colunas necessárias

    if df is None or df.empty:
        logger.warning("Nenhum dado encontrado para identificar ciclos.")
        return None

    # Verifica se todas as colunas de bolas esperadas estão presentes
    if not all(col in df.columns for col in NEW_BALL_COLUMNS):
        logger.error("Dados lidos do banco não contêm todas as colunas de bolas esperadas (b1 a b15).")
        return None

    cycles_data = []
    current_cycle_numbers = set()
    cycle_start_concurso = df['concurso'].min() # Começa no primeiro concurso dos dados
    cycle_count = 0

    logger.info(f"Analisando concursos de {cycle_start_concurso} a {df['concurso'].max()}...")

    for index, row in df.iterrows():
        current_concurso = row['concurso']
        # Pega as 15 dezenas sorteadas, tratando NAs e convertendo para int
        drawn_numbers = set(int(num) for num in row[NEW_BALL_COLUMNS].dropna().values)

        # Adiciona os números sorteados ao conjunto do ciclo atual
        current_cycle_numbers.update(drawn_numbers)

        # Verifica se o ciclo se completou (todas as 25 dezenas apareceram)
        if len(current_cycle_numbers) == 25:
            cycle_count += 1
            cycle_end_concurso = current_concurso
            duration = cycle_end_concurso - cycle_start_concurso + 1

            cycles_data.append({
                'numero_ciclo': cycle_count,
                'concurso_inicio': cycle_start_concurso,
                'concurso_fim': cycle_end_concurso,
                'duracao': duration
            })
            logger.debug(f"Ciclo {cycle_count} concluído no concurso {cycle_end_concurso}. Duração: {duration} concursos.")

            # Prepara para o próximo ciclo
            cycle_start_concurso = cycle_end_concurso + 1
            current_cycle_numbers = set() # Reinicia o conjunto

    logger.info(f"Identificação de ciclos concluída. {cycle_count} ciclos completos encontrados.")

    if not cycles_data:
        logger.warning("Nenhum ciclo completo foi identificado nos dados.")
        return pd.DataFrame(columns=['numero_ciclo', 'concurso_inicio', 'concurso_fim', 'duracao'])

    # Cria um DataFrame com os resultados
    cycles_df = pd.DataFrame(cycles_data)
    return cycles_df