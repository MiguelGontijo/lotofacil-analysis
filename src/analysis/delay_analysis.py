# src/analysis/delay_analysis.py

import pandas as pd
from typing import Optional, Dict, List, Set # Adicionado Set

# Importações locais
from src.database_manager import read_data_from_db
# Removido ALL_NUMBERS da importação do config
from src.config import logger, NEW_BALL_COLUMNS

# Define ALL_NUMBERS localmente neste módulo
ALL_NUMBERS: List[int] = list(range(1, 26))
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS


def calculate_current_delay(concurso_maximo: Optional[int] = None) -> Optional[pd.Series]:
    """
    Calcula o atraso atual das dezenas.
    RETORNA: Series com atraso atual ou None.
    """
    logger.info(f"Calculando atraso atual até {concurso_maximo or 'último'}...")
    # Usa a leitura de dados padrão (ordenada ASC)
    df = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo)
    if df is None or df.empty:
        logger.warning("Nenhum dado encontrado para calcular atrasos.")
        return None
    if not all(col in df.columns for col in NEW_BALL_COLUMNS):
        logger.error("Dados lidos não contêm todas as colunas de bolas esperadas.")
        return None

    effective_max_concurso_val = df['concurso'].max()
    if pd.isna(effective_max_concurso_val): # Checagem adicional
         logger.error("Não foi possível determinar o concurso máximo efetivo.")
         return None
    effective_max_concurso = int(effective_max_concurso_val)

    logger.info(f"Ref. atraso: Concurso {effective_max_concurso}")

    last_seen: Dict[int, int] = {} # Armazena {numero: concurso}
    # Itera do mais recente para o mais antigo
    for index, row in df.iloc[::-1].iterrows():
        current_concurso_scan_val = row['concurso']
        if pd.isna(current_concurso_scan_val): continue
        current_concurso_scan = int(current_concurso_scan_val)

        drawn_numbers: Set[int] = set(int(num) for num in row[NEW_BALL_COLUMNS].dropna().values)
        for number in ALL_NUMBERS:
            if number not in last_seen and number in drawn_numbers:
                last_seen[number] = current_concurso_scan
        if len(last_seen) == len(ALL_NUMBERS): # Usar len(ALL_NUMBERS) é mais genérico
            logger.debug(f"Última ocorrência de todas as dezenas encontrada até o concurso {current_concurso_scan}.")
            break # Otimização

    delays: Dict[int, object] = {} # Usar 'object' para permitir pd.NA
    for number in ALL_NUMBERS:
        last_seen_concurso = last_seen.get(number)
        if last_seen_concurso is not None:
            delays[number] = effective_max_concurso - last_seen_concurso
        else:
            logger.warning(f"Dezena {number} não encontrada no período. Atraso indefinido (NA).")
            delays[number] = pd.NA

    delay_series = pd.Series(delays, name='Atraso Atual').sort_index()
    try:
        # Tenta converter para Int64, que suporta NA
        delay_series = delay_series.astype('Int64')
    except (pd.errors.IntCastingNaNError, TypeError):
        logger.debug("Atrasos contêm NAs ou tipos mistos, mantendo object/float.")

    logger.info("Cálculo de atraso atual concluído.")
    return delay_series # <<< RETORNA A SERIES


def calculate_max_delay(concurso_maximo: Optional[int] = None) -> Optional[pd.Series]:
    """
    Calcula o atraso máximo histórico.
    RETORNA: Series com atraso máximo ou None.
    """
    logger.info(f"Calculando atraso máximo histórico até {concurso_maximo or 'último'}...")
    df = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo)
    if df is None or df.empty:
        logger.warning("Nenhum dado encontrado para calcular atraso máximo.")
        return None
    if not all(col in df.columns for col in NEW_BALL_COLUMNS):
        logger.error("Dados lidos não contêm todas as colunas de bolas esperadas.")
        return None

    effective_max_concurso_val = df['concurso'].max()
    first_concurso_val = df['concurso'].min()

    if pd.isna(effective_max_concurso_val) or pd.isna(first_concurso_val):
        logger.error("Não foi possível determinar o período de concursos (min/max).")
        return None
    effective_max_concurso = int(effective_max_concurso_val)
    first_concurso = int(first_concurso_val)

    last_seen_concurso: Dict[int, int] = {n: first_concurso - 1 for n in ALL_NUMBERS}
    max_delay: Dict[int, int] = {n: 0 for n in ALL_NUMBERS}

    logger.info(f"Analisando concursos de {first_concurso} a {effective_max_concurso} para atraso máximo...")

    for index, row in df.iterrows():
        current_concurso_val = row['concurso']
        if pd.isna(current_concurso_val): continue
        current_concurso = int(current_concurso_val)

        drawn_numbers: Set[int] = set(int(num) for num in row[NEW_BALL_COLUMNS].dropna().values)

        for n in ALL_NUMBERS:
            if n in drawn_numbers:
                if last_seen_concurso[n] >= first_concurso: # Verifica se já foi visto antes no período
                     current_delay = current_concurso - last_seen_concurso[n] - 1
                     max_delay[n] = max(max_delay[n], current_delay)
                # else: Primeira aparição, o atraso "anterior" não conta ou é zero
                last_seen_concurso[n] = current_concurso # Atualiza último visto

    logger.debug("Verificando atraso final após o último sorteio...")
    for n in ALL_NUMBERS:
         if last_seen_concurso[n] >= first_concurso: # Só calcula se já foi visto
              final_delay = effective_max_concurso - last_seen_concurso[n]
              max_delay[n] = max(max_delay[n], final_delay)
         else:
             # Nunca visto no período
             logger.warning(f"Dezena {n} nunca vista até {effective_max_concurso}.")
             max_delay[n] = effective_max_concurso - first_concurso + 1


    max_delay_series = pd.Series(max_delay, name='Atraso Máximo Histórico').sort_index().astype(int)
    logger.info("Cálculo de atraso máximo histórico concluído.")
    return max_delay_series # <<< RETORNA A SERIES