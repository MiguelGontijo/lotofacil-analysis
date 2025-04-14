# src/analysis/cycle_analysis.py

import pandas as pd
from typing import List, Dict, Optional, Tuple, Set

from src.database_manager import read_data_from_db
from src.config import logger, NEW_BALL_COLUMNS
from src.analysis.frequency_analysis import calculate_frequency # Importa função que retorna Series

ALL_NUMBERS_SET: Set[int] = set(range(1, 26))
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS

def identify_cycles() -> Optional[pd.DataFrame]:
    """ Identifica os ciclos completos da Lotofácil. Retorna DataFrame. """
    # (Código da função identify_cycles permanece o mesmo da versão anterior correta)
    logger.info("Iniciando identificação de ciclos...")
    df = read_data_from_db(columns=BASE_COLS)
    if df is None or df.empty: return None
    if not all(col in df.columns for col in NEW_BALL_COLUMNS): return None
    cycles_data: List[Dict] = []
    current_cycle_numbers: Set[int] = set()
    min_concurso_val = df['concurso'].min()
    if pd.isna(min_concurso_val): return None
    cycle_start_concurso: int = int(min_concurso_val)
    cycle_count: int = 0
    last_valid_concurso_val = df['concurso'].max()
    last_valid_concurso: int = 0
    if not pd.isna(last_valid_concurso_val): last_valid_concurso = int(last_valid_concurso_val)
    logger.info(f"Analisando concursos de {cycle_start_concurso} a {last_valid_concurso}...")
    for index, row in df.iterrows():
        current_concurso_val = row['concurso']
        if pd.isna(current_concurso_val): continue
        current_concurso: int = int(current_concurso_val)
        drawn_numbers: Set[int] = set(int(num) for num in row[NEW_BALL_COLUMNS].dropna().values)
        current_cycle_numbers.update(drawn_numbers)
        if current_cycle_numbers == ALL_NUMBERS_SET:
            cycle_count += 1
            cycles_data.append({
                'numero_ciclo': cycle_count, 'concurso_inicio': cycle_start_concurso,
                'concurso_fim': current_concurso, 'duracao': current_concurso - cycle_start_concurso + 1
            })
            cycle_start_concurso = current_concurso + 1
            current_cycle_numbers = set()
    logger.info(f"Identificação de ciclos concluída. {cycle_count} ciclos completos.")
    if not cycles_data: return pd.DataFrame(columns=[...])
    return pd.DataFrame(cycles_data)


# --- FUNÇÃO MODIFICADA ---
def calculate_frequency_per_cycle(cycles_df: pd.DataFrame) -> Dict[int, Optional[pd.Series]]:
    """
    Calcula a frequência das dezenas para CADA ciclo completo.
    RETORNA: Dicionário {numero_ciclo: Series_de_frequencia} ou vazio.
    """
    cycle_frequencies: Dict[int, Optional[pd.Series]] = {}
    if cycles_df is None or cycles_df.empty:
        logger.warning("DataFrame de ciclos vazio. Não é possível calcular frequência por ciclo.")
        return cycle_frequencies

    logger.info(f"Calculando frequência para {len(cycles_df)} ciclos completos...")
    for index, row in cycles_df.iterrows():
        cycle_num = int(row['numero_ciclo'])
        start_c = int(row['concurso_inicio'])
        end_c = int(row['concurso_fim'])
        logger.debug(f"Calculando freq. para Ciclo {cycle_num} ({start_c}-{end_c})...")
        freq_series = calculate_frequency(concurso_minimo=start_c, concurso_maximo=end_c)
        cycle_frequencies[cycle_num] = freq_series
        # Opcional: Adicionar verificação se freq_series é None

    logger.info("Cálculo de frequência por ciclo concluído.")
    return cycle_frequencies


# --- NOVA FUNÇÃO ---
def calculate_current_incomplete_cycle_stats(concurso_maximo: int) -> Tuple[Optional[int], Optional[Set[int]], Optional[pd.Series]]:
    """
    Identifica o início do ciclo incompleto atual (até concurso_maximo),
    os números que já saíram nele, e calcula a frequência dentro dele.

    Args:
        concurso_maximo (int): O último concurso a considerar.

    Returns:
        Tuple[Optional[int], Optional[Set[int]], Optional[pd.Series]]: Retorna uma tupla contendo:
            - O concurso de início do ciclo atual (ou None).
            - O conjunto de números sorteados neste ciclo (ou None).
            - A Series de frequência das dezenas neste ciclo (ou None).
    """
    logger.info(f"Identificando ciclo incompleto atual e calculando stats até {concurso_maximo}...")
    # 1. Identifica todos os ciclos completos até o concurso_maximo
    #    Precisamos ler os dados para identify_cycles funcionar corretamente até o ponto certo
    #    (Idealmente, identify_cycles aceitaria max_concurso, mas por ora fazemos assim)
    all_cycles_df = identify_cycles() # Requer leitura completa do BD, pode ser ineficiente

    if all_cycles_df is None:
         logger.error("Falha ao identificar ciclos para achar o ciclo atual.")
         return None, None, None

    # Filtra ciclos que terminaram *antes* ou *no* concurso máximo
    relevant_cycles = all_cycles_df[all_cycles_df['concurso_fim'] <= concurso_maximo]

    start_of_current_cycle: Optional[int] = None
    if relevant_cycles.empty:
        # Nenhum ciclo completo antes, ciclo atual começa no início
        df_min_concurso = read_data_from_db(columns=['concurso']) # Leitura mínima
        if df_min_concurso is not None and not df_min_concurso.empty:
             min_c = df_min_concurso['concurso'].min()
             if not pd.isna(min_c): start_of_current_cycle = int(min_c)
    else:
        # Ciclo atual começa depois do fim do último ciclo completo
        last_complete_cycle_end = relevant_cycles['concurso_fim'].max()
        start_of_current_cycle = int(last_complete_cycle_end + 1)

    if start_of_current_cycle is None or start_of_current_cycle > concurso_maximo :
        logger.warning(f"Não foi possível determinar início válido para ciclo incompleto até {concurso_maximo}.")
        # Pode acontecer se concurso_maximo for o exato fim de um ciclo
        # Nesse caso, não há ciclo incompleto, retornamos None
        return None, None, None # Não há ciclo incompleto a analisar

    logger.info(f"Ciclo incompleto atual iniciado em {start_of_current_cycle}.")

    # 2. Calcula frequência e números vistos dentro deste ciclo incompleto
    # Usa a função calculate_frequency que já lê o período correto
    current_cycle_freq = calculate_frequency(concurso_minimo=start_of_current_cycle, concurso_maximo=concurso_maximo)

    # 3. Identifica os números que já saíram neste ciclo
    current_cycle_numbers_drawn: Optional[Set[int]] = None
    if current_cycle_freq is not None:
        # Números com frequência > 0 saíram neste ciclo
        current_cycle_numbers_drawn = set(current_cycle_freq[current_cycle_freq > 0].index)
        logger.info(f"{len(current_cycle_numbers_drawn)} dezenas sorteadas no ciclo incompleto atual.")

    return start_of_current_cycle, current_cycle_numbers_drawn, current_cycle_freq