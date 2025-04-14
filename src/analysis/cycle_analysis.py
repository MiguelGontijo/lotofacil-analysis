# src/analysis/cycle_analysis.py

import pandas as pd
from typing import List, Dict, Optional, Tuple, Set

from src.database_manager import read_data_from_db
from src.config import logger, NEW_BALL_COLUMNS
from src.analysis.frequency_analysis import calculate_frequency # Importa função que retorna Series

ALL_NUMBERS_SET: Set[int] = set(range(1, 26))
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS

def identify_cycles() -> Optional[pd.DataFrame]:
    """
    Identifica os ciclos completos da Lotofácil.
    RETORNA: DataFrame com info dos ciclos ou None.
    """
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
            cycle_end_concurso: int = current_concurso
            duration: int = cycle_end_concurso - cycle_start_concurso + 1
            cycles_data.append({
                'numero_ciclo': cycle_count, 'concurso_inicio': cycle_start_concurso,
                'concurso_fim': cycle_end_concurso, 'duracao': duration
            })
            cycle_start_concurso = cycle_end_concurso + 1
            current_cycle_numbers = set()

    logger.info(f"Identificação de ciclos concluída. {cycle_count} ciclos completos.")
    if not cycles_data: return pd.DataFrame(columns=[...]) # Como antes
    return pd.DataFrame(cycles_data) # <<< RETORNA DATAFRAME


def run_cycle_frequency_analysis(cycles_df: pd.DataFrame, num_cycles_each_end: int = 3):
    """
    Calcula e EXIBE (não retorna agregado) a frequência das dezenas para ciclos selecionados.
    """
    if cycles_df is None or cycles_df.empty:
        logger.warning("DataFrame de ciclos vazio.")
        return

    logger.info(f"Iniciando análise de frequência para ciclos selecionados...")
    cycles_to_analyze: Dict[str, Tuple[int, int]] = {}
    # ... (lógica para selecionar ciclos igual à anterior) ...
    total_cycles = len(cycles_df)
    if total_cycles > 0:
        for i in range(min(num_cycles_each_end, total_cycles)):
            row = cycles_df.iloc[i]
            name = f"Ciclo {row['numero_ciclo']} ({row['duracao']} conc.)"
            cycles_to_analyze[name] = (int(row['concurso_inicio']), int(row['concurso_fim']))
    if total_cycles > num_cycles_each_end:
         start_index = max(num_cycles_each_end, total_cycles - num_cycles_each_end)
         for i in range(total_cycles - 1, start_index - 1, -1):
             row = cycles_df.iloc[i]
             name = f"Ciclo {row['numero_ciclo']} ({row['duracao']} conc.)"
             if name not in cycles_to_analyze: cycles_to_analyze[name] = (int(row['concurso_inicio']), int(row['concurso_fim']))
    try:
        shortest_cycle = cycles_df.loc[cycles_df['duracao'].idxmin()]
        name_short = f"Ciclo Mais Curto ({shortest_cycle['duracao']} conc. - nº {shortest_cycle['numero_ciclo']})"
        if name_short not in cycles_to_analyze: cycles_to_analyze[name_short] = (int(shortest_cycle['concurso_inicio']), int(shortest_cycle['concurso_fim']))
        longest_cycle = cycles_df.loc[cycles_df['duracao'].idxmax()]
        name_long = f"Ciclo Mais Longo ({longest_cycle['duracao']} conc. - nº {longest_cycle['numero_ciclo']})"
        if name_long not in cycles_to_analyze: cycles_to_analyze[name_long] = (int(longest_cycle['concurso_inicio']), int(longest_cycle['concurso_fim']))
    except ValueError: logger.warning("Não foi possível determinar ciclo curto/longo.")


    # Calcula e imprime frequência para cada ciclo selecionado
    print("\n--- Análise de Frequência Dentro de Ciclos Selecionados ---")
    for name, (start, end) in cycles_to_analyze.items():
        logger.debug(f"Calculando frequência para {name} (Concursos {start}-{end})")
        # Chama a função que RETORNA a Series
        freq_series = calculate_frequency(concurso_minimo=start, concurso_maximo=end)
        # IMPRIME o resultado aqui
        if freq_series is not None:
            print(f"\n>> Frequência no {name} (Concursos {start}-{end}) <<")
            print("Top 5 Mais Frequentes:")
            print(freq_series.nlargest(5).to_string())
            print("\nTop 5 Menos Frequentes:")
            print(freq_series.nsmallest(5).to_string())
            print("-" * 30)
        else:
            logger.warning(f"Não foi possível calcular a frequência para {name}")

    logger.info("Análise de frequência por ciclo concluída (resultados impressos).")