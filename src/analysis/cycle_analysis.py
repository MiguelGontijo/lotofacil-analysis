# src/analysis/cycle_analysis.py

import pandas as pd
from typing import List, Dict, Optional, Tuple, Set

# Importações locais
from src.database_manager import read_data_from_db
# Importa logger e colunas do config
from src.config import logger, NEW_BALL_COLUMNS
# Importa a função de cálculo de frequência atualizada
from src.analysis.frequency_analysis import calculate_frequency

# Define ALL_NUMBERS_SET localmente neste módulo
ALL_NUMBERS_SET: Set[int] = set(range(1, 26))
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS

def identify_cycles() -> Optional[pd.DataFrame]:
    """
    Identifica os ciclos completos da Lotofácil.
    (Código desta função permanece o mesmo da versão anterior correta)
    """
    logger.info("Iniciando identificação de ciclos da Lotofácil...")
    df = read_data_from_db(columns=BASE_COLS)
    if df is None or df.empty:
        logger.warning("Nenhum dado encontrado para identificar ciclos.")
        return None
    if not all(col in df.columns for col in NEW_BALL_COLUMNS):
        logger.error("Dados lidos não contêm todas as colunas de bolas esperadas.")
        return None

    cycles_data: List[Dict] = []
    current_cycle_numbers: Set[int] = set()
    min_concurso_val = df['concurso'].min()
    if pd.isna(min_concurso_val):
         logger.error("Coluna 'concurso' contém valores nulos ou inesperados no início.")
         return None
    cycle_start_concurso: int = int(min_concurso_val)

    cycle_count: int = 0
    last_valid_concurso_val = df['concurso'].max()
    last_valid_concurso: int = 0
    if not pd.isna(last_valid_concurso_val):
        last_valid_concurso = int(last_valid_concurso_val)

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
            logger.debug(f"Ciclo {cycle_count} concluído no concurso {cycle_end_concurso}.")
            cycle_start_concurso = cycle_end_concurso + 1
            current_cycle_numbers = set()

    logger.info(f"Identificação de ciclos concluída. {cycle_count} ciclos completos encontrados.")
    if not cycles_data:
        logger.warning("Nenhum ciclo completo foi identificado.")
        return pd.DataFrame(columns=['numero_ciclo', 'concurso_inicio', 'concurso_fim', 'duracao'])
    return pd.DataFrame(cycles_data)


# Função para analisar frequência dentro dos ciclos (COM A CORREÇÃO DE SINTAXE)
def run_cycle_frequency_analysis(cycles_df: pd.DataFrame, num_cycles_each_end: int = 3):
    """
    Calcula e exibe a frequência das dezenas para ciclos selecionados.
    """
    if cycles_df is None or cycles_df.empty:
        logger.warning("DataFrame de ciclos vazio. Nenhuma análise por ciclo realizada.")
        return

    logger.info(f"Iniciando análise de frequência para ciclos selecionados...")
    cycles_to_analyze: Dict[str, Tuple[int, int]] = {}
    total_cycles = len(cycles_df)

    # Primeiros N ciclos
    if total_cycles > 0:
        for i in range(min(num_cycles_each_end, total_cycles)):
            row = cycles_df.iloc[i]
            name = f"Ciclo {row['numero_ciclo']} ({row['duracao']} conc.)"
            cycles_to_analyze[name] = (int(row['concurso_inicio']), int(row['concurso_fim']))

    # Últimos N ciclos
    if total_cycles > num_cycles_each_end:
         start_index = max(num_cycles_each_end, total_cycles - num_cycles_each_end)
         for i in range(total_cycles - 1, start_index - 1, -1):
             row = cycles_df.iloc[i]
             name = f"Ciclo {row['numero_ciclo']} ({row['duracao']} conc.)"
             if name not in cycles_to_analyze:
                  cycles_to_analyze[name] = (int(row['concurso_inicio']), int(row['concurso_fim']))

    # Ciclo mais curto e mais longo
    try:
        shortest_cycle_idx = cycles_df['duracao'].idxmin()
        shortest_cycle = cycles_df.loc[shortest_cycle_idx]
        name_short = f"Ciclo Mais Curto ({shortest_cycle['duracao']} conc. - nº {shortest_cycle['numero_ciclo']})"
        if name_short not in cycles_to_analyze:
            cycles_to_analyze[name_short] = (int(shortest_cycle['concurso_inicio']), int(shortest_cycle['concurso_fim']))

        longest_cycle_idx = cycles_df['duracao'].idxmax()
        longest_cycle = cycles_df.loc[longest_cycle_idx]
        name_long = f"Ciclo Mais Longo ({longest_cycle['duracao']} conc. - nº {longest_cycle['numero_ciclo']})"
        if name_long not in cycles_to_analyze:
            # *** LINHA CORRIGIDA ABAIXO ***
            cycles_to_analyze[name_long] = (int(longest_cycle['concurso_inicio']), int(longest_cycle['concurso_fim']))
    except ValueError:
        logger.warning("Não foi possível determinar ciclo mais curto/longo.")

    # Calcula e imprime frequência para cada ciclo selecionado
    print("\n--- Análise de Frequência Dentro de Ciclos Selecionados ---")
    for name, (start, end) in cycles_to_analyze.items():
        logger.debug(f"Calculando frequência para {name} (Concursos {start}-{end})")
        freq_series = calculate_frequency(concurso_minimo=start, concurso_maximo=end)
        if freq_series is not None:
            print(f"\n>> Frequência no {name} (Concursos {start}-{end}) <<")
            print("Top 5 Mais Frequentes:")
            print(freq_series.nlargest(5).to_string())
            print("\nTop 5 Menos Frequentes:")
            print(freq_series.nsmallest(5).to_string())
            print("-" * 30)
        else:
            logger.warning(f"Não foi possível calcular a frequência para {name}")

    logger.info("Análise de frequência por ciclo concluída.")