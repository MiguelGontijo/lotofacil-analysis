# src/analysis/cycle_analysis.py

import pandas as pd
from typing import List, Dict, Optional, Tuple, Set

from src.database_manager import read_data_from_db
from src.config import logger, NEW_BALL_COLUMNS
from src.analysis.frequency_analysis import calculate_frequency

ALL_NUMBERS_SET: Set[int] = set(range(1, 26))
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS

# --- FUNÇÃO MODIFICADA ---
def identify_cycles(concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """
    Identifica os ciclos completos da Lotofácil ATÉ um concurso_maximo.
    RETORNA: DataFrame com info dos ciclos ou None.
    """
    period_str = f"até {concurso_maximo}" if concurso_maximo else "em todo o histórico"
    logger.info(f"Iniciando identificação de ciclos {period_str}...")
    # Lê dados apenas até o concurso_maximo, se fornecido
    df = read_data_from_db(columns=BASE_COLS, concurso_maximo=concurso_maximo)

    if df is None or df.empty:
        logger.warning(f"Nenhum dado encontrado {period_str} para identificar ciclos.")
        return None # Retorna None se não houver dados no período
    if not all(col in df.columns for col in NEW_BALL_COLUMNS): return None # Erro já logado

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

    logger.info(f"Identificação de ciclos {period_str} concluída. {cycle_count} ciclos completos.")
    if not cycles_data: return pd.DataFrame(columns=['numero_ciclo', 'concurso_inicio', 'concurso_fim', 'duracao'])
    return pd.DataFrame(cycles_data)


# --- FUNÇÃO MODIFICADA ---
def calculate_frequency_per_cycle(cycles_df: pd.DataFrame) -> Dict[int, Optional[pd.Series]]:
    """ Calcula a frequência para CADA ciclo completo no DataFrame fornecido. """
    cycle_frequencies: Dict[int, Optional[pd.Series]] = {}
    if cycles_df is None or cycles_df.empty:
        logger.warning("DataFrame de ciclos vazio para calcular frequência por ciclo.")
        return cycle_frequencies

    logger.info(f"Calculando frequência para {len(cycles_df)} ciclos...")
    for index, row in cycles_df.iterrows():
        cycle_num, start_c, end_c = int(row['numero_ciclo']), int(row['concurso_inicio']), int(row['concurso_fim'])
        logger.debug(f"Calculando freq. p/ Ciclo {cycle_num} ({start_c}-{end_c})...")
        cycle_frequencies[cycle_num] = calculate_frequency(concurso_minimo=start_c, concurso_maximo=end_c)

    logger.info("Cálculo de frequência por ciclo concluído.")
    return cycle_frequencies


# --- FUNÇÃO MODIFICADA ---
def calculate_current_incomplete_cycle_stats(concurso_maximo: int) -> Tuple[Optional[int], Optional[Set[int]], Optional[pd.Series]]:
    """ Identifica e calcula stats do ciclo incompleto atual ATÉ concurso_maximo. """
    logger.info(f"Identificando ciclo incompleto e calculando stats até {concurso_maximo}...")
    # 1. Identifica ciclos completos APENAS até concurso_maximo
    #    Agora identify_cycles faz isso eficientemente.
    relevant_cycles = identify_cycles(concurso_maximo=concurso_maximo)

    start_of_current_cycle: Optional[int] = None
    # Se identify_cycles falhou ou retornou vazio, tentamos pegar o primeiro concurso
    if relevant_cycles is None :
         logger.error("Falha ao identificar ciclos para achar o ciclo atual.")
         # Tenta ler só o primeiro concurso para definir o início
         df_min = read_data_from_db(columns=['concurso'], concurso_maximo=concurso_maximo)
         if df_min is not None and not df_min.empty:
             min_c = df_min['concurso'].min()
             start_of_current_cycle = int(min_c) if not pd.isna(min_c) else None
    elif relevant_cycles.empty:
        # Nenhum ciclo completo antes, ciclo atual começa no início dos dados ATÉ concurso_maximo
        df_min = read_data_from_db(columns=['concurso'], concurso_maximo=concurso_maximo)
        if df_min is not None and not df_min.empty:
            min_c = df_min['concurso'].min()
            start_of_current_cycle = int(min_c) if not pd.isna(min_c) else None
            logger.info(f"Nenhum ciclo completo até {concurso_maximo}, ciclo atual começa em {start_of_current_cycle}.")
    else:
        # Ciclo atual começa depois do fim do último ciclo completo encontrado até concurso_maximo
        last_complete_cycle_end = relevant_cycles['concurso_fim'].max()
        start_of_current_cycle = int(last_complete_cycle_end + 1)

    if start_of_current_cycle is None or start_of_current_cycle > concurso_maximo :
        logger.warning(f"Não há ciclo incompleto válido até {concurso_maximo}.")
        return None, None, None

    logger.info(f"Ciclo incompleto atual iniciado em {start_of_current_cycle} (até {concurso_maximo}).")

    # 2. Calcula frequência dentro deste ciclo incompleto
    current_cycle_freq = calculate_frequency(concurso_minimo=start_of_current_cycle, concurso_maximo=concurso_maximo)

    # 3. Identifica números sorteados neste ciclo
    current_cycle_numbers_drawn: Optional[Set[int]] = None
    if current_cycle_freq is not None:
        current_cycle_numbers_drawn = set(current_cycle_freq[current_cycle_freq > 0].index)
        logger.info(f"{len(current_cycle_numbers_drawn)} dezenas sorteadas no ciclo incompleto atual.")

    return start_of_current_cycle, current_cycle_numbers_drawn, current_cycle_freq

# A função run_cycle_frequency_analysis (que imprime) permanece igual
def run_cycle_frequency_analysis(cycles_df: pd.DataFrame, num_cycles_each_end: int = 3):
    """ Calcula e EXIBE a frequência das dezenas para ciclos selecionados. """
    if cycles_df is None or cycles_df.empty: return # Log já feito antes
    logger.info(f"Calculando/Exibindo frequência para ciclos selecionados...")
    # Calcula frequência para TODOS os ciclos no DF fornecido
    cycle_freq_dict = calculate_frequency_per_cycle(cycles_df)
    if not cycle_freq_dict: return # Log já feito antes

    # Lógica para selecionar ciclos para exibição (igual anterior)
    cycles_to_display_map: Dict[str, int] = {}
    total_cycles = len(cycles_df); n = num_cycles_each_end
    if total_cycles > 0: # Primeiros N
        for i in range(min(n, total_cycles)): row = cycles_df.iloc[i]; cn=int(row['numero_ciclo']); cycles_to_display_map[f"Ciclo {cn} ({int(row['duracao'])}c) [{int(row['concurso_inicio'])}-{int(row['concurso_fim'])}]"] = cn
    if total_cycles > n: # Ultimos N
        start_idx = max(n, total_cycles - n)
        for i in range(total_cycles - 1, start_idx - 1, -1): row = cycles_df.iloc[i]; cn=int(row['numero_ciclo']); name = f"Ciclo {cn} ({int(row['duracao'])}c) [{int(row['concurso_inicio'])}-{int(row['concurso_fim'])}]"; cycles_to_display_map.setdefault(name, cn)
    try: # Extremos
        sc = cycles_df.loc[cycles_df['duracao'].idxmin()]; sn=int(sc['numero_ciclo']); ns=f"Ciclo Mais Curto ({int(sc['duracao'])}c-nº{sn}) [{int(sc['concurso_inicio'])}-{int(sc['concurso_fim'])}]"; cycles_to_display_map.setdefault(ns, sn)
        lc = cycles_df.loc[cycles_df['duracao'].idxmax()]; ln=int(lc['numero_ciclo']); nl=f"Ciclo Mais Longo ({int(lc['duracao'])}c-nº{ln}) [{int(lc['concurso_inicio'])}-{int(lc['concurso_fim'])}]"; cycles_to_display_map.setdefault(nl, ln)
    except ValueError: logger.warning("Não foi possível determinar ciclos extremos.")

    print("\n--- Análise de Frequência Dentro de Ciclos Selecionados ---")
    display_items = sorted([(num, nome) for nome, num in cycles_to_display_map.items()])
    for cycle_num, cycle_name in display_items:
        freq_series = cycle_freq_dict.get(cycle_num)
        if freq_series is not None and not freq_series.empty:
            print(f"\n>> Frequência no {cycle_name} <<"); print("Top 5 Mais Frequentes:"); print(freq_series.nlargest(5).to_string()); print("\nTop 5 Menos Frequentes:"); print(freq_series.nsmallest(5).to_string()); print("-" * 30)
        else: logger.warning(f"Frequência não encontrada/vazia para ciclo {cycle_num}")
    logger.info("Análise de frequência por ciclo concluída (resumo impresso).")