# src/analysis/cycle_analysis.py

import pandas as pd
import numpy as np
import sqlite3
from typing import List, Dict, Optional, Tuple, Set

# Importa funções do DB Manager (SEM get_cycles_df)
from src.database_manager import read_data_from_db, create_cycles_table, get_last_cycle_end, save_to_db
# Importa do config
from src.config import (
    logger, NEW_BALL_COLUMNS, ALL_NUMBERS, ALL_NUMBERS_SET,
    DATABASE_PATH, TABLE_NAME, CYCLES_TABLE_NAME
)
# Importa a função de cálculo de frequência
from src.analysis.frequency_analysis import calculate_frequency

# Usa constantes do config
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS


# --- FUNÇÃO INTERNA PARA ENCONTRAR CICLOS EM UM DF ---
def _find_new_cycles_in_data(df_new_data: pd.DataFrame, start_contest_for_search: int, initial_seen_numbers: Set[int], last_cycle_number: int) -> List[Dict]:
    # (Código idêntico ao da última versão correta)
    new_cycles_found: List[Dict] = []; current_cycle_numbers = initial_seen_numbers.copy(); cycle_start_concurso = start_contest_for_search; cycle_count = last_cycle_number
    logger.info(f"Buscando novos ciclos a partir do concurso {cycle_start_concurso}...")
    for index, row in df_new_data.iterrows():
        current_concurso_val = row['concurso'];
        if pd.isna(current_concurso_val): continue
        current_concurso: int = int(current_concurso_val)
        if current_concurso < start_contest_for_search: continue
        drawn_numbers: Set[int] = set(int(num) for num in row[NEW_BALL_COLUMNS].dropna().values)
        current_cycle_numbers.update(drawn_numbers)
        if current_cycle_numbers == ALL_NUMBERS_SET:
            cycle_count += 1; cycle_end_concurso: int = current_concurso; duration: int = cycle_end_concurso - cycle_start_concurso + 1
            new_cycles_found.append({'numero_ciclo': cycle_count, 'concurso_inicio': cycle_start_concurso, 'concurso_fim': cycle_end_concurso, 'duracao': duration})
            logger.debug(f"Novo ciclo completo: {cycle_count} (Fim: {cycle_end_concurso})")
            cycle_start_concurso = cycle_end_concurso + 1; current_cycle_numbers = set()
    logger.info(f"{len(new_cycles_found)} novos ciclos completos encontrados.")
    return new_cycles_found


# --- FUNÇÃO PARA ATUALIZAR TABELA PERSISTENTE ---
def update_cycles_table(force_rebuild: bool = False):
    # (Código idêntico ao da última versão correta)
    logger.info(f"Atualizando tabela 'ciclos'... {'(Rebuild)' if force_rebuild else ''}")
    create_cycles_table(); last_recorded_end = -1; last_cycle_num = 0; start_search_from = 1
    db_path = DATABASE_PATH; cycles_table_name = CYCLES_TABLE_NAME
    if force_rebuild:
        logger.warning("REBUILD: Apagando dados de 'ciclos'.");
        try:
            with sqlite3.connect(db_path) as conn: conn.execute(f"DELETE FROM {cycles_table_name};")
        except sqlite3.Error as e: logger.error(f"Erro ao limpar 'ciclos': {e}"); return
    else:
        last_recorded_end_val = get_last_cycle_end()
        if last_recorded_end_val is not None:
             last_recorded_end = last_recorded_end_val; start_search_from = last_recorded_end + 1
             try:
                 with sqlite3.connect(db_path) as conn: res = conn.execute(f"SELECT MAX(numero_ciclo) FROM {cycles_table_name}").fetchone(); last_cycle_num = int(res[0]) if res and res[0] is not None else 0
             except sqlite3.Error as e: logger.error(f"Erro buscar MAX(numero_ciclo): {e}")
             logger.info(f"Último ciclo termina em {last_recorded_end}. Buscando a partir de {start_search_from}.")
        else: logger.info("Tabela 'ciclos' vazia. Buscando do início.")
    read_from = start_search_from; initial_numbers = set(); effective_last_cycle_num = 0
    if not force_rebuild and last_recorded_end > 0:
         try:
             with sqlite3.connect(db_path) as conn: last_cycle_info = pd.read_sql(f"SELECT concurso_inicio FROM {cycles_table_name} WHERE concurso_fim = ?", conn, params=(last_recorded_end,))
             if not last_cycle_info.empty:
                  read_from = int(last_cycle_info.iloc[0]['concurso_inicio']); logger.info(f"Recalculando estado a partir de {read_from}")
                  with sqlite3.connect(db_path) as conn: res = conn.execute(f"SELECT MAX(numero_ciclo) FROM {cycles_table_name} WHERE concurso_fim < ?", (read_from,)).fetchone(); effective_last_cycle_num = int(res[0]) if res and res[0] is not None else 0
             else: logger.warning(f"Info do último ciclo {last_recorded_end} não encontrada."); read_from = start_search_from
         except Exception as e: logger.error(f"Erro ao ler último ciclo: {e}."); read_from = start_search_from
    df_data_to_scan = read_data_from_db(table_name=TABLE_NAME, columns=BASE_COLS, concurso_minimo=read_from)
    if df_data_to_scan is None or df_data_to_scan.empty: logger.info(f"Nenhum dado novo em '{TABLE_NAME}' a partir de {read_from}."); return
    all_found_cycles = _find_new_cycles_in_data(df_data_to_scan, read_from, initial_numbers, effective_last_cycle_num)
    new_cycles = [c for c in all_found_cycles if c['concurso_fim'] > last_recorded_end]
    if new_cycles:
        new_cycles_df = pd.DataFrame(new_cycles); write_mode = 'replace' if force_rebuild else 'append'
        if write_mode == 'append' and last_recorded_end > 0:
             min_recalculated_start = new_cycles_df['concurso_inicio'].min()
             try:
                  with sqlite3.connect(db_path) as conn: conn.execute(f"DELETE FROM {cycles_table_name} WHERE concurso_inicio >= ?", (min_recalculated_start,)); logger.info(f"Ciclos antigos a partir de {min_recalculated_start} removidos.")
             except sqlite3.Error as e: logger.error(f"Erro ao deletar ciclos antigos p/ append: {e}")
        success = save_to_db(new_cycles_df, table_name=cycles_table_name, if_exists=write_mode)
        if success:
            logger.info(f"{len(new_cycles)} ciclos {'add/atualizados' if write_mode=='append' else 'reconstruídos'}.")
            try:
                with sqlite3.connect(db_path) as conn: conn.execute(f"CREATE INDEX IF NOT EXISTS idx_ciclos_numero ON {cycles_table_name} (numero_ciclo);"); conn.execute(f"CREATE INDEX IF NOT EXISTS idx_ciclos_concurso_fim ON {cycles_table_name} (concurso_fim);"); logger.info(f"Índices para '{cycles_table_name}' ok.")
            except sqlite3.Error as e: logger.error(f"Erro criar índices para '{cycles_table_name}': {e}")
        else: logger.error("Falha ao salvar novos ciclos no BD.")
    else: logger.info("Nenhum NOVO ciclo completo encontrado.")


# --- FUNÇÃO PARA LER CICLOS DA TABELA ---
# (Esta função está definida aqui e não precisa ser importada do db_manager)
def get_cycles_df(concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """ Lê os dados da tabela 'ciclos' do banco de dados. """
    cycles_table_name = CYCLES_TABLE_NAME # Usa constante do config
    logger.info(f"Buscando dados da tabela '{cycles_table_name}' {('ate ' + str(concurso_maximo)) if concurso_maximo else ''}...")
    create_cycles_table(); db_path = DATABASE_PATH;
    try:
        with sqlite3.connect(db_path) as conn:
            sql_query = f"SELECT numero_ciclo, concurso_inicio, concurso_fim, duracao FROM {cycles_table_name}"; params = []
            if concurso_maximo is not None: sql_query += " WHERE concurso_fim <= ?"; params.append(concurso_maximo)
            sql_query += " ORDER BY numero_ciclo ASC;"
            df = pd.read_sql_query(sql_query, conn, params=params); logger.info(f"{len(df)} ciclos lidos.");
            if not df.empty:
                 for col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            return df
    except sqlite3.Error as e: logger.error(f"Erro ao ler '{cycles_table_name}': {e}"); return None


# --- FUNÇÃO PARA CALCULAR FREQUÊNCIA POR CICLO ---
def calculate_frequency_per_cycle(cycles_df: pd.DataFrame) -> Dict[int, Optional[pd.Series]]:
    # (Código idêntico ao da última versão correta)
    cycle_frequencies: Dict[int, Optional[pd.Series]] = {};
    if cycles_df is None or cycles_df.empty: return cycle_frequencies
    logger.info(f"Calculando frequência para {len(cycles_df)} ciclos...");
    from src.analysis.frequency_analysis import calculate_frequency # Import local ok
    for index, row in cycles_df.iterrows():
        try: cycle_num = int(row['numero_ciclo']); start_c = int(row['concurso_inicio']); end_c = int(row['concurso_fim']); cycle_frequencies[cycle_num] = calculate_frequency(concurso_minimo=start_c, concurso_maximo=end_c)
        except Exception as e: logger.error(f"Erro processar linha ciclo {index}: {row}. E: {e}"); cycle_frequencies[int(row.get('numero_ciclo',-1))] = None
    logger.info("Cálculo de frequência por ciclo concluído."); return cycle_frequencies


# --- FUNÇÃO PARA CALCULAR STATS DO CICLO ATUAL INCOMPLETO ---
def calculate_current_incomplete_cycle_stats(concurso_maximo: int) -> Tuple[Optional[int], Optional[Set[int]], Optional[pd.Series]]:
    # (Código idêntico ao da última versão correta - já usa get_cycles_df local)
    logger.info(f"Identificando ciclo incompleto até {concurso_maximo}...")
    relevant_cycles = get_cycles_df(concurso_maximo=concurso_maximo) # Chama a função local
    start_of_current_cycle: Optional[int] = None
    if relevant_cycles is None : logger.error("Falha ler ciclos."); start_of_current_cycle = None
    elif relevant_cycles.empty: df_min = read_data_from_db(columns=['concurso'], concurso_maximo=concurso_maximo); min_c = df_min['concurso'].min() if df_min is not None else None; start_of_current_cycle = int(min_c) if not pd.isna(min_c) else None; logger.info(f"Nenhum ciclo completo na tabela até {concurso_maximo}.")
    else: last_complete_cycle_end_val = relevant_cycles['concurso_fim'].max(); start_of_current_cycle = int(last_complete_cycle_end_val + 1) if pd.notna(last_complete_cycle_end_val) else None
    if start_of_current_cycle is None or start_of_current_cycle > concurso_maximo : logger.warning(f"Não há ciclo incompleto válido até {concurso_maximo}."); return None, None, None
    logger.info(f"Ciclo incompleto atual: {start_of_current_cycle} - {concurso_maximo}.")
    current_cycle_freq = calculate_frequency(concurso_minimo=start_of_current_cycle, concurso_maximo=concurso_maximo)
    current_cycle_numbers_drawn: Optional[Set[int]] = None
    if current_cycle_freq is not None: current_cycle_numbers_drawn = set(current_cycle_freq[current_cycle_freq > 0].index); logger.info(f"{len(current_cycle_numbers_drawn)}/{len(ALL_NUMBERS)} dezenas sorteadas no ciclo atual.")
    return start_of_current_cycle, current_cycle_numbers_drawn, current_cycle_freq


# --- FUNÇÃO PARA CALCULAR ATRASO DENTRO DO CICLO ATUAL ---
def calculate_current_intra_cycle_delay(current_cycle_start: Optional[int], concurso_maximo: int) -> Optional[pd.Series]:
    # (Código idêntico ao da última versão correta)
    if current_cycle_start is None or current_cycle_start > concurso_maximo: return None
    logger.info(f"Calculando atraso intra-ciclo atual ({current_cycle_start} - {concurso_maximo})...")
    df_cycle = read_data_from_db(columns=BASE_COLS, concurso_minimo=current_cycle_start, concurso_maximo=concurso_maximo)
    if df_cycle is None: return None
    cycle_len_so_far = concurso_maximo - current_cycle_start + 1
    if df_cycle.empty: return pd.Series(cycle_len_so_far, index=ALL_NUMBERS, name='Atraso Intra-Ciclo').astype('Int64')
    last_seen_in_cycle: Dict[int, int] = {}
    for index, row in df_cycle.iloc[::-1].iterrows():
        current_concurso_scan = int(row['concurso'])
        drawn_numbers = set(int(num) for num in row[NEW_BALL_COLUMNS].dropna().values)
        for number in ALL_NUMBERS:
            if number not in last_seen_in_cycle and number in drawn_numbers: last_seen_in_cycle[number] = current_concurso_scan
        if len(last_seen_in_cycle) == len(ALL_NUMBERS): break
    delays: Dict[int, object] = {};
    for number in ALL_NUMBERS: last_seen = last_seen_in_cycle.get(number); delays[number] = (concurso_maximo - last_seen) if last_seen is not None else cycle_len_so_far
    delay_series = pd.Series(delays, name='Atraso Intra-Ciclo').sort_index().astype('Int64')
    logger.info("Cálculo de atraso intra-ciclo atual concluído."); return delay_series


# --- FUNÇÃO PARA CALCULAR STATS HISTÓRICOS DE ATRASO INTRA-CICLO ---
def calculate_historical_intra_cycle_delay_stats(cycles_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    # (Código idêntico ao da última versão correta)
    if cycles_df is None or cycles_df.empty: return None
    logger.info(f"Calculando stats históricos de atraso intra-ciclo para {len(cycles_df)} ciclos...")
    all_intra_delays: Dict[int, List[int]] = {n: [] for n in ALL_NUMBERS}; processed_cycles = 0
    for index, cycle_row in cycles_df.iterrows():
        start_c, end_c, cycle_num = int(cycle_row['concurso_inicio']), int(cycle_row['concurso_fim']), int(cycle_row['numero_ciclo'])
        df_cycle = read_data_from_db(columns=BASE_COLS, concurso_minimo=start_c, concurso_maximo=end_c)
        if df_cycle is None or df_cycle.empty: continue
        last_seen_in_this_cycle: Dict[int, int] = {}
        for _, draw_row in df_cycle.iterrows():
            current_concurso = int(draw_row['concurso'])
            drawn_numbers = set(int(num) for num in draw_row[NEW_BALL_COLUMNS].dropna().values)
            for n in drawn_numbers:
                if n in last_seen_in_this_cycle: delay = current_concurso - last_seen_in_this_cycle[n] - 1; all_intra_delays[n].append(delay)
                last_seen_in_this_cycle[n] = current_concurso
        processed_cycles += 1; #if processed_cycles % 100 == 0: logger.info(f"{processed_cycles}/{len(cycles_df)} ciclos processados...") # Log opcional
    stats_data = []
    for n in ALL_NUMBERS:
        delays = all_intra_delays[n]; mean_delay, max_delay_calc, std_dev_delay = np.nan, 0, np.nan
        if delays: mean_delay = np.mean(delays); max_delay_calc = np.max(delays);
        if len(delays) > 1: std_dev_delay = np.std(delays, ddof=1)
        elif len(delays) == 1: std_dev_delay = 0
        stats_data.append({'dezena': n, 'avg_hist_intra_delay': mean_delay, 'max_hist_intra_delay': max_delay_calc, 'std_hist_intra_delay': std_dev_delay})
    results_df = pd.DataFrame(stats_data).set_index('dezena')
    logger.info("Cálculo de stats históricos de atraso intra-ciclo concluído."); return results_df


# --- FUNÇÃO PARA EXIBIR STATS DE CICLOS SELECIONADOS ---
def run_cycle_frequency_analysis(cycles_df: pd.DataFrame, num_cycles_each_end: int = 3):
    # (Código idêntico ao da última versão correta, com indentação já corrigida)
    if cycles_df is None or cycles_df.empty: logger.warning("DataFrame de ciclos vazio."); return
    logger.info(f"Calculando/Exibindo frequência para ciclos selecionados...")
    cycle_freq_dict = calculate_frequency_per_cycle(cycles_df);
    if not cycle_freq_dict: logger.warning("Dicionário de frequência por ciclo vazio."); return
    cycles_to_display_map: Dict[str, int] = {}; total_cycles = len(cycles_df); n = num_cycles_each_end
    if total_cycles > 0: # Primeiros N
        for i in range(min(n, total_cycles)): row = cycles_df.iloc[i]; cn=int(row['numero_ciclo']); cycles_to_display_map[f"Ciclo {cn} ({int(row['duracao'])}c) [{int(row['concurso_inicio'])}-{int(row['concurso_fim'])}]"] = cn
    if total_cycles > n: # Ultimos N
        start_idx = max(n, total_cycles - n);
        for i in range(total_cycles - 1, start_idx - 1, -1): row = cycles_df.iloc[i]; cn=int(row['numero_ciclo']); name = f"Ciclo {cn} ({int(row['duracao'])}c) [{int(row['concurso_inicio'])}-{int(row['concurso_fim'])}]"; cycles_to_display_map.setdefault(name, cn)
    try: # Extremos
        sc = cycles_df.loc[cycles_df['duracao'].idxmin()]; sn=int(sc['numero_ciclo']); ns=f"Ciclo Curto({int(sc['duracao'])}c-nº{sn}) [{int(sc['concurso_inicio'])}-{int(sc['concurso_fim'])}]"; cycles_to_display_map.setdefault(ns, sn)
        lc = cycles_df.loc[cycles_df['duracao'].idxmax()]; ln=int(lc['numero_ciclo']); nl=f"Ciclo Longo({int(lc['duracao'])}c-nº{ln}) [{int(lc['concurso_inicio'])}-{int(lc['concurso_fim'])}]"; cycles_to_display_map.setdefault(nl, ln)
    except Exception as e: logger.warning(f"Não determinar ciclos extremos: {e}")
    print("\n--- Análise de Frequência Dentro de Ciclos Selecionados ---")
    display_items = sorted([(num, nome) for nome, num in cycles_to_display_map.items()])
    if not display_items: logger.warning("Nenhum ciclo selecionado para exibição."); return
    for cycle_num, cycle_name in display_items:
        freq_series = cycle_freq_dict.get(cycle_num)
        if freq_series is not None and not freq_series.empty:
            print(f"\n>> Frequência no {cycle_name} <<")
            try:
                print("Top 5 +:")
                print(freq_series.nlargest(min(5, len(freq_series))).to_string())
                print("\nTop 5 -:")
                print(freq_series.nsmallest(min(5, len(freq_series))).to_string())
                print("-" * 30)
            except Exception as e:
                 logger.error(f"Erro ao obter top/bottom 5 para ciclo {cycle_num}: {e}")
        else:
            logger.warning(f"Frequência não encontrada ou vazia para ciclo {cycle_num} ({cycle_name})")
    logger.info("Análise de frequência por ciclo concluída (resumo impresso).")