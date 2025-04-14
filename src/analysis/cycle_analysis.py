# src/analysis/cycle_analysis.py

import pandas as pd
import sqlite3
from typing import List, Dict, Optional, Tuple, Set

# Importa funções do DB Manager
from src.database_manager import read_data_from_db, create_cycles_table, get_last_cycle_end, save_to_db
# Importa do config (SEM ALL_NUMBERS_SET)
from src.config import logger, NEW_BALL_COLUMNS, DATABASE_PATH, TABLE_NAME
# Importa a função de cálculo de frequência
from src.analysis.frequency_analysis import calculate_frequency

# Define ALL_NUMBERS_SET localmente
ALL_NUMBERS_SET: Set[int] = set(range(1, 26))
BASE_COLS: List[str] = ['concurso'] + NEW_BALL_COLUMNS


# --- FUNÇÃO INTERNA PARA ENCONTRAR CICLOS EM UM DF ---
def _find_new_cycles_in_data(df_new_data: pd.DataFrame,
                             start_contest_for_search: int,
                             initial_seen_numbers: Set[int],
                             last_cycle_number: int) -> List[Dict]:
    """ Lógica central para encontrar ciclos completos DENTRO de um DataFrame. """
    new_cycles_found: List[Dict] = []
    current_cycle_numbers = initial_seen_numbers.copy()
    cycle_start_concurso = start_contest_for_search
    cycle_count = last_cycle_number

    logger.info(f"Buscando novos ciclos a partir do concurso {cycle_start_concurso}...")
    for index, row in df_new_data.iterrows():
        current_concurso_val = row['concurso']
        if pd.isna(current_concurso_val): continue
        current_concurso: int = int(current_concurso_val)
        if current_concurso < start_contest_for_search: continue

        drawn_numbers: Set[int] = set(int(num) for num in row[NEW_BALL_COLUMNS].dropna().values)
        current_cycle_numbers.update(drawn_numbers)

        # Usa ALL_NUMBERS_SET local
        if current_cycle_numbers == ALL_NUMBERS_SET:
            cycle_count += 1
            cycle_end_concurso: int = current_concurso
            duration: int = cycle_end_concurso - cycle_start_concurso + 1
            new_cycles_found.append({
                'numero_ciclo': cycle_count, 'concurso_inicio': cycle_start_concurso,
                'concurso_fim': cycle_end_concurso, 'duracao': duration
            })
            logger.debug(f"Novo ciclo completo encontrado: {cycle_count} (Fim: {cycle_end_concurso})")
            cycle_start_concurso = cycle_end_concurso + 1
            current_cycle_numbers = set()

    logger.info(f"{len(new_cycles_found)} novos ciclos completos encontrados.")
    return new_cycles_found


# --- FUNÇÃO PARA ATUALIZAR TABELA PERSISTENTE ---
def update_cycles_table(force_rebuild: bool = False):
    """ Identifica e salva/atualiza ciclos na tabela 'ciclos', criando índices. """
    logger.info(f"Atualizando tabela 'ciclos'... {'(Rebuild)' if force_rebuild else ''}")
    create_cycles_table() # Garante que a tabela exista

    last_recorded_end = -1; last_cycle_num = 0; start_search_from = 1
    db_path = DATABASE_PATH; cycles_table_name = 'ciclos'

    if force_rebuild:
        logger.warning("REBUILD: Apagando dados de 'ciclos'.")
        try:
            with sqlite3.connect(db_path) as conn: conn.execute(f"DELETE FROM {cycles_table_name};")
        except sqlite3.Error as e: logger.error(f"Erro ao limpar 'ciclos': {e}"); return
    else:
        last_recorded_end_val = get_last_cycle_end()
        if last_recorded_end_val is not None:
             last_recorded_end = last_recorded_end_val; start_search_from = last_recorded_end + 1
             try:
                 with sqlite3.connect(db_path) as conn:
                     res = conn.execute("SELECT MAX(numero_ciclo) FROM ciclos").fetchone()
                     if res and res[0] is not None: last_cycle_num = int(res[0])
             except sqlite3.Error as e: logger.error(f"Erro buscar MAX(numero_ciclo): {e}")
             logger.info(f"Último ciclo termina em {last_recorded_end}. Buscando a partir de {start_search_from}.")
        else: logger.info("Tabela 'ciclos' vazia. Buscando do início.")

    # Lógica para determinar de onde ler (igual anterior)
    read_from = start_search_from; initial_numbers = set(); effective_last_cycle_num = 0
    if not force_rebuild and last_recorded_end > 0:
         try:
             with sqlite3.connect(db_path) as conn: last_cycle_info = pd.read_sql(f"SELECT concurso_inicio FROM ciclos WHERE concurso_fim = ?", conn, params=(last_recorded_end,))
             if not last_cycle_info.empty:
                  read_from = int(last_cycle_info.iloc[0]['concurso_inicio']); logger.info(f"Recalculando estado a partir de {read_from}")
                  with sqlite3.connect(db_path) as conn: res = conn.execute("SELECT MAX(numero_ciclo) FROM ciclos WHERE concurso_fim < ?", (read_from,)).fetchone(); effective_last_cycle_num = int(res[0]) if res and res[0] is not None else 0
             else: logger.warning(f"Info do último ciclo {last_recorded_end} não encontrada."); read_from = start_search_from
         except Exception as e: logger.error(f"Erro ao ler último ciclo: {e}."); read_from = start_search_from

    df_data_to_scan = read_data_from_db(columns=BASE_COLS, concurso_minimo=read_from)
    if df_data_to_scan is None or df_data_to_scan.empty: logger.info(f"Nenhum dado novo em 'sorteios' a partir de {read_from}."); return

    all_found_cycles = _find_new_cycles_in_data(df_data_to_scan, read_from, initial_numbers, effective_last_cycle_num)
    new_cycles = [c for c in all_found_cycles if c['concurso_fim'] > last_recorded_end]

    if new_cycles:
        new_cycles_df = pd.DataFrame(new_cycles)
        write_mode = 'replace' if force_rebuild else 'append'
        # Deleta ciclos recalculados antes do append
        if write_mode == 'append' and last_recorded_end > 0:
             min_recalculated_start = new_cycles_df['concurso_inicio'].min()
             try:
                  with sqlite3.connect(db_path) as conn:
                       conn.execute(f"DELETE FROM {cycles_table_name} WHERE concurso_inicio >= ?", (min_recalculated_start,))
                       logger.info(f"Ciclos antigos a partir de {min_recalculated_start} removidos antes do append.")
             except sqlite3.Error as e: logger.error(f"Erro ao deletar ciclos antigos p/ append: {e}")

        # Salva os dados SEM índice automático
        success = save_to_db(new_cycles_df, table_name=cycles_table_name, if_exists=write_mode)
        if success:
            logger.info(f"{len(new_cycles)} ciclos {'adicionados/atualizados' if write_mode=='append' else 'reconstruídos'}.")
            # CRIA OS ÍNDICES CORRETOS APÓS SALVAR
            try:
                with sqlite3.connect(db_path) as conn:
                    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_ciclos_numero ON {cycles_table_name} (numero_ciclo);")
                    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_ciclos_concurso_fim ON {cycles_table_name} (concurso_fim);")
                    logger.info(f"Índices para '{cycles_table_name}' criados/verificados.")
            except sqlite3.Error as e: logger.error(f"Erro ao criar índices para '{cycles_table_name}': {e}")
        else: logger.error("Falha ao salvar novos ciclos no BD.")
    else: logger.info("Nenhum NOVO ciclo completo encontrado.")


# --- FUNÇÃO PARA LER CICLOS DA TABELA ---
def get_cycles_df(concurso_maximo: Optional[int] = None) -> Optional[pd.DataFrame]:
    """ Lê os dados da tabela 'ciclos' do banco de dados. """
    logger.info(f"Buscando dados da tabela 'ciclos' {('ate ' + str(concurso_maximo)) if concurso_maximo else ''}...")
    create_cycles_table(); db_path = DATABASE_PATH; cycles_table_name = 'ciclos'
    try:
        with sqlite3.connect(db_path) as conn:
            sql_query = f"SELECT numero_ciclo, concurso_inicio, concurso_fim, duracao FROM {cycles_table_name}"; params = []
            if concurso_maximo is not None: sql_query += " WHERE concurso_fim <= ?"; params.append(concurso_maximo)
            sql_query += " ORDER BY numero_ciclo ASC;"
            df = pd.read_sql_query(sql_query, conn, params=params); logger.info(f"{len(df)} ciclos lidos.");
            if not df.empty:
                 for col in df.columns: df[col] = pd.to_numeric(df[col], errors='ignore')
            return df
    except sqlite3.Error as e: logger.error(f"Erro ao ler 'ciclos': {e}"); return None


# *** FUNÇÃO INCLUÍDA NOVAMENTE ***
def calculate_frequency_per_cycle(cycles_df: pd.DataFrame) -> Dict[int, Optional[pd.Series]]:
    """ Calcula a frequência para CADA ciclo completo no DataFrame fornecido. """
    cycle_frequencies: Dict[int, Optional[pd.Series]] = {}
    if cycles_df is None or cycles_df.empty:
        logger.warning("DataFrame de ciclos vazio para calcular frequência por ciclo.")
        return cycle_frequencies

    logger.info(f"Calculando frequência para {len(cycles_df)} ciclos...")
    for index, row in cycles_df.iterrows():
        cycle_num, start_c, end_c = int(row['numero_ciclo']), int(row['concurso_inicio']), int(row['concurso_fim'])
        # Logger reduzido para não poluir tanto em backtests futuros
        # logger.debug(f"Calculando freq. p/ Ciclo {cycle_num} ({start_c}-{end_c})...")
        cycle_frequencies[cycle_num] = calculate_frequency(concurso_minimo=start_c, concurso_maximo=end_c)

    logger.info("Cálculo de frequência por ciclo concluído.")
    return cycle_frequencies


# *** FUNÇÃO INCLUÍDA NOVAMENTE ***
def run_cycle_frequency_analysis(cycles_df: pd.DataFrame, num_cycles_each_end: int = 3):
    """ Calcula e EXIBE a frequência das dezenas para ciclos selecionados. """
    if cycles_df is None or cycles_df.empty:
        logger.warning("DataFrame de ciclos vazio para run_cycle_frequency_analysis.")
        return

    logger.info(f"Calculando/Exibindo frequência para ciclos selecionados...")
    # Calcula frequência para TODOS os ciclos no DF fornecido
    cycle_freq_dict = calculate_frequency_per_cycle(cycles_df)
    if not cycle_freq_dict:
        logger.warning("Dicionário de frequência por ciclo vazio.")
        return

    # Lógica para selecionar ciclos para exibição
    cycles_to_display_map: Dict[str, int] = {}; total_cycles = len(cycles_df); n = num_cycles_each_end
    if total_cycles > 0: # Primeiros N
        for i in range(min(n, total_cycles)): row = cycles_df.iloc[i]; cn=int(row['numero_ciclo']); cycles_to_display_map[f"Ciclo {cn} ({int(row['duracao'])}c) [{int(row['concurso_inicio'])}-{int(row['concurso_fim'])}]"] = cn
    if total_cycles > n: # Ultimos N
        start_idx = max(n, total_cycles - n);
        for i in range(total_cycles - 1, start_idx - 1, -1): row = cycles_df.iloc[i]; cn=int(row['numero_ciclo']); name = f"Ciclo {cn} ({int(row['duracao'])}c) [{int(row['concurso_inicio'])}-{int(row['concurso_fim'])}]"; cycles_to_display_map.setdefault(name, cn)
    try: # Extremos
        sc = cycles_df.loc[cycles_df['duracao'].idxmin()]; sn=int(sc['numero_ciclo']); ns=f"Ciclo Curto({int(sc['duracao'])}c-nº{sn}) [{int(sc['concurso_inicio'])}-{int(sc['concurso_fim'])}]"; cycles_to_display_map.setdefault(ns, sn)
        lc = cycles_df.loc[cycles_df['duracao'].idxmax()]; ln=int(lc['numero_ciclo']); nl=f"Ciclo Longo({int(lc['duracao'])}c-nº{ln}) [{int(lc['concurso_inicio'])}-{int(lc['concurso_fim'])}]"; cycles_to_display_map.setdefault(nl, ln)
    except ValueError: logger.warning("Não foi possível determinar ciclos extremos.")
    except KeyError as e: logger.error(f"Coluna não encontrada ao buscar ciclos extremos: {e}")


    print("\n--- Análise de Frequência Dentro de Ciclos Selecionados ---")
    display_items = sorted([(num, nome) for nome, num in cycles_to_display_map.items()])

    if not display_items:
        logger.warning("Nenhum ciclo selecionado para exibição.")
        return

    for cycle_num, cycle_name in display_items:
        freq_series = cycle_freq_dict.get(cycle_num)
        if freq_series is not None and not freq_series.empty:
            print(f"\n>> Frequência no {cycle_name} <<");
            # Usar try-except para nlargest/nsmallest caso a série seja muito pequena
            try:
                print("Top 5 +:"); print(freq_series.nlargest(min(5, len(freq_series))).to_string())
                print("\nTop 5 -:"); print(freq_series.nsmallest(min(5, len(freq_series))).to_string())
            except Exception as e:
                 logger.error(f"Erro ao obter top/bottom 5 para ciclo {cycle_num}: {e}")
            print("-" * 30)
        else:
            logger.warning(f"Frequência não encontrada ou vazia para ciclo {cycle_num} ({cycle_name})")
    logger.info("Análise de frequência por ciclo concluída (resumo impresso).")


# --- FUNÇÃO PARA CALCULAR STATS DO CICLO ATUAL INCOMPLETO (Usa get_cycles_df) ---
# (Código igual anterior)
def calculate_current_incomplete_cycle_stats(concurso_maximo: int) -> Tuple[Optional[int], Optional[Set[int]], Optional[pd.Series]]:
    logger.info(f"Identificando ciclo incompleto até {concurso_maximo}...")
    relevant_cycles = get_cycles_df(concurso_maximo=concurso_maximo) # <<< Usa a tabela
    start_of_current_cycle: Optional[int] = None
    if relevant_cycles is None : logger.error("Falha ao ler ciclos da tabela."); start_of_current_cycle = None
    elif relevant_cycles.empty: df_min = read_data_from_db(columns=['concurso'], concurso_maximo=concurso_maximo); min_c = df_min['concurso'].min() if df_min is not None else None; start_of_current_cycle = int(min_c) if not pd.isna(min_c) else None; logger.info(f"Nenhum ciclo completo na tabela até {concurso_maximo}.")
    else: last_complete_cycle_end = relevant_cycles['concurso_fim'].max(); start_of_current_cycle = int(last_complete_cycle_end + 1)
    if start_of_current_cycle is None or start_of_current_cycle > concurso_maximo : logger.warning(f"Não há ciclo incompleto válido até {concurso_maximo}."); return None, None, None
    logger.info(f"Ciclo incompleto atual: {start_of_current_cycle} - {concurso_maximo}.")
    current_cycle_freq = calculate_frequency(concurso_minimo=start_of_current_cycle, concurso_maximo=concurso_maximo)
    current_cycle_numbers_drawn: Optional[Set[int]] = None
    if current_cycle_freq is not None: current_cycle_numbers_drawn = set(current_cycle_freq[current_cycle_freq > 0].index); logger.info(f"{len(current_cycle_numbers_drawn)} dezenas sorteadas no ciclo atual.")
    return start_of_current_cycle, current_cycle_numbers_drawn, current_cycle_freq