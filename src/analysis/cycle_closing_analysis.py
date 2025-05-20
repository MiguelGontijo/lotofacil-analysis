# Lotofacil_Analysis/src/analysis/cycle_closing_analysis.py
import pandas as pd
from collections import Counter
import numpy as np
import logging
from typing import List, Dict, Optional, Tuple, Set, Any

logger = logging.getLogger(__name__)

def _get_draw_numbers_from_row(draw_row: pd.Series, ball_columns: List[str]) -> Set[int]:
    drawn_numbers = set()
    try:
        # Garante que está tentando converter apenas valores que parecem números
        valid_numbers = [int(n) for n in draw_row[ball_columns].dropna().values if str(n).replace('.0','',1).isdigit()]
        drawn_numbers.update(valid_numbers)
    except ValueError:
        logger.warning(f"Valor não numérico encontrado nas dezenas ao processar linha: {draw_row.to_dict()}")
    return drawn_numbers

def get_cycles_df_corrected( # Esta função parece carregar do DB e pode não ser usada se cycles_df vem do shared_context
    db_manager: Any,
    config: Any,
    concurso_maximo: Optional[int] = None
) -> pd.DataFrame:
    cycles_table_name = config.ANALYSIS_CYCLES_DETAIL_TABLE_NAME
    logger.info(f"Buscando dados da tabela de detalhes de ciclos: '{cycles_table_name}'")

    if not db_manager.table_exists(cycles_table_name):
        logger.warning(f"Tabela '{cycles_table_name}' não existe. Retornando DataFrame vazio.")
        return pd.DataFrame()

    ciclo_num_col_in_db = getattr(config, 'CICLO_NUM_COLUMN_NAME', 'ciclo_num')
    # Certifique-se que as colunas na query SELECT correspondem às colunas reais na tabela
    # E que a renomeação para 'duracao' esteja correta se a coluna original é 'duracao_concursos'
    sql_query = f"""
        SELECT 
            {ciclo_num_col_in_db} AS ciclo_num, 
            concurso_inicio, 
            concurso_fim, 
            duracao_concursos -- Mantém o nome original da tabela
        FROM {cycles_table_name}
    """
    params: List[Any] = []
    if concurso_maximo is not None:
        if db_manager.column_exists(cycles_table_name, 'concurso_fim'):
             sql_query += " WHERE concurso_fim <= ?"
             params.append(concurso_maximo)
        else:
            logger.warning(f"Coluna 'concurso_fim' não existe na tabela '{cycles_table_name}' para filtro.")
    sql_query += f" ORDER BY {ciclo_num_col_in_db} ASC;"
    
    df = db_manager.load_dataframe(table_name=cycles_table_name, query=sql_query, params=tuple(params))

    if df.empty:
        logger.info(f"Nenhum ciclo encontrado na '{cycles_table_name}'.")
        return pd.DataFrame()
        
    cols_to_convert = ['ciclo_num', 'concurso_inicio', 'concurso_fim', 'duracao_concursos']
    for col in cols_to_convert:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    logger.info(f"{len(df)} ciclos lidos da tabela '{cycles_table_name}'.")
    return df

def get_draw_numbers_for_contest(
    db_manager: Any,
    config: Any,
    contest_id: int
) -> Optional[Set[int]]:
    main_draws_table = config.MAIN_DRAWS_TABLE_NAME
    ball_cols_str = ", ".join(config.BALL_NUMBER_COLUMNS)
    query = f"SELECT {ball_cols_str} FROM {main_draws_table} WHERE {config.CONTEST_ID_COLUMN_NAME} = ?"
    df_draw = db_manager.execute_query(query, params=(contest_id,))
    
    if df_draw is not None and not df_draw.empty:
        try:
            draw_series = df_draw.iloc[0]
            drawn_numbers = set(int(n) for n in draw_series.dropna().values if pd.notna(n) and str(n).replace('.0','',1).isdigit())
            return drawn_numbers
        except ValueError as ve:
            logger.error(f"Erro ao converter dezenas para int no concurso {contest_id}: {ve}")
            return None
    logger.warning(f"Nenhum dado encontrado para o concurso {contest_id} ao buscar dezenas.")
    return None

def calculate_closing_number_stats(
    db_manager: Any,
    config: Any,
    cycles_df: pd.DataFrame # Este DF vem do shared_context e tem 'ciclo_num' e 'duracao_concursos'
) -> pd.DataFrame:
    logger.info("Calculando estatísticas de frequência de fechamento de ciclo...")

    default_stats_df = pd.DataFrame({
        'closing_freq': 0,
        'sole_closing_freq': 0
    }, index=pd.Index(config.ALL_NUMBERS, name=config.DEZENA_COLUMN_NAME))

    if cycles_df is None or cycles_df.empty:
        logger.warning("DataFrame de ciclos (cycles_df) está vazio ou é None. Retornando stats default.")
        return default_stats_df

    # CORREÇÃO: Usar 'ciclo_num' e 'duracao_concursos' que são as colunas em cycles_df
    required_cols_in_cycles_df = ['ciclo_num', 'concurso_inicio', 'concurso_fim', 'duracao_concursos']
    missing_cols = [col for col in required_cols_in_cycles_df if col not in cycles_df.columns]
    if missing_cols:
        logger.error(f"DataFrame de ciclos (cycles_df) não possui as colunas: {missing_cols}. Presentes: {cycles_df.columns.tolist()}. Retornando stats default.")
        return default_stats_df

    closed_cycles_df = cycles_df[
        pd.to_numeric(cycles_df['concurso_fim'], errors='coerce').notna() &
        pd.to_numeric(cycles_df['duracao_concursos'], errors='coerce').notna() &
        (pd.to_numeric(cycles_df['duracao_concursos'], errors='coerce') > 0)
    ].copy()

    if closed_cycles_df.empty:
        logger.warning("Nenhum ciclo fechado válido para análise de fechamento. Retornando stats default.")
        return default_stats_df

    df_len = len(closed_cycles_df)
    logger.info(f"Analisando {df_len} ciclos fechados para estatísticas de fechamento...")
    
    closing_counter = Counter()
    sole_closing_counter = Counter()
    processed_valid_cycles = 0
    
    main_draws_table = config.MAIN_DRAWS_TABLE_NAME
    ball_cols_for_query_str = ", ".join(config.BALL_NUMBER_COLUMNS)

    for index, cycle_row in closed_cycles_df.iterrows():
        try:
            cycle_num = int(cycle_row['ciclo_num']) # Usando 'ciclo_num'
            start_c = int(cycle_row['concurso_inicio'])
            end_c = int(cycle_row['concurso_fim'])
        except Exception as e:
            logger.error(f"Erro ao processar dados da linha do ciclo {index} (num: {cycle_row.get('ciclo_num', 'N/A')}): {e}")
            continue
            
        concurso_fim_menos_1 = end_c - 1
        seen_in_cycle_before_closing_draw: Set[int] = set()

        if start_c <= concurso_fim_menos_1:
            query_seen_before = f"""
                SELECT {ball_cols_for_query_str} 
                FROM {main_draws_table} 
                WHERE {config.CONTEST_ID_COLUMN_NAME} BETWEEN ? AND ?
            """
            df_seen_before = db_manager.execute_query(query_seen_before, params=(start_c, concurso_fim_menos_1))
            
            if df_seen_before is not None and not df_seen_before.empty:
                for _, draw_row_seen in df_seen_before.iterrows():
                    seen_in_cycle_before_closing_draw.update(_get_draw_numbers_from_row(draw_row_seen, config.BALL_NUMBER_COLUMNS))
        
        drawn_at_closing_contest = get_draw_numbers_for_contest(db_manager, config, end_c)
        if drawn_at_closing_contest is None:
            logger.warning(f"Não obter dezenas para concurso de fechamento {end_c} do ciclo {cycle_num}. Pulando.")
            continue
            
        missing_for_cycle_closure = set(config.ALL_NUMBERS) - seen_in_cycle_before_closing_draw
        actual_closing_numbers = drawn_at_closing_contest.intersection(missing_for_cycle_closure)
        
        if not actual_closing_numbers:
            logger.warning(
                f"Ciclo {cycle_num} (final {end_c}): Nenhuma dezena de fechamento. "
                f"Faltavam: {sorted(list(missing_for_cycle_closure)) if missing_for_cycle_closure else 'Nenhuma'}. "
                f"Sorteadas em {end_c}: {sorted(list(drawn_at_closing_contest))}."
            )
            if not missing_for_cycle_closure:
                 logger.debug(f"Ciclo {cycle_num}, todas dezenas vistas antes do concurso {end_c}.")
            continue 
        
        closing_counter.update(actual_closing_numbers)
        if len(actual_closing_numbers) == 1:
            sole_closing_counter.update(actual_closing_numbers) 
        
        processed_valid_cycles += 1
        if processed_valid_cycles > 0 and processed_valid_cycles % 20 == 0:
            logger.info(f"{processed_valid_cycles}/{df_len} ciclos processados para análise de fechamento...")

    stats_df_final = pd.DataFrame(index=pd.Index(config.ALL_NUMBERS, name=config.DEZENA_COLUMN_NAME))
    stats_df_final['closing_freq'] = stats_df_final.index.map(closing_counter).fillna(0).astype(int)
    stats_df_final['sole_closing_freq'] = stats_df_final.index.map(sole_closing_counter).fillna(0).astype(int)
    
    logger.info(f"Cálculo de stats de fechamento concluído. {processed_valid_cycles} ciclos válidos analisados.")
    return stats_df_final