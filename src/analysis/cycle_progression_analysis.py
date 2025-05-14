# src/analysis/cycle_progression_analysis.py
import pandas as pd
from typing import List, Dict, Set, Any, Optional
import logging
import numpy as np

# from src.config import ALL_NUMBERS # Será de config.ALL_NUMBERS
# Também precisará de CONTEST_ID_COLUMN_NAME, DATE_COLUMN_NAME, BALL_NUMBER_COLUMNS

logger = logging.getLogger(__name__)

def calculate_cycle_progression(all_data_df: pd.DataFrame, config: Any) -> Optional[pd.DataFrame]: # Recebe config
    logger.info("Iniciando cálculo da progressão dos ciclos concurso a concurso.")
    if all_data_df is None or all_data_df.empty:
        logger.warning("DataFrame de entrada para calculate_cycle_progression está vazio.")
        return None

    ALL_NUMBERS_SET: Set[int] = set(config.ALL_NUMBERS) # Usa config

    # Usa nomes de colunas do config
    required_cols = [config.CONTEST_ID_COLUMN_NAME, config.DATE_COLUMN_NAME] + config.BALL_NUMBER_COLUMNS
    missing_cols = [col for col in required_cols if col not in all_data_df.columns]
    if missing_cols:
        logger.error(f"Colunas essenciais ausentes no DataFrame: {missing_cols} (Esperado: {required_cols}). Não é possível calcular progressão de ciclo.")
        logger.debug(f"Colunas disponíveis: {all_data_df.columns.tolist()}")
        return None

    df_sorted = all_data_df.sort_values(by=config.CONTEST_ID_COLUMN_NAME).reset_index(drop=True)
    dezena_cols = config.BALL_NUMBER_COLUMNS # Usa config

    progression_data: List[Dict[str, Any]] = []
    current_cycle_numbers_to_find = ALL_NUMBERS_SET.copy()
    current_cycle_num = 1
    
    logger.debug(f"Progressão de ciclo: Iniciando loop por {len(df_sorted)} concursos.")
    for index, row in df_sorted.iterrows():
        contest_number = int(row[config.CONTEST_ID_COLUMN_NAME]) # Usa config
        contest_date = row[config.DATE_COLUMN_NAME] # Usa config
        
        numbers_needed_before_this_draw = current_cycle_numbers_to_find.copy()
        qty_needed_before_this_draw = len(numbers_needed_before_this_draw)

        try:
            drawn_numbers_in_this_contest = set()
            for col in dezena_cols:
                if pd.notna(row[col]):
                    drawn_numbers_in_this_contest.add(int(row[col]))
        except ValueError:
            logger.warning(f"Erro ao converter dezenas para int no concurso {contest_number}. Pulando registro deste concurso.")
            continue
        
        dezenas_sorteadas_str = ",".join(map(str, sorted(list(drawn_numbers_in_this_contest))))
        hit_this_contest = numbers_needed_before_this_draw.intersection(drawn_numbers_in_this_contest)
        hit_this_contest_str = ",".join(map(str, sorted(list(hit_this_contest)))) if hit_this_contest else None
        qty_hit_this_contest = len(hit_this_contest)
        current_cycle_numbers_to_find.difference_update(drawn_numbers_in_this_contest)
        
        numbers_needed_after_this_draw_str = None
        qty_needed_after_this_draw = 0
        cycle_closed_this_contest = False

        if not current_cycle_numbers_to_find:
            cycle_closed_this_contest = True
            numbers_needed_after_this_draw_str = None 
            qty_needed_after_this_draw = 0
        else:
            numbers_needed_after_this_draw_str = ",".join(map(str, sorted(list(current_cycle_numbers_to_find))))
            qty_needed_after_this_draw = len(current_cycle_numbers_to_find)

        progression_data.append({
            config.CONTEST_ID_COLUMN_NAME: contest_number, # Usa config
            config.DATE_COLUMN_NAME: contest_date, # Usa config
            'ciclo_num_associado': current_cycle_num,
            'dezenas_sorteadas_neste_concurso': dezenas_sorteadas_str,
            'numeros_que_faltavam_antes_deste_concurso': ",".join(map(str, sorted(list(numbers_needed_before_this_draw)))) if numbers_needed_before_this_draw else None,
            'qtd_faltavam_antes_deste_concurso': qty_needed_before_this_draw,
            'dezenas_apuradas_neste_concurso': hit_this_contest_str,
            'qtd_apuradas_neste_concurso': qty_hit_this_contest,
            'numeros_faltantes_apos_este_concurso': numbers_needed_after_this_draw_str,
            'qtd_faltantes_apos_este_concurso': qty_needed_after_this_draw,
            'ciclo_fechou_neste_concurso': 1 if cycle_closed_this_contest else 0
        })
        
        if cycle_closed_this_contest:
            logger.debug(f"Ciclo {current_cycle_num} FECHOU no concurso {contest_number}.")
            current_cycle_num += 1
            current_cycle_numbers_to_find = ALL_NUMBERS_SET.copy()

    if not progression_data:
        logger.warning("Nenhum dado de progressão de ciclo foi gerado.")
        return None
        
    df_progression = pd.DataFrame(progression_data)
    # Renomear colunas para nomes padronizados se eles foram usados como chaves de dicionário
    df_progression.rename(columns={
        config.CONTEST_ID_COLUMN_NAME: "Concurso", # Se a tabela final espera "Concurso"
        config.DATE_COLUMN_NAME: "Data"           # Se a tabela final espera "Data"
    }, inplace=True)

    logger.info(f"Cálculo da progressão de ciclo concluído. {len(df_progression)} registros gerados.")
    return df_progression