# src/analysis/repetition_analysis.py
import pandas as pd
from typing import List, Dict, Any, Set 
import logging

logger = logging.getLogger(__name__)

def calculate_previous_draw_repetitions(all_data_df: pd.DataFrame, config: Any) -> pd.DataFrame: # Recebe config
    logger.info("Iniciando análise de repetição de dezenas do concurso anterior.")
    
    contest_col = config.CONTEST_ID_COLUMN_NAME
    date_col = config.DATE_COLUMN_NAME
    ball_cols = config.BALL_NUMBER_COLUMNS

    # Define colunas default para o DataFrame de resultado
    default_cols_result = [contest_col, 'QtdDezenasRepetidas', 'DezenasRepetidas']
    if date_col in all_data_df.columns: # Adiciona coluna de data se existir no input
        default_cols_result.insert(1, date_col)

    if all_data_df is None or all_data_df.empty or len(all_data_df) < 2:
        logger.warning("DataFrame de entrada insuficiente para análise de repetição (necessário >= 2 concursos).")
        return pd.DataFrame(columns=default_cols_result)

    required_cols = [contest_col] + ball_cols
    missing_cols = [col for col in required_cols if col not in all_data_df.columns]
    if missing_cols:
        logger.error(f"Colunas essenciais ausentes: {missing_cols} (Esperado: {required_cols}). Colunas disponíveis: {all_data_df.columns.tolist()}")
        return pd.DataFrame(columns=default_cols_result)

    df_sorted = all_data_df.copy()
    try:
        df_sorted[contest_col] = pd.to_numeric(df_sorted[contest_col])
    except Exception as e_conv:
        logger.error(f"Não foi possível converter '{contest_col}' para numérico: {e_conv}")
        return pd.DataFrame(columns=default_cols_result)
    df_sorted = df_sorted.sort_values(by=contest_col).reset_index(drop=True)
    
    repetition_data: List[Dict[str, Any]] = []

    for i in range(1, len(df_sorted)):
        current_row = df_sorted.iloc[i]
        previous_row = df_sorted.iloc[i-1]
        current_contest_id = int(current_row[contest_col])

        try:
            current_draw_numbers = set(int(num) for col_name in ball_cols if pd.notna(current_row[col_name]) for num in [current_row[col_name]])
            previous_draw_numbers = set(int(num) for col_name in ball_cols if pd.notna(previous_row[col_name]) for num in [previous_row[col_name]])

            repeated_numbers = current_draw_numbers.intersection(previous_draw_numbers)
            repeated_count = len(repeated_numbers)
            repeated_numbers_str = ",".join(map(str, sorted(list(repeated_numbers)))) if repeated_numbers else None
            
            data_entry: Dict[str, Any] = {
                contest_col: current_contest_id,
                'QtdDezenasRepetidas': repeated_count,
                'DezenasRepetidas': repeated_numbers_str
            }
            if date_col in current_row and pd.notna(current_row[date_col]):
                data_entry[date_col] = current_row[date_col]
            repetition_data.append(data_entry)
        except Exception as e:
            logger.error(f"Erro ao processar repetição para o concurso {current_contest_id}: {e}", exc_info=True)
            error_entry: Dict[str,Any] = {contest_col: current_contest_id, 'QtdDezenasRepetidas': 0, 'DezenasRepetidas': None}
            if date_col in current_row and pd.notna(current_row[date_col]): error_entry[date_col] = current_row[date_col]
            repetition_data.append(error_entry)

    if not repetition_data:
        logger.warning("Nenhum dado de repetição foi gerado.")
        return pd.DataFrame(columns=default_cols_result)
        
    df_repetitions = pd.DataFrame(repetition_data)
    
    # Reordenar e renomear colunas para o padrão da tabela final
    final_ordered_cols = ["Concurso"]
    if "Data" in default_cols_result or date_col in df_repetitions.columns : # Se a coluna de data deve estar no resultado
        final_ordered_cols.append("Data")
    final_ordered_cols.extend(['QtdDezenasRepetidas', 'DezenasRepetidas'])
    
    # Renomeia as colunas do config para os nomes fixos da tabela
    rename_map_final = {
        contest_col: "Concurso",
        date_col: "Data" # Se date_col foi usado e é diferente de "Data"
    }
    df_repetitions.rename(columns=rename_map_final, inplace=True, errors='ignore')
    
    # Garante que todas as colunas de final_ordered_cols existam e estejam na ordem correta
    existing_final_cols = [col for col in final_ordered_cols if col in df_repetitions.columns]
    df_repetitions = df_repetitions[existing_final_cols]
    
    logger.info(f"Análise de repetição concluída. {len(df_repetitions)} registros gerados.")
    return df_repetitions