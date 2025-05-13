# src/analysis/block_aggregator.py
import pandas as pd
import logging
from typing import Dict, List, Any, Optional
import numpy as np

from src.database_manager import DatabaseManager
from src.config import CHUNK_TYPES_CONFIG, ALL_NUMBERS

logger = logging.getLogger(__name__)

def aggregate_block_data_to_wide_format(db_manager: DatabaseManager):
    # ... (código da função como na versão anterior e validada) ...
    logger.info("Iniciando agregação de dados de bloco para formato largo.")
    metric_configs = [
        {"source_table_prefix": "evol_metric_frequency", "value_column": "frequencia_absoluta", "analysis_type_name": "frequencia_bloco", "dtype": "Int64"},
        {"source_table_prefix": "evol_rank_frequency_bloco", "value_column": "rank_no_bloco", "analysis_type_name": "rank_freq_bloco", "dtype": "Int64"},
        {"source_table_prefix": "evol_metric_atraso_medio_bloco", "value_column": "atraso_medio_no_bloco", "analysis_type_name": "atraso_medio_bloco", "dtype": "float"},
        {"source_table_prefix": "evol_metric_atraso_maximo_bloco", "value_column": "atraso_maximo_no_bloco", "analysis_type_name": "atraso_maximo_bloco", "dtype": "Int64"},
        {"source_table_prefix": "evol_metric_atraso_final_bloco", "value_column": "atraso_final_no_bloco", "analysis_type_name": "atraso_final_bloco", "dtype": "Int64"},
    ]
    for chunk_type, config in CHUNK_TYPES_CONFIG.items():
        chunk_sizes = config.get('sizes', [])
        for size in chunk_sizes:
            consolidated_table_name = f"bloco_analises_consolidadas_{chunk_type}_{size}"
            logger.info(f"Processando para tabela consolidada de BLOCKS: '{consolidated_table_name}'")
            all_wide_dfs_for_this_chunk_config: List[pd.DataFrame] = []
            for metric_config in metric_configs:
                source_table_prefix = metric_config["source_table_prefix"]
                value_column_in_long_table = metric_config["value_column"]
                analysis_type_name_for_wide_table = metric_config["analysis_type_name"]
                expected_dtype_str = metric_config["dtype"]
                long_format_table_name = f"{source_table_prefix}_{chunk_type}_{size}"
                if not db_manager.table_exists(long_format_table_name): logger.warning(f"Tabela '{long_format_table_name}' não encontrada. Pulando."); continue
                df_long = db_manager.load_dataframe_from_db(long_format_table_name)
                if df_long is None or df_long.empty: logger.warning(f"DataFrame de '{long_format_table_name}' vazio. Pulando."); continue
                index_cols = ['chunk_seq_id', 'chunk_start_contest', 'chunk_end_contest']; columns_col = 'dezena'
                required_for_pivot = index_cols + [columns_col, value_column_in_long_table]
                if not all(col in df_long.columns for col in required_for_pivot): logger.error(f"Tabela '{long_format_table_name}' não tem colunas para pivotar. Pulando."); continue
                try:
                    if expected_dtype_str == "Int64": df_long[value_column_in_long_table] = pd.to_numeric(df_long[value_column_in_long_table], errors='coerce')
                    elif expected_dtype_str == "float": df_long[value_column_in_long_table] = pd.to_numeric(df_long[value_column_in_long_table], errors='coerce')
                    df_wide_metric = df_long.pivot_table(index=index_cols, columns=columns_col, values=value_column_in_long_table, fill_value=pd.NA if expected_dtype_str == "Int64" else np.nan)
                    df_wide_metric.columns = [f'dezena_{int(col)}' for col in df_wide_metric.columns]; df_wide_metric.reset_index(inplace=True)
                    df_wide_metric['tipo_analise'] = analysis_type_name_for_wide_table
                    final_cols_order = index_cols + ['tipo_analise'] + [f'dezena_{i}' for i in range(1, 26)]
                    for i in range(1, 26):
                        col_name_to_check = f'dezena_{i}'
                        if col_name_to_check not in df_wide_metric.columns: df_wide_metric[col_name_to_check] = pd.NA if expected_dtype_str == "Int64" else np.nan
                        if expected_dtype_str == "Int64": df_wide_metric[col_name_to_check] = df_wide_metric[col_name_to_check].astype('Int64')
                        elif expected_dtype_str == "float": df_wide_metric[col_name_to_check] = df_wide_metric[col_name_to_check].astype('float')
                    all_wide_dfs_for_this_chunk_config.append(df_wide_metric[final_cols_order])
                except Exception as e: logger.error(f"Erro ao pivotar '{long_format_table_name}' para '{analysis_type_name_for_wide_table}': {e}", exc_info=True)
            if all_wide_dfs_for_this_chunk_config:
                df_consolidated_wide = pd.concat(all_wide_dfs_for_this_chunk_config, ignore_index=True)
                try:
                    db_manager.save_dataframe_to_db(df_consolidated_wide, consolidated_table_name, if_exists='replace')
                    logger.info(f"Tabela consolidada de BLOCKS '{consolidated_table_name}' salva com {len(df_consolidated_wide)} linhas.")
                except Exception as e: logger.error(f"Erro ao salvar tabela consolidada de BLOCKS '{consolidated_table_name}': {e}", exc_info=True)
            else: logger.warning(f"Nenhum DataFrame largo gerado para BLOCKS '{consolidated_table_name}'.")
    logger.info("Agregação de dados de bloco para formato largo concluída.")


def aggregate_cycle_data_to_wide_format(db_manager: DatabaseManager):
    logger.info("Iniciando agregação de dados de CICLO para formato largo (incluindo rank).")

    cycle_metric_configs = [
        {"source_table": "ciclo_metric_frequency", "value_column": "frequencia_no_ciclo", "analysis_type_name": "frequencia_no_ciclo", "dtype": "Int64"},
        {"source_table": "ciclo_metric_atraso_medio", "value_column": "atraso_medio_no_ciclo", "analysis_type_name": "atraso_medio_no_ciclo", "dtype": "float"},
        {"source_table": "ciclo_metric_atraso_maximo", "value_column": "atraso_maximo_no_ciclo", "analysis_type_name": "atraso_maximo_no_ciclo", "dtype": "Int64"},
        {"source_table": "ciclo_metric_atraso_final", "value_column": "atraso_final_no_ciclo", "analysis_type_name": "atraso_final_no_ciclo", "dtype": "Int64"},
        # <<< NOVA MÉTRICA DE RANK DE CICLO ADICIONADA AQUI >>>
        {"source_table": "ciclo_rank_frequency", "value_column": "rank_freq_no_ciclo", "analysis_type_name": "rank_freq_no_ciclo", "dtype": "Int64"},
    ]

    consolidated_cycle_table_name = "ciclo_analises_consolidadas"
    all_wide_dfs_for_cycles: List[pd.DataFrame] = []

    df_ciclos_detalhe = db_manager.load_dataframe_from_db("ciclos_detalhe")
    if df_ciclos_detalhe is None or df_ciclos_detalhe.empty:
        logger.warning("Tabela 'ciclos_detalhe' não encontrada ou vazia. Não é possível criar tabela consolidada de ciclos.")
        return
    
    df_ciclos_base_info = df_ciclos_detalhe[['ciclo_num', 'concurso_inicio', 'concurso_fim', 'duracao_concursos']].copy()
    df_ciclos_base_info.rename(columns={
        'concurso_inicio': 'concurso_inicio_ciclo', 'concurso_fim': 'concurso_fim_ciclo', 'duracao_concursos': 'duracao_ciclo'
    }, inplace=True)

    for metric_config in cycle_metric_configs:
        long_format_table_name = metric_config["source_table"]
        value_column_in_long_table = metric_config["value_column"]
        analysis_type_name_for_wide_table = metric_config["analysis_type_name"]
        expected_dtype_str = metric_config["dtype"]

        if not db_manager.table_exists(long_format_table_name):
            logger.warning(f"Tabela '{long_format_table_name}' não encontrada. Pulando '{analysis_type_name_for_wide_table}'.")
            continue
        
        df_long_cycle_metric = db_manager.load_dataframe_from_db(long_format_table_name)
        if df_long_cycle_metric is None or df_long_cycle_metric.empty:
            logger.warning(f"DataFrame de '{long_format_table_name}' vazio. Pulando '{analysis_type_name_for_wide_table}'.")
            continue

        index_cols_cycle = ['ciclo_num'] 
        columns_col_cycle = 'dezena'
        
        # Adiciona a coluna de frequência se ela não existir na tabela de rank, mas for necessária para referência.
        # A tabela ciclo_rank_frequency já inclui 'frequencia_no_ciclo'
        required_cols_for_pivot = index_cols_cycle + [columns_col_cycle, value_column_in_long_table]
        if not all(col in df_long_cycle_metric.columns for col in required_cols_for_pivot) :
             logger.error(f"Tabela '{long_format_table_name}' não tem colunas para pivotar. Faltando: {set(required_cols_for_pivot) - set(df_long_cycle_metric.columns)}. Pulando.")
             continue
                
        logger.debug(f"Pivotando '{long_format_table_name}' para métrica de ciclo '{analysis_type_name_for_wide_table}'.")
        try:
            if expected_dtype_str == "Int64": df_long_cycle_metric[value_column_in_long_table] = pd.to_numeric(df_long_cycle_metric[value_column_in_long_table], errors='coerce')
            elif expected_dtype_str == "float": df_long_cycle_metric[value_column_in_long_table] = pd.to_numeric(df_long_cycle_metric[value_column_in_long_table], errors='coerce')
            
            df_wide_metric = df_long_cycle_metric.pivot_table(
                index=index_cols_cycle,
                columns=columns_col_cycle,
                values=value_column_in_long_table,
                fill_value=pd.NA if expected_dtype_str == "Int64" else np.nan
            )
            df_wide_metric.columns = [f'dezena_{int(col)}' for col in df_wide_metric.columns]
            df_wide_metric.reset_index(inplace=True)
            df_wide_metric['tipo_analise_ciclo'] = analysis_type_name_for_wide_table
            
            df_wide_metric = pd.merge(df_ciclos_base_info, df_wide_metric, on='ciclo_num', how='inner')
            final_cols_order = ['ciclo_num', 'concurso_inicio_ciclo', 'concurso_fim_ciclo', 'duracao_ciclo', 
                                'tipo_analise_ciclo'] + [f'dezena_{i}' for i in range(1, 26)]
            for i in range(1, 26):
                col_name_to_check = f'dezena_{i}'
                if col_name_to_check not in df_wide_metric.columns:
                    df_wide_metric[col_name_to_check] = pd.NA if expected_dtype_str == "Int64" else np.nan
                if expected_dtype_str == "Int64": df_wide_metric[col_name_to_check] = df_wide_metric[col_name_to_check].astype('Int64')
                elif expected_dtype_str == "float": df_wide_metric[col_name_to_check] = df_wide_metric[col_name_to_check].astype('float')
            
            all_wide_dfs_for_cycles.append(df_wide_metric[final_cols_order])
        except Exception as e:
            logger.error(f"Erro ao pivotar '{long_format_table_name}' para '{analysis_type_name_for_wide_table}': {e}", exc_info=True)

    if all_wide_dfs_for_cycles:
        df_consolidated_cycles_wide = pd.concat(all_wide_dfs_for_cycles, ignore_index=True)
        try:
            db_manager.save_dataframe_to_db(df_consolidated_cycles_wide, consolidated_cycle_table_name, if_exists='replace')
            logger.info(f"Tabela consolidada de CICLOS '{consolidated_cycle_table_name}' salva com {len(df_consolidated_cycles_wide)} linhas.")
        except Exception as e:
            logger.error(f"Erro ao salvar tabela consolidada de CICLOS '{consolidated_cycle_table_name}': {e}", exc_info=True)
    else:
        logger.warning(f"Nenhum DataFrame largo gerado para CICLOS. Tabela '{consolidated_cycle_table_name}' não foi criada.")
    logger.info("Agregação de dados de CICLO para formato largo concluída.")