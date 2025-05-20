# Lotofacil_Analysis/src/analysis/block_aggregator.py
import pandas as pd
import logging
from typing import Dict, List, Any, Optional
import numpy as np

logger = logging.getLogger(__name__)

def aggregate_block_data_to_wide_format(db_manager: Any, config: Any):
    logger.info("Iniciando agregação de dados de bloco para formato largo (incluindo métricas de grupo).")

    per_dezena_metric_configs = [
        {"source_table_prefix_const_name": "EVOL_METRIC_FREQUENCY_BLOCK_PREFIX", "value_column": "frequencia_absoluta", "analysis_type_name": "frequencia_bloco", "dtype": "Int64"},
        {"source_table_prefix_const_name": "EVOL_RANK_FREQUENCY_BLOCK_PREFIX", "value_column": "rank_no_bloco", "analysis_type_name": "rank_freq_bloco", "dtype": "Int64"},
        {"source_table_prefix_const_name": "EVOL_METRIC_ATRASO_MEDIO_BLOCK_PREFIX", "value_column": "atraso_medio_no_bloco", "analysis_type_name": "atraso_medio_bloco", "dtype": "float"},
        {"source_table_prefix_const_name": "EVOL_METRIC_ATRASO_MAXIMO_BLOCK_PREFIX", "value_column": "atraso_maximo_no_bloco", "analysis_type_name": "atraso_maximo_bloco", "dtype": "Int64"},
        {"source_table_prefix_const_name": "EVOL_METRIC_ATRASO_FINAL_BLOCK_PREFIX", "value_column": "atraso_final_no_bloco", "analysis_type_name": "atraso_final_no_bloco", "dtype": "Int64"},
        {"source_table_prefix_const_name": "EVOL_METRIC_OCCURRENCE_STD_DEV_BLOCK_PREFIX", "value_column": "occurrence_std_dev", "analysis_type_name": "occurrence_std_dev_bloco", "dtype": "float"},
        {"source_table_prefix_const_name": "EVOL_METRIC_DELAY_STD_DEV_BLOCK_PREFIX", "value_column": "delay_std_dev", "analysis_type_name": "delay_std_dev_bloco", "dtype": "float"},
    ]

    required_config_attrs = [
        'CHUNK_TYPES_CONFIG', 'BLOCK_ANALISES_CONSOLIDADAS_PREFIX',
        'ALL_NUMBERS', 'EVOL_BLOCK_GROUP_METRICS_PREFIX'
    ] + [item["source_table_prefix_const_name"] for item in per_dezena_metric_configs]

    missing_attrs = [attr for attr in required_config_attrs if not hasattr(config, attr)]
    if missing_attrs:
        logger.error(f"Atributos de config críticos ausentes para aggregate_block_data_to_wide_format: {missing_attrs}.")
        return

    for chunk_type, list_of_sizes in config.CHUNK_TYPES_CONFIG.items():
        for size_val in list_of_sizes:
            consolidated_table_name = f"{config.BLOCK_ANALISES_CONSOLIDADAS_PREFIX}_{chunk_type}_{size_val}"
            logger.info(f"Processando para tabela consolidada de BLOCKS: '{consolidated_table_name}'")
            all_wide_dfs_for_this_chunk_config: List[pd.DataFrame] = []

            for metric_config_item in per_dezena_metric_configs:
                prefix_constant_name = metric_config_item["source_table_prefix_const_name"]
                source_table_prefix = getattr(config, prefix_constant_name)
                value_column_in_long_table = metric_config_item["value_column"]
                analysis_type_name_for_wide_table = metric_config_item["analysis_type_name"]
                expected_dtype_str = metric_config_item["dtype"]
                long_format_table_name = f"{source_table_prefix}_{chunk_type}_{size_val}"

                if not db_manager.table_exists(long_format_table_name):
                    logger.debug(f"Tabela '{long_format_table_name}' não encontrada para {analysis_type_name_for_wide_table}. Pulando.")
                    continue
                df_long = db_manager.load_dataframe(long_format_table_name)
                if df_long is None or df_long.empty:
                    logger.debug(f"DataFrame de '{long_format_table_name}' vazio para {analysis_type_name_for_wide_table}. Pulando.")
                    continue

                index_cols = ['chunk_seq_id', 'chunk_start_contest', 'chunk_end_contest']
                columns_col = 'dezena'
                required_for_pivot = index_cols + [columns_col, value_column_in_long_table]

                if not all(col in df_long.columns for col in required_for_pivot):
                    logger.error(f"Tabela '{long_format_table_name}' s/ colunas {required_for_pivot}. Colunas: {df_long.columns.tolist()}. Pulando.")
                    continue
                try:
                    if expected_dtype_str == "Int64":
                        df_long[value_column_in_long_table] = pd.to_numeric(df_long[value_column_in_long_table], errors='coerce').astype('Int64')
                    elif expected_dtype_str == "float":
                        df_long[value_column_in_long_table] = pd.to_numeric(df_long[value_column_in_long_table], errors='coerce').astype('float')

                    df_wide_metric = df_long.pivot_table(
                        index=index_cols, columns=columns_col,
                        values=value_column_in_long_table,
                        fill_value=pd.NA if expected_dtype_str == "Int64" else np.nan
                    )
                    df_wide_metric.columns = [f'dezena_{int(col)}' for col in df_wide_metric.columns]
                    df_wide_metric.reset_index(inplace=True)
                    df_wide_metric['tipo_analise'] = analysis_type_name_for_wide_table

                    final_cols_order = index_cols + ['tipo_analise'] + [f'dezena_{i}' for i in config.ALL_NUMBERS]
                    for i in config.ALL_NUMBERS:
                        col_name_to_check = f'dezena_{i}'
                        if col_name_to_check not in df_wide_metric.columns:
                            df_wide_metric[col_name_to_check] = pd.NA if expected_dtype_str == "Int64" else np.nan
                        if expected_dtype_str == "Int64":
                            df_wide_metric[col_name_to_check] = pd.to_numeric(df_wide_metric[col_name_to_check], errors='coerce').astype('Int64')
                        elif expected_dtype_str == "float":
                             df_wide_metric[col_name_to_check] = pd.to_numeric(df_wide_metric[col_name_to_check], errors='coerce').astype('float')

                    actual_final_cols = [col for col in final_cols_order if col in df_wide_metric.columns]
                    all_wide_dfs_for_this_chunk_config.append(df_wide_metric[actual_final_cols])
                except Exception as e:
                    logger.error(f"Erro ao pivotar '{long_format_table_name}' para '{analysis_type_name_for_wide_table}': {e}", exc_info=True)

            block_group_metrics_table_prefix = config.EVOL_BLOCK_GROUP_METRICS_PREFIX
            block_group_metrics_table_name = f"{block_group_metrics_table_prefix}_{chunk_type}_{size_val}"
            df_block_group_metrics = None
            if db_manager.table_exists(block_group_metrics_table_name):
                df_block_group_metrics = db_manager.load_dataframe(block_group_metrics_table_name)
                if df_block_group_metrics is None or df_block_group_metrics.empty:
                     df_block_group_metrics = None
            else:
                logger.debug(f"Tabela de métricas de grupo '{block_group_metrics_table_name}' não encontrada.")

            if all_wide_dfs_for_this_chunk_config:
                df_consolidated_wide = pd.concat(all_wide_dfs_for_this_chunk_config, ignore_index=True)
                if df_block_group_metrics is not None and not df_block_group_metrics.empty: # Adicionado check de não vazio
                    merge_keys = ['chunk_seq_id', 'chunk_start_contest', 'chunk_end_contest']
                    if not df_block_group_metrics[merge_keys].isnull().all(axis=1).any():
                        if all(key in df_consolidated_wide.columns for key in merge_keys) and \
                           all(key in df_block_group_metrics.columns for key in merge_keys):
                            df_consolidated_wide = pd.merge(df_consolidated_wide, df_block_group_metrics, on=merge_keys, how='left')
                        else:
                            logger.warning(f"Chaves de merge para '{consolidated_table_name}' não encontradas. Grupo de bloco não adicionado.")
                    else:
                        logger.warning(f"Métricas de grupo de bloco '{block_group_metrics_table_name}' com chaves nulas. Merge não realizado.")

                if not df_consolidated_wide.empty:
                    # CORREÇÃO APLICADA: removido index=False
                    db_manager.save_dataframe(df_consolidated_wide, consolidated_table_name, if_exists='replace')
                    logger.info(f"Tabela consolidada de BLOCKS '{consolidated_table_name}' salva ({len(df_consolidated_wide)} linhas).")
                else:
                    logger.info(f"DataFrame consolidado de BLOCKS para '{consolidated_table_name}' vazio. Nada salvo.")
            elif df_block_group_metrics is not None and not df_block_group_metrics.empty :
                 logger.info(f"Salvando apenas métricas de grupo para BLOCKS '{consolidated_table_name}'.")
                 # CORREÇÃO APLICADA: removido index=False
                 db_manager.save_dataframe(df_block_group_metrics, consolidated_table_name, if_exists='replace')
            else:
                 logger.warning(f"Nenhum DataFrame gerado para BLOCKS '{consolidated_table_name}'. Tabela não criada/atualizada.")
    logger.info("Agregação de dados de bloco para formato largo concluída.")


def aggregate_cycle_data_to_wide_format(db_manager: Any, config: Any):
    logger.info("Iniciando agregação de dados de CICLO para formato largo.")
    cycle_per_dezena_metric_configs = [
        {"source_table_name_const": "CYCLE_METRIC_FREQUENCY_TABLE_NAME", "value_column": "frequencia_no_ciclo", "analysis_type_name": "frequencia_no_ciclo", "dtype": "Int64"},
        {"source_table_name_const": "CYCLE_METRIC_ATRASO_MEDIO_TABLE_NAME", "value_column": "atraso_medio_no_ciclo", "analysis_type_name": "atraso_medio_no_ciclo", "dtype": "float"},
        {"source_table_name_const": "CYCLE_METRIC_ATRASO_MAXIMO_TABLE_NAME", "value_column": "atraso_maximo_no_ciclo", "analysis_type_name": "atraso_maximo_no_ciclo", "dtype": "Int64"},
        {"source_table_name_const": "CYCLE_METRIC_ATRASO_FINAL_TABLE_NAME", "value_column": "atraso_final_no_ciclo", "analysis_type_name": "atraso_final_no_ciclo", "dtype": "Int64"},
        {"source_table_name_const": "CYCLE_RANK_FREQUENCY_TABLE_NAME", "value_column": "rank_freq_no_ciclo", "analysis_type_name": "rank_freq_no_ciclo", "dtype": "Int64"},
    ]
    consolidated_cycle_table_name = config.CYCLE_ANALISES_CONSOLIDADAS_TABLE_NAME
    all_wide_dfs_for_cycles: List[pd.DataFrame] = []
    cycles_detail_input_table = config.ANALYSIS_CYCLES_DETAIL_TABLE_NAME

    if not db_manager.table_exists(cycles_detail_input_table):
        logger.error(f"Tabela '{cycles_detail_input_table}' não encontrada. Não criar consolidada de ciclos.")
        return
    df_ciclos_detalhe = db_manager.load_dataframe(cycles_detail_input_table)
    if df_ciclos_detalhe is None or df_ciclos_detalhe.empty:
        logger.warning(f"Tabela '{cycles_detail_input_table}' vazia. Não criar consolidada de ciclos.")
        return

    df_closed_ciclos_base_info = df_ciclos_detalhe[df_ciclos_detalhe['concurso_fim'].notna()].copy()
    if df_closed_ciclos_base_info.empty:
        logger.warning(f"Nenhum ciclo fechado em '{cycles_detail_input_table}'. Consolidada de ciclos não gerada.")
        return

    rename_map_ciclos = {'concurso_inicio': 'concurso_inicio_ciclo',
                         'concurso_fim': 'concurso_fim_ciclo'}
    if 'duracao_concursos' in df_closed_ciclos_base_info.columns:
        rename_map_ciclos['duracao_concursos'] = 'duracao_ciclo'
    df_closed_ciclos_base_info.rename(columns=rename_map_ciclos, inplace=True)
    
    # Garantir que ciclo_num exista e seja usado como base
    if 'ciclo_num' not in df_closed_ciclos_base_info.columns:
        logger.error(f"Coluna 'ciclo_num' não encontrada em '{cycles_detail_input_table}' após renomeações.")
        return
        
    base_info_cols = ['ciclo_num', 'concurso_inicio_ciclo', 'concurso_fim_ciclo', 'duracao_ciclo']
    actual_base_info_cols = [col for col in base_info_cols if col in df_closed_ciclos_base_info.columns]
    df_closed_ciclos_base_info = df_closed_ciclos_base_info[actual_base_info_cols].drop_duplicates(subset=['ciclo_num'])


    for metric_config_item in cycle_per_dezena_metric_configs:
        table_name_constant_key = metric_config_item["source_table_name_const"]
        long_format_table_name = getattr(config, table_name_constant_key, table_name_constant_key)
        value_column_in_long_table = metric_config_item["value_column"]
        analysis_type_name_for_wide_table = metric_config_item["analysis_type_name"]
        expected_dtype_str = metric_config_item["dtype"]

        if not db_manager.table_exists(long_format_table_name):
            logger.warning(f"Tabela '{long_format_table_name}' não encontrada para {analysis_type_name_for_wide_table}. Pulando.")
            continue
        df_long_cycle_metric = db_manager.load_dataframe(long_format_table_name)
        if df_long_cycle_metric is None or df_long_cycle_metric.empty:
            logger.warning(f"DataFrame de '{long_format_table_name}' vazio. Pulando."); continue

        index_cols_cycle = ['ciclo_num']; columns_col_cycle = 'dezena'
        required_for_pivot_cycle = index_cols_cycle + [columns_col_cycle, value_column_in_long_table]
        if not all(col in df_long_cycle_metric.columns for col in required_for_pivot_cycle) :
            logger.error(f"Tabela '{long_format_table_name}' s/ colunas {required_for_pivot_cycle}. Colunas: {df_long_cycle_metric.columns.tolist()}. Pulando.")
            continue
        try:
            if expected_dtype_str == "Int64":
                df_long_cycle_metric[value_column_in_long_table] = pd.to_numeric(df_long_cycle_metric[value_column_in_long_table], errors='coerce').astype('Int64')
            elif expected_dtype_str == "float":
                 df_long_cycle_metric[value_column_in_long_table] = pd.to_numeric(df_long_cycle_metric[value_column_in_long_table], errors='coerce').astype('float')

            df_wide_metric = df_long_cycle_metric.pivot_table(
                index=index_cols_cycle, columns=columns_col_cycle,
                values=value_column_in_long_table,
                fill_value=pd.NA if expected_dtype_str == "Int64" else np.nan
            )
            df_wide_metric.columns = [f'dezena_{int(col)}' for col in df_wide_metric.columns]
            df_wide_metric.reset_index(inplace=True)
            df_wide_metric['tipo_analise_ciclo'] = analysis_type_name_for_wide_table
            
            # Merge com df_closed_ciclos_base_info para adicionar informações do ciclo
            if 'ciclo_num' in df_wide_metric.columns: # df_closed_ciclos_base_info já foi verificado
                df_wide_metric_merged = pd.merge(df_closed_ciclos_base_info.copy(), df_wide_metric, on='ciclo_num', how='inner') # Usar .copy() para evitar warnings
            else:
                logger.warning(f"Não merge com base_info para {analysis_type_name_for_wide_table} (sem 'ciclo_num' no df_wide_metric).")
                continue # Pula esta métrica se não puder fazer merge

            final_cols_order = actual_base_info_cols + ['tipo_analise_ciclo'] + [f'dezena_{i}' for i in config.ALL_NUMBERS]
            for i in config.ALL_NUMBERS:
                col_name_to_check = f'dezena_{i}'
                if col_name_to_check not in df_wide_metric_merged.columns:
                    df_wide_metric_merged[col_name_to_check] = pd.NA if expected_dtype_str == "Int64" else np.nan
                if expected_dtype_str == "Int64":
                    df_wide_metric_merged[col_name_to_check] = pd.to_numeric(df_wide_metric_merged[col_name_to_check], errors='coerce').astype('Int64')
                elif expected_dtype_str == "float":
                     df_wide_metric_merged[col_name_to_check] = pd.to_numeric(df_wide_metric_merged[col_name_to_check], errors='coerce').astype('float')

            current_final_cols = [col for col in final_cols_order if col in df_wide_metric_merged.columns]
            all_wide_dfs_for_cycles.append(df_wide_metric_merged[current_final_cols])
        except Exception as e:
            logger.error(f"Erro ao pivotar '{long_format_table_name}' para '{analysis_type_name_for_wide_table}': {e}", exc_info=True)

    df_consolidated_cycles_wide = pd.DataFrame()
    if all_wide_dfs_for_cycles:
        df_consolidated_cycles_wide = pd.concat(all_wide_dfs_for_cycles, ignore_index=True)

    cycle_group_metrics_table_name = config.CYCLE_GROUP_METRICS_TABLE_NAME
    df_cycle_group_metrics = None
    if db_manager.table_exists(cycle_group_metrics_table_name):
        df_cycle_group_metrics = db_manager.load_dataframe(cycle_group_metrics_table_name)
        if df_cycle_group_metrics is None or df_cycle_group_metrics.empty:
            df_cycle_group_metrics = None
    else:
        logger.debug(f"Tabela '{cycle_group_metrics_table_name}' não encontrada.")

    if df_cycle_group_metrics is not None and not df_cycle_group_metrics.empty: # Adicionado check de não vazio
        # Se df_consolidated_cycles_wide estiver vazio, mas temos métricas de grupo e info base de ciclos
        if df_consolidated_cycles_wide.empty and not df_closed_ciclos_base_info.empty:
             if 'ciclo_num' in df_closed_ciclos_base_info.columns and 'ciclo_num' in df_cycle_group_metrics.columns:
                logger.info("Construindo consolidada de ciclos só com base e grupo (sem dados por dezena).")
                df_consolidated_cycles_wide = pd.merge(df_closed_ciclos_base_info.copy(), df_cycle_group_metrics, on='ciclo_num', how='inner') # Usar .copy()
             else:
                logger.warning("Não merge de grupo de ciclo com base_info (sem 'ciclo_num').")
        elif not df_consolidated_cycles_wide.empty: # Se já temos dados por dezena, fazemos merge neles
            if 'ciclo_num' in df_consolidated_cycles_wide.columns and 'ciclo_num' in df_cycle_group_metrics.columns:
                 df_consolidated_cycles_wide = pd.merge(df_consolidated_cycles_wide, df_cycle_group_metrics, on='ciclo_num', how='left')
                 logger.info(f"Métricas de grupo de ciclo adicionadas. Shape: {df_consolidated_cycles_wide.shape}")
            else:
                logger.warning("Coluna 'ciclo_num' não encontrada para merge de grupo de ciclo.")
        else: # Caso onde df_consolidated_cycles_wide estava vazio E df_closed_ciclos_base_info estava vazio
            logger.warning("Não foi possível mesclar métricas de grupo de ciclo por falta de dados base de ciclo.")


    if not df_consolidated_cycles_wide.empty:
        try:
            # CORREÇÃO APLICADA: removido index=False
            db_manager.save_dataframe(df_consolidated_cycles_wide, consolidated_cycle_table_name, if_exists='replace')
            logger.info(f"Tabela consolidada de CICLOS '{consolidated_cycle_table_name}' salva ({len(df_consolidated_cycles_wide)}).")
        except Exception as e:
            logger.error(f"Erro ao salvar consolidada de CICLOS '{consolidated_cycle_table_name}': {e}", exc_info=True)
    else:
        logger.warning(f"Nenhum DataFrame consolidado para CICLOS. Tabela '{consolidated_cycle_table_name}' não criada.")
    logger.info("Agregação de dados de CICLO para formato largo concluída.")