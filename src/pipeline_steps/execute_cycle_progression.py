import logging
import pandas as pd
from typing import Any, Dict, List, Set

from src.analysis.cycle_progression_analysis import calculate_cycle_progression
# Removido: from src.config import config_obj # Usaremos o config injetado

logger = logging.getLogger(__name__)

def run_cycle_progression_analysis_step(
    all_data_df: pd.DataFrame,
    db_manager: Any, # Idealmente DatabaseManager
    config: Any, # Nome do parâmetro corrigido de config_param para config
    shared_context: Dict[str, Any],
    **kwargs
) -> bool:
    step_name = "Cycle Progression and Status Analysis"
    logger.info(f"==== Iniciando Etapa: {step_name} ====")

    if all_data_df.empty:
        logger.warning(f"{step_name}: DataFrame de dados (all_data_df) está vazio. Etapa pulada.")
        return True

    # Verificar se atributos essenciais de config existem
    required_config_attrs = [
        'CONTEST_ID_COLUMN_NAME', 'ALL_NUMBERS',
        'ANALYSIS_CYCLE_PROGRESSION_RAW_TABLE_NAME',
        'ANALYSIS_CYCLE_STATUS_DEZENAS_TABLE_NAME'
    ]
    for attr in required_config_attrs:
        if not hasattr(config, attr): # Usar o 'config' injetado
            logger.error(f"{step_name}: Atributo de config '{attr}' não encontrado. Abortando.")
            return False

    try:
        # A função calculate_cycle_progression precisará ser ajustada se ela usa config_obj internamente,
        # ou podemos passar o objeto config para ela.
        # Assumindo que calculate_cycle_progression pode aceitar config:
        # df_progression_raw = calculate_cycle_progression(all_data_df, config)
        # Se calculate_cycle_progression depende de config_obj globalmente, essa é uma refatoração maior.
        # Por ora, vamos manter a chamada como está, e o problema interno de calculate_cycle_progression
        # com config_obj (se houver) precisaria ser tratado em src/analysis/cycle_progression_analysis.py.
        # O código original fornecido aqui já chamava calculate_cycle_progression(all_data_df, config_obj)
        # Onde config_obj era importado. Se config_obj é o mesmo que o 'config' injetado,
        # então está ok, mas o ideal é injetar dependências.
        # Para esta correção, vamos focar na assinatura do step e assumir que config_obj é o desejado por calculate_cycle_progression.
        # No entanto, a boa prática é usar o 'config' injetado.
        # Se calculate_cycle_progression SÓ usa config_obj, então o parâmetro 'config' neste step
        # não estaria sendo usado para essa chamada específica, mas poderia ser para outras coisas.
        # Vou manter a chamada a calculate_cycle_progression como estava, mas a recomendação é refatorá-la.
        df_progression_raw = calculate_cycle_progression(all_data_df, config) # << MUDADO PARA USAR config injetado

        status_dezenas_records: List[Dict[str, Any]] = []

        if df_progression_raw is None or df_progression_raw.empty:
            logger.warning(f"{step_name}: Nenhum dado de progressão de ciclo foi gerado. "
                           "A tabela de status de dezenas será populada com defaults.")
            all_contest_ids_in_input = sorted(all_data_df[config.CONTEST_ID_COLUMN_NAME].unique()) # Usar config injetado
            for c_id in all_contest_ids_in_input:
                for dezena_val in config.ALL_NUMBERS: # Usar config injetado
                    status_dezenas_records.append({
                        config.CONTEST_ID_COLUMN_NAME: c_id, # Usar config injetado
                        'dezena': dezena_val,
                        'is_missing_in_current_cycle': 0
                    })
            if not all_contest_ids_in_input:
                 logger.info(f"{step_name}: Nenhum dado de progressão e nenhum concurso. Saindo.")
                 return True
        else:
            raw_progression_table_name = config.ANALYSIS_CYCLE_PROGRESSION_RAW_TABLE_NAME # Usar config injetado
            db_manager.save_dataframe(df_progression_raw, raw_progression_table_name, if_exists='replace')
            logger.info(f"Dados brutos de progressão de ciclo salvos na '{raw_progression_table_name}' ({len(df_progression_raw)}).")

            logger.info(f"{step_name}: Transformando dados de progressão para status de dezenas...")
            col_id_from_raw = "Concurso"
            col_faltantes_from_raw = 'numeros_faltantes_apos_este_concurso'

            if col_id_from_raw not in df_progression_raw.columns or \
               col_faltantes_from_raw not in df_progression_raw.columns:
                logger.error(f"Colunas esperadas não encontradas em df_progression_raw. Não gerar status.")
                return False

            for _, row in df_progression_raw.iterrows():
                concurso_id_val = int(row[col_id_from_raw])
                faltantes_str = row[col_faltantes_from_raw]
                numeros_faltantes_set: Set[int] = set()
                if pd.notna(faltantes_str) and isinstance(faltantes_str, str) and faltantes_str.strip():
                    try:
                        numeros_faltantes_set = set(map(int, faltantes_str.split(',')))
                    except ValueError:
                        logger.warning(f"Erro ao parsear faltantes ('{faltantes_str}') para concurso {concurso_id_val}.")

                for dezena_val in config.ALL_NUMBERS: # Usar config injetado
                    is_missing = 1 if dezena_val in numeros_faltantes_set else 0
                    status_dezenas_records.append({
                        config.CONTEST_ID_COLUMN_NAME: concurso_id_val, # Usar config injetado
                        'dezena': dezena_val,
                        'is_missing_in_current_cycle': is_missing
                    })

        if not status_dezenas_records:
            logger.warning(f"{step_name}: Nenhum registro de status de dezenas foi gerado.")
        else:
            df_status_dezenas = pd.DataFrame(status_dezenas_records)
            df_status_dezenas.drop_duplicates(subset=[config.CONTEST_ID_COLUMN_NAME, 'dezena'], keep='last', inplace=True) # Usar config

            status_table_name = config.ANALYSIS_CYCLE_STATUS_DEZENAS_TABLE_NAME # Usar config injetado
            db_manager.save_dataframe(df_status_dezenas, status_table_name, if_exists='replace')
            logger.info(f"Dados de status de dezenas salvos na '{status_table_name}' ({len(df_status_dezenas)}).")

        logger.info(f"==== Etapa: {step_name} CONCLUÍDA ====")
        return True

    except AttributeError as ae: # Este AttributeError agora pode ser mais específico se o config não tiver um atributo
        logger.error(f"Erro de atributo em {step_name}: {ae}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False
    except Exception as e:
        logger.error(f"Erro na etapa {step_name}: {e}", exc_info=True)
        logger.info(f"==== Etapa: {step_name} FALHOU ====")
        return False