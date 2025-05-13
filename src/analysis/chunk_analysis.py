# src/analysis/chunk_analysis.py
import pandas as pd
from typing import List, Dict, Tuple, Any, Set # Adicionado Set
import logging # Adicionado

# Importar apenas as constantes REALMENTE existentes e necessárias de config.py
from src.config import ALL_NUMBERS, CHUNK_TYPES_CONFIG
# logger, DATABASE_PATH, ALL_NUMBERS_SET são removidos da importação de config

from src.database_manager import DatabaseManager # Para type hinting

logger = logging.getLogger(__name__) # Logger específico para este módulo

# Se um conjunto de ALL_NUMBERS for frequentemente usado para checagens de 'in',
# pode ser útil defini-lo aqui para performance, embora para 25 números, a diferença seja mínima.
ALL_NUMBERS_AS_SET: Set[int] = set(ALL_NUMBERS)

def get_chunk_definitions(total_contests: int, chunk_config_type: str, chunk_config_sizes: List[int]) -> List[Tuple[int, int, str]]:
    """
    Gera definições de blocos (chunks) com base no tipo e tamanhos configurados.
    Esta função é chamada com uma lista contendo um único tamanho de chunk_config_sizes por vez
    a partir de calculate_chunk_metrics_and_persist.

    Args:
        total_contests: Número total de concursos disponíveis.
        chunk_config_type: Tipo de configuração do bloco (ex: 'linear', 'fibonacci').
        chunk_config_sizes: Lista de tamanhos para o tipo de bloco (espera-se um único tamanho nesta lista).

    Returns:
        Lista de tuplas, onde cada tupla contém (concurso_inicial, concurso_final, nome_sufixo_bloco).
    """
    definitions: List[Tuple[int, int, str]] = []
    if not chunk_config_sizes:
        logger.warning(f"Nenhum tamanho configurado para o tipo de bloco: {chunk_config_type}")
        return definitions

    # chunk_config_sizes é esperado como uma lista com um único item [size]
    # devido a como é chamado em calculate_chunk_metrics_and_persist
    for sz_item in chunk_config_sizes:
        if sz_item <= 0:
            logger.warning(f"Tamanho de chunk inválido (deve ser > 0): {sz_item} para o tipo {chunk_config_type}. Pulando este tamanho.")
            continue
            
        current_pos = 0
        while current_pos < total_contests:
            start_contest = current_pos + 1 # Concursos são 1-based
            end_contest = min(current_pos + sz_item, total_contests)
            
            # Adiciona o chunk apenas se start_contest não ultrapassar end_contest
            # (embora com current_pos < total_contests e sz_item > 0, start_contest <= end_contest deve ser verdade
            # a menos que o último chunk seja menor que sz_item e já tenha sido coberto)
            if start_contest > end_contest and current_pos >= total_contests : # Evita loop infinito se algo der errado
                 break # Segurança

            # O sufixo é apenas para identificação se necessário, mas não é usado criticamente mais tarde
            suffix = f"{chunk_config_type}_{sz_item}"
            definitions.append((start_contest, end_contest, suffix))
            
            current_pos = end_contest
            if current_pos >= total_contests: # Sai se todos os concursos foram cobertos
                break
                
    logger.debug(f"Definições de chunk para tipo='{chunk_config_type}', tamanhos={chunk_config_sizes}: {len(definitions)} blocos gerados.")
    return definitions


def calculate_frequency_in_chunk(df_chunk: pd.DataFrame) -> pd.Series:
    """
    Calcula a frequência absoluta de cada dezena em um chunk de dados.
    As dezenas são indexadas por ALL_NUMBERS.
    """
    if df_chunk.empty:
        # Retorna uma série com todas as dezenas e frequência 0
        return pd.Series(0, index=pd.Index(ALL_NUMBERS, name="dezena"), name="frequencia_absoluta", dtype='int')
    
    # As dezenas estão nas colunas 'bola_1' a 'bola_15'
    dezena_cols = [f'bola_{i}' for i in range(1, 16)]
    
    # Verifica se todas as colunas de bolas esperadas existem no df_chunk
    missing_ball_cols = [col for col in dezena_cols if col not in df_chunk.columns]
    if missing_ball_cols:
        logger.warning(f"Colunas de bolas ausentes no chunk: {missing_ball_cols}. Frequência pode ser imprecisa.")
        # Continuar com as colunas existentes
        dezena_cols = [col for col in dezena_cols if col in df_chunk.columns]
        if not dezena_cols: # Nenhuma coluna de bola encontrada
            logger.error("Nenhuma coluna de bola encontrada no chunk. Não é possível calcular frequência.")
            return pd.Series(0, index=pd.Index(ALL_NUMBERS, name="dezena"), name="frequencia_absoluta", dtype='int')

    all_drawn_numbers_in_chunk = df_chunk[dezena_cols].values.flatten()
    
    # Conta frequências e reindexa para garantir todas as dezenas de ALL_NUMBERS
    frequency_series = pd.Series(all_drawn_numbers_in_chunk).value_counts()
    frequency_series = frequency_series.reindex(ALL_NUMBERS, fill_value=0)
    frequency_series.name = "frequencia_absoluta"
    frequency_series.index.name = "dezena" # Nomeia o índice
    
    return frequency_series.astype(int)


def calculate_chunk_metrics_and_persist(all_data_df: pd.DataFrame, db_manager: DatabaseManager):
    """
    Calcula métricas de chunk (inicialmente frequência) para todas as configurações
    e persiste os resultados em tabelas formatadas para análise de evolução.

    Args:
        all_data_df: DataFrame com todos os concursos. Colunas 'Concurso', 'Data', 'bola_1'...'bola_15'.
        db_manager: Instância do DatabaseManager para salvar os dados.
    """
    logger.info("Iniciando cálculo e persistência de métricas de chunk para evolução.")
    if 'Concurso' not in all_data_df.columns:
        logger.error("Coluna 'Concurso' não encontrada no DataFrame principal. Não é possível processar chunks.")
        return
        
    total_contests = all_data_df['Concurso'].max()
    if pd.isna(total_contests) or total_contests <= 0:
        logger.error(f"Número total de concursos inválido: {total_contests}. Não é possível processar chunks.")
        return

    for chunk_type, config in CHUNK_TYPES_CONFIG.items():
        chunk_sizes = config.get('sizes', [])
        # Métricas a serem calculadas, por enquanto apenas frequência.
        # No futuro, pode vir de config.get('metrics', ['frequency'])
        # metrics_to_calculate = ['frequency'] # Hardcoded por enquanto

        for size in chunk_sizes:
            logger.info(f"Processando chunks: tipo='{chunk_type}', tamanho={size}, métrica='frequency'")
            
            # Passa size como uma lista de um elemento para get_chunk_definitions
            chunk_definitions = get_chunk_definitions(total_contests, chunk_type, [size])

            if not chunk_definitions:
                logger.warning(f"Nenhuma definição de chunk para tipo='{chunk_type}', tamanho={size}. Pulando.")
                continue

            all_metrics_for_this_config: List[Dict[str, Any]] = []
            
            for idx, (start_contest, end_contest, _) in enumerate(chunk_definitions):
                mask = (all_data_df['Concurso'] >= start_contest) & (all_data_df['Concurso'] <= end_contest)
                df_current_chunk = all_data_df[mask]

                # Mesmo se o chunk estiver vazio (ex: poucos concursos totais e tamanho de chunk grande),
                # queremos calcular a frequência (que será 0 para todas as dezenas).
                # calculate_frequency_in_chunk já lida com df_current_chunk vazio.
                if df_current_chunk.empty:
                     logger.debug(f"Chunk {chunk_type}_{size} (Concursos {start_contest}-{end_contest}) está vazio. Frequências serão 0.")
                
                frequency_series_chunk = calculate_frequency_in_chunk(df_current_chunk)
                
                for dezena_val, freq_val in frequency_series_chunk.items():
                    all_metrics_for_this_config.append({
                        'chunk_seq_id': idx + 1, 
                        'chunk_start_contest': start_contest,
                        'chunk_end_contest': end_contest,
                        'dezena': int(dezena_val),
                        'frequencia_absoluta': int(freq_val)
                    })

            if not all_metrics_for_this_config:
                logger.info(f"Nenhuma métrica calculada para chunks tipo='{chunk_type}', tamanho={size}. Nada a persistir.")
                continue

            metrics_df = pd.DataFrame(all_metrics_for_this_config)
            table_name = f"evol_metric_frequency_{chunk_type}_{size}"
            
            try:
                db_manager.save_dataframe_to_db(metrics_df, table_name, if_exists='replace')
                logger.info(f"Métricas de frequência para chunks tipo='{chunk_type}', tamanho={size} salvas na tabela '{table_name}'. {len(metrics_df)} registros.")
            except Exception as e:
                logger.error(f"Erro ao salvar métricas para chunks tipo='{chunk_type}', tamanho={size} na tabela '{table_name}': {e}", exc_info=True)

    logger.info("Cálculo e persistência de métricas de chunk para evolução concluídos.")