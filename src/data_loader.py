# src/data_loader.py
import pandas as pd
from pathlib import Path
import logging

# Importar as constantes de configuração relevantes
from src.config import (
    RAW_DATA_FILE_NAME,      # Usado para construir caminhos no main.py
    CLEANED_DATA_FILE_NAME,
    COLUMNS_TO_KEEP,         # Nomes das colunas originais no CSV a serem mantidas/processadas
    NEW_COLUMN_NAMES,        # Novos nomes para estas colunas
    BALL_NUMBER_COLUMNS      # Nomes das colunas das bolas após renomeação (ex: 'bola_1')
)

logger = logging.getLogger(__name__)

def load_and_clean_data(raw_file_path: str, cleaned_file_path_to_save: str) -> pd.DataFrame:
    """
    Carrega os dados brutos do arquivo CSV, realiza a limpeza e os transforma.
    Salva os dados limpos em formato pickle para carregamentos futuros mais rápidos.

    Args:
        raw_file_path: Caminho para o arquivo CSV de dados brutos.
        cleaned_file_path_to_save: Caminho onde o DataFrame limpo será salvo (formato pickle).

    Returns:
        pd.DataFrame: DataFrame com os dados limpos e transformados.
                     Retorna um DataFrame vazio em caso de erro.
    """
    try:
        logger.info(f"Iniciando carregamento e limpeza de dados de: {raw_file_path}")
        try:
            # Assumindo que o separador e encoding podem variar.
            # O seu arquivo 'historico.csv' usa ';' como delimitador.
            df = pd.read_csv(raw_file_path, sep=';', encoding='utf-8', header=0)
            logger.info(f"Dados carregados com sucesso de CSV (UTF-8): {raw_file_path}")
        except UnicodeDecodeError:
            logger.warning(f"Falha ao decodificar {raw_file_path} com UTF-8. Tentando com ISO-8859-1.")
            df = pd.read_csv(raw_file_path, sep=';', encoding='iso-8859-1', header=0)
            logger.info(f"Dados carregados com sucesso de CSV (ISO-8859-1): {raw_file_path}")
        # Removido o fallback para Excel, pois EXCEL_FILE_PATH não está em config.py
        # e o foco é no arquivo CSV especificado por raw_file_path.

        logger.debug(f"Colunas originais do CSV: {df.columns.tolist()}")
        
        # Verifica se as colunas definidas em COLUMNS_TO_KEEP existem no DataFrame carregado
        actual_columns_to_keep_from_config = [col for col in COLUMNS_TO_KEEP if col in df.columns]
        
        if len(actual_columns_to_keep_from_config) != len(COLUMNS_TO_KEEP):
            missing_cols = set(COLUMNS_TO_KEEP) - set(actual_columns_to_keep_from_config)
            logger.warning(f"Colunas de COLUMNS_TO_KEEP não encontradas no arquivo: {missing_cols}. Usando as que existem.")
            if not actual_columns_to_keep_from_config:
                logger.error(f"Nenhuma coluna de COLUMNS_TO_KEEP ({COLUMNS_TO_KEEP}) encontrada no arquivo. Verifique a configuração e o arquivo de dados.")
                return pd.DataFrame()
        
        # Seleciona apenas as colunas que existem e estavam em COLUMNS_TO_KEEP
        df_processed = df[actual_columns_to_keep_from_config].copy()

        # Renomeia as colunas selecionadas
        # Garante que NEW_COLUMN_NAMES tenha o mesmo número de elementos que as colunas efetivamente mantidas
        current_col_names = df_processed.columns.tolist()
        rename_map = dict(zip(current_col_names, NEW_COLUMN_NAMES[:len(current_col_names)]))
        df_processed.rename(columns=rename_map, inplace=True)
        logger.debug(f"Colunas renomeadas para: {df_processed.columns.tolist()}")

        # Processamento de data e colunas de bolas
        if 'Data Sorteio' in df_processed.columns:
            df_processed['Data'] = pd.to_datetime(df_processed['Data Sorteio'], format='%d/%m/%Y', errors='coerce')
            df_processed.drop(columns=['Data Sorteio'], inplace=True) # Remove a original
            df_processed.dropna(subset=['Data'], inplace=True) # Remove linhas onde a data é inválida
        else:
            logger.warning("Coluna 'Data Sorteio' não encontrada para conversão. Verifique COLUMNS_TO_KEEP e NEW_COLUMN_NAMES.")

        for ball_col_name in BALL_NUMBER_COLUMNS: # BALL_NUMBER_COLUMNS são os nomes FINAIS (ex: 'bola_1')
            if ball_col_name in df_processed.columns:
                df_processed[ball_col_name] = pd.to_numeric(df_processed[ball_col_name], errors='coerce')
            else:
                logger.warning(f"Coluna de bola esperada '{ball_col_name}' não encontrada após renomeação.")
        
        # Remove linhas onde alguma bola é NaN (problema no dado original ou conversão)
        # Verifica se todas as colunas de bolas existem antes de tentar o dropna nelas
        existing_ball_cols_for_dropna = [col for col in BALL_NUMBER_COLUMNS if col in df_processed.columns]
        if existing_ball_cols_for_dropna:
            df_processed.dropna(subset=existing_ball_cols_for_dropna, inplace=True)
            # Converte para inteiro
            for ball_col_name in existing_ball_cols_for_dropna:
                 df_processed[ball_col_name] = df_processed[ball_col_name].astype(int)
        else:
            logger.warning("Nenhuma coluna de bola encontrada para verificar NaNs ou converter para inteiro.")


        # Garantir as colunas finais e sua ordem
        # O código em `chunk_analysis.py` espera 'Concurso' e 'bola_1'...'bola_15', 'Data'.
        final_expected_cols = ['Concurso', 'Data'] + BALL_NUMBER_COLUMNS
        
        # Selecionar apenas as colunas esperadas que realmente existem no DataFrame
        cols_to_select_final = [col for col in final_expected_cols if col in df_processed.columns]
        
        # Verificar se as colunas essenciais estão presentes
        essential_cols = ['Concurso'] + BALL_NUMBER_COLUMNS
        missing_essential_cols = [col for col in essential_cols if col not in df_processed.columns]
        if missing_essential_cols:
            logger.error(f"Colunas essenciais estão faltando após o processamento: {missing_essential_cols}. Verifique a configuração e os dados.")
            return pd.DataFrame()
            
        df_final = df_processed[cols_to_select_final]

        df_final.to_pickle(cleaned_file_path_to_save)
        logger.info(f"Dados limpos e transformados ({len(df_final)} linhas) salvos em: {cleaned_file_path_to_save}")
        
        return df_final

    except FileNotFoundError:
        logger.error(f"Arquivo de dados brutos não encontrado em: {raw_file_path}. Verifique o caminho.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro durante o carregamento e limpeza dos dados de '{raw_file_path}': {e}", exc_info=True)
        return pd.DataFrame()

def load_cleaned_data(data_dir_path: str) -> pd.DataFrame:
    """
    Carrega os dados limpos de um arquivo pickle.

    Args:
        data_dir_path: Caminho para o diretório que contém o arquivo de dados limpos.
                       O nome do arquivo é obtido de CLEANED_DATA_FILE_NAME em config.py.

    Returns:
        pd.DataFrame: DataFrame com os dados limpos.
                     Retorna um DataFrame vazio se o arquivo não for encontrado ou em caso de erro.
    """
    # CLEANED_DATA_FILE_NAME é apenas o nome do arquivo, data_dir_path é o diretório
    cleaned_file_path = Path(data_dir_path) / CLEANED_DATA_FILE_NAME
    try:
        logger.info(f"Carregando dados limpos de: {cleaned_file_path}")
        df = pd.read_pickle(cleaned_file_path)
        logger.info(f"Dados limpos carregados com sucesso de: {cleaned_file_path}. DataFrame com {len(df)} linhas.")
        return df
    except FileNotFoundError:
        logger.warning(f"Arquivo de dados limpos não encontrado em: {cleaned_file_path}. Execute o processo de limpeza primeiro.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erro ao carregar dados limpos de '{cleaned_file_path}': {e}", exc_info=True)
        return pd.DataFrame()