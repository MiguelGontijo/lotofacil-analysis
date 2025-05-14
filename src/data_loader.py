# src/data_loader.py
import pandas as pd
from pathlib import Path
import logging

# Importar as constantes de configuração relevantes
from src.config import (
    RAW_DATA_FILE_NAME,
    CLEANED_DATA_FILE_NAME,
    COLUMNS_TO_KEEP,
    NEW_COLUMN_NAMES,
    BALL_NUMBER_COLUMNS,
    CONTEST_ID_COLUMN_NAME, # <<< ADICIONADO IMPORT
    DATE_COLUMN_NAME # Para consistência com final_expected_cols
)

logger = logging.getLogger(__name__)

def load_and_clean_data(raw_file_path: str, cleaned_file_path_to_save: str) -> pd.DataFrame:
    """
    Carrega os dados brutos do arquivo CSV, realiza a limpeza e os transforma.
    Salva os dados limpos em formato pickle para carregamentos futuros mais rápidos.
    """
    try:
        logger.info(f"Iniciando carregamento e limpeza de dados de: {raw_file_path}")
        try:
            df = pd.read_csv(raw_file_path, sep=';', encoding='utf-8', header=0)
            logger.info(f"Dados carregados com sucesso de CSV (UTF-8): {raw_file_path}")
        except UnicodeDecodeError:
            logger.warning(f"Falha ao decodificar {raw_file_path} com UTF-8. Tentando com ISO-8859-1.")
            df = pd.read_csv(raw_file_path, sep=';', encoding='iso-8859-1', header=0)
            logger.info(f"Dados carregados com sucesso de CSV (ISO-8859-1): {raw_file_path}")

        logger.debug(f"Colunas originais do CSV: {df.columns.tolist()}")
        
        actual_columns_to_keep_from_config = [col for col in COLUMNS_TO_KEEP if col in df.columns]
        
        if len(actual_columns_to_keep_from_config) != len(COLUMNS_TO_KEEP):
            missing_cols = set(COLUMNS_TO_KEEP) - set(actual_columns_to_keep_from_config)
            logger.warning(f"Colunas de COLUMNS_TO_KEEP não encontradas no arquivo: {missing_cols}. Usando as que existem.")
            if not actual_columns_to_keep_from_config:
                logger.error(f"Nenhuma coluna de COLUMNS_TO_KEEP ({COLUMNS_TO_KEEP}) encontrada no arquivo. Verifique a configuração e o arquivo de dados.")
                return pd.DataFrame()
        
        df_processed = df[actual_columns_to_keep_from_config].copy()

        current_col_names = df_processed.columns.tolist()
        # Garante que NEW_COLUMN_NAMES tenha o tamanho certo para o zip
        effective_new_column_names = NEW_COLUMN_NAMES[:len(current_col_names)]
        rename_map = dict(zip(current_col_names, effective_new_column_names))
        df_processed.rename(columns=rename_map, inplace=True)
        logger.debug(f"Colunas renomeadas para: {df_processed.columns.tolist()}")

        # A lógica original do seu data_loader verifica por 'Data Sorteio' APÓS a renomeação.
        # Isso implica que 'Data Sorteio' não deve ser renomeada agressivamente por NEW_COLUMN_NAMES
        # se esta lógica for para funcionar como está.
        # Se NEW_COLUMN_NAMES[1] (correspondente a COLUMNS_TO_KEEP[1] == 'Data Sorteio')
        # for, por exemplo, 'draw_date_str', então a verificação abaixo deveria ser
        # if 'draw_date_str' in df_processed.columns:
        # Para o config.py que forneci, NEW_COLUMN_NAMES[1] é 'Data Sorteio'.
        
        date_column_after_rename = None
        if COLUMNS_TO_KEEP[1] == 'Data Sorteio': # Assumindo que a segunda coluna em COLUMNS_TO_KEEP é a data
            date_column_after_rename = NEW_COLUMN_NAMES[1] # Este é o nome da coluna de data após a renomeação

        if date_column_after_rename and date_column_after_rename in df_processed.columns:
            # DATE_COLUMN_NAME é o nome final da coluna de data ('date' por default)
            df_processed[DATE_COLUMN_NAME] = pd.to_datetime(df_processed[date_column_after_rename], format='%d/%m/%Y', errors='coerce')
            if date_column_after_rename != DATE_COLUMN_NAME: # Só dropa se o nome for diferente do nome final
                df_processed.drop(columns=[date_column_after_rename], inplace=True)
            df_processed.dropna(subset=[DATE_COLUMN_NAME], inplace=True)
        else:
            logger.warning(f"Coluna de data ('{date_column_after_rename}') não encontrada após renomeação para conversão. Verifique COLUMNS_TO_KEEP e NEW_COLUMN_NAMES.")

        for ball_col_name in BALL_NUMBER_COLUMNS:
            if ball_col_name in df_processed.columns:
                df_processed[ball_col_name] = pd.to_numeric(df_processed[ball_col_name], errors='coerce')
            else:
                logger.warning(f"Coluna de bola esperada '{ball_col_name}' não encontrada após renomeação.")
        
        existing_ball_cols_for_dropna = [col for col in BALL_NUMBER_COLUMNS if col in df_processed.columns]
        if existing_ball_cols_for_dropna:
            df_processed.dropna(subset=existing_ball_cols_for_dropna, inplace=True)
            for ball_col_name in existing_ball_cols_for_dropna:
                 df_processed[ball_col_name] = df_processed[ball_col_name].astype(int)
        else:
            logger.warning("Nenhuma coluna de bola encontrada para verificar NaNs ou converter para inteiro.")

        # --- CORREÇÃO AQUI para essential_cols e final_expected_cols ---
        # Usar os nomes de colunas FINAIS padronizados do config.py
        final_expected_cols = [CONTEST_ID_COLUMN_NAME, DATE_COLUMN_NAME] + BALL_NUMBER_COLUMNS
        
        cols_to_select_final = [col for col in final_expected_cols if col in df_processed.columns]
        
        essential_cols = [CONTEST_ID_COLUMN_NAME] + BALL_NUMBER_COLUMNS # Colunas essenciais após toda a renomeação
        # --- FIM DA CORREÇÃO ---
        
        missing_essential_cols = [col for col in essential_cols if col not in df_processed.columns]
        if missing_essential_cols:
            logger.error(f"Colunas essenciais estão faltando após o processamento: {missing_essential_cols}. Verifique a configuração e os dados.")
            logger.debug(f"Colunas disponíveis em df_processed: {df_processed.columns.tolist()}")
            return pd.DataFrame()
            
        df_final = df_processed[cols_to_select_final].copy() # Usar .copy() para evitar SettingWithCopyWarning

        # Adicionar a coluna 'drawn_numbers' (lista de dezenas) que muitas análises podem esperar
        # Esta coluna não estava sendo criada no seu data_loader.py, mas é um padrão útil.
        # As análises que forneci (como combination_analysis) esperam esta coluna.
        # Se você não a quiser, as análises precisarão ser ajustadas para ler de 'ball_1'...'ball_15'.
        try:
            df_final['drawn_numbers'] = df_final[BALL_NUMBER_COLUMNS].apply(lambda row: sorted([int(num) for num in row if pd.notna(num)]), axis=1)
            logger.info("Coluna 'drawn_numbers' (lista de dezenas) criada.")
        except Exception as e_drawn:
            logger.warning(f"Não foi possível criar a coluna 'drawn_numbers': {e_drawn}")


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
    """
    cleaned_file_path = Path(data_dir_path) / CLEANED_DATA_FILE_NAME # CLEANED_DATA_FILE_NAME é importado do config
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