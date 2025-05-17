# src/analysis/positional_analysis.py
import pandas as pd
import logging
from typing import List, Any
# Importa Config de forma a ser compatível com a estrutura do projeto
# Se config_obj é globalmente acessível ou passado via contexto, ajuste conforme necessário.
# Para este módulo, assumiremos que um objeto config será passado para a função.

logger = logging.getLogger(__name__)

def analyze_draw_position_frequency(all_draws_df: pd.DataFrame, config: Any) -> pd.DataFrame:
    """
    Analisa a frequência de cada dezena (1-25) em cada uma das 15 posições de sorteio.

    Args:
        all_draws_df: DataFrame contendo todos os sorteios históricos.
                      Esperado que tenha colunas como 'ball_1', 'ball_2', ..., 'ball_15'
                      conforme definido em config.BALL_NUMBER_COLUMNS.
        config: O objeto de configuração (instância da classe Config).

    Returns:
        Um DataFrame com dezenas 1-25 como índice ('Dezena') e colunas 'Posicao_1'
        até 'Posicao_15' contendo as contagens de frequência.
        O DataFrame é retornado com 'Dezena' como uma coluna regular para fácil salvamento no BD.
    """
    logger.info("Iniciando análise de frequência posicional das dezenas.")

    if all_draws_df.empty:
        logger.warning("DataFrame de sorteios está vazio. Retornando DataFrame de frequência posicional vazio.")
        pos_cols_names = [f"Posicao_{i+1}" for i in range(config.NUMBERS_PER_DRAW)]
        empty_df = pd.DataFrame(columns=['Dezena'] + pos_cols_names)
        # Para consistência, se esperamos 'Dezena' como coluna, não definir como índice aqui.
        # Se a tabela do BD tem 'Dezena' como PK e o df tem que ter essa coluna, então está ok.
        return empty_df

    ball_cols = config.BALL_NUMBER_COLUMNS
    if not all(col in all_draws_df.columns for col in ball_cols):
        missing_cols = [col for col in ball_cols if col not in all_draws_df.columns]
        logger.error(f"Colunas de bolas esperadas ({ball_cols}) ausentes no DataFrame: {missing_cols}.")
        # Retorna um DataFrame vazio com a estrutura esperada, mas sem dados.
        pos_cols_names = [f"Posicao_{i+1}" for i in range(config.NUMBERS_PER_DRAW)]
        empty_df_structure = pd.DataFrame(0, index=config.ALL_NUMBERS, columns=pos_cols_names)
        empty_df_structure.index.name = 'Dezena'
        return empty_df_structure.reset_index() # 'Dezena' como coluna

    numbers_range = config.ALL_NUMBERS 
    position_columns = [f"Posicao_{i+1}" for i in range(config.NUMBERS_PER_DRAW)] 
    
    # Inicializa o DataFrame com dezenas como índice para facilitar o incremento
    positional_freq_df = pd.DataFrame(0, index=numbers_range, columns=position_columns)
    positional_freq_df.index.name = 'Dezena'

    for _, row in all_draws_df.iterrows():
        for i, ball_col_name in enumerate(ball_cols): # ball_cols é ['ball_1', ..., 'ball_15']
            try:
                # As colunas de bolas no DataFrame limpo já devem ser numéricas (int)
                # Se vierem como string do CSV, data_loader.py deve converter.
                # Se ainda assim puderem ser string ou float, a conversão é necessária.
                number_drawn = row[ball_col_name]
                if pd.isna(number_drawn): # Checa por NaN ou NaT
                    logger.warning(f"Valor NaN encontrado na coluna {ball_col_name} no concurso {row.get(config.CONTEST_ID_COLUMN_NAME, 'N/A')}. Pulando esta entrada.")
                    continue
                
                number_drawn = int(number_drawn) # Garante que é int

                if number_drawn in positional_freq_df.index:
                    # As colunas de posição são 'Posicao_1', 'Posicao_2', ...
                    # O 'i' do enumerate(ball_cols) vai de 0 a 14.
                    col_pos_name = f"Posicao_{i+1}" 
                    positional_freq_df.loc[number_drawn, col_pos_name] += 1
                else:
                    logger.warning(f"Dezena {number_drawn} da coluna {ball_col_name} no concurso {row.get(config.CONTEST_ID_COLUMN_NAME, 'N/A')} está fora do range esperado ({numbers_range}).")

            except ValueError:
                logger.warning(f"Valor não numérico ou não conversível para int encontrado na coluna {ball_col_name} no concurso {row.get(config.CONTEST_ID_COLUMN_NAME, 'N/A')}. Valor: {row[ball_col_name]}. Pulando esta entrada.")
            except Exception as e:
                logger.error(f"Erro inesperado ao processar {ball_col_name} no concurso {row.get(config.CONTEST_ID_COLUMN_NAME, 'N/A')}: {e}", exc_info=True)
    
    logger.info("Análise de frequência posicional concluída.")
    # Retorna com 'Dezena' como uma coluna para facilitar o salvamento no banco de dados,
    # onde 'Dezena' é a chave primária na tabela.
    return positional_freq_df.reset_index()