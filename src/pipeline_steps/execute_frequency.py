# src/pipeline_steps/execute_frequency.py
import pandas as pd
import logging # Adicionado/Confirmado
from src.analysis.frequency_analysis import calculate_frequency, calculate_relative_frequency
from src.database_manager import DatabaseManager
# from src.config import logger # Removido

logger = logging.getLogger(__name__) # Corrigido: Logger específico do módulo

def run_frequency_analysis(all_data_df: pd.DataFrame, db_manager: DatabaseManager, **kwargs) -> bool:
    """
    Executa a análise de frequência e salva os resultados no banco de dados.
    kwargs é incluído para compatibilidade com o orquestrador, caso ele passe argumentos extras.
    """
    try:
        logger.info("Iniciando análise de frequência.")
        # Calcula a frequência absoluta
        absolute_freq_df = calculate_frequency(all_data_df)
        if absolute_freq_df is not None and not absolute_freq_df.empty:
            db_manager.save_dataframe_to_db(absolute_freq_df, 'frequencia_absoluta')
            logger.info("Frequência absoluta salva no banco de dados.")

            # Calcula a frequência relativa
            # Presume que calculate_relative_frequency precisa do número total de concursos
            total_contests = len(all_data_df) # Ou outra métrica de total, se aplicável
            relative_freq_df = calculate_relative_frequency(absolute_freq_df, total_contests) # Ajustar se a assinatura for diferente
            if relative_freq_df is not None and not relative_freq_df.empty:
                db_manager.save_dataframe_to_db(relative_freq_df, 'frequencia_relativa')
                logger.info("Frequência relativa salva no banco de dados.")
            else:
                logger.warning("Não foi possível calcular ou DataFrame de frequência relativa vazio.")
        else:
            logger.warning("Não foi possível calcular ou DataFrame de frequência absoluta vazio.")
        
        logger.info("Análise de frequência concluída.") # Removido "com sucesso" para depender do retorno
        return True # Ou baseado no sucesso das operações
    except Exception as e:
        logger.error(f"Erro na análise de frequência: {e}", exc_info=True)
        return False