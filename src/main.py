# src/main.py

import logging
import pandas as pd # Import pandas para formatação
from src.config import logger, TABLE_NAME, DATABASE_PATH
from src.data_loader import load_and_clean_data
from src.database_manager import save_to_db, read_data_from_db
from src.analysis.frequency_analysis import (
    calculate_overall_frequency,
    calculate_windowed_frequency,
    calculate_cumulative_frequency_history
)

# Configurar Pandas para mostrar mais linhas/colunas se necessário
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)


# Renomeamos a função para refletir melhor sua ação
def run_pipeline(reload_data: bool = False):
    """
    Executa o pipeline completo: carrega do Excel (opcional), salva no BD (opcional),
    e executa as análises de frequência.

    Args:
        reload_data (bool): Se True, recarrega os dados do Excel e substitui no BD.
                            Se False, assume que os dados já estão no BD.
    """
    logger.info("Iniciando a aplicação Lotofacil Analysis")

    if reload_data:
        logger.info("Opção 'reload_data' ativa. Recarregando do Excel e salvando no BD.")
        cleaned_data = load_and_clean_data()

        if cleaned_data is not None:
            logger.info("Dados carregados e limpos com sucesso do Excel.")
            success = save_to_db(df=cleaned_data, if_exists='replace')
            if success:
                logger.info(f"Dados salvos com sucesso no banco de dados: {DATABASE_PATH}")
            else:
                logger.error("Falha ao salvar os dados no banco de dados. Análises podem não funcionar.")
                return
        else:
            logger.error("Não foi possível carregar os dados do Excel. Processo interrompido.")
            return
    else:
        logger.info("Opção 'reload_data' inativa. Assumindo que os dados estão no BD.")
        test_read = read_data_from_db(columns=['concurso'], concurso_maximo=1)
        if test_read is None or test_read.empty:
             logger.error(f"Não foi possível ler dados do BD ({DATABASE_PATH}, tabela {TABLE_NAME}). Execute com reload_data=True na primeira vez ou verifique o BD.")
             return

    logger.info("Iniciando análises de frequência...")
    CONCURSO_MAXIMO_TESTE = None # Exemplo: 1500

    overall_freq = calculate_overall_frequency(concurso_maximo=CONCURSO_MAXIMO_TESTE)
    if overall_freq is not None:
        print("\n--- Frequência Geral das Dezenas ---")
        print(overall_freq.to_string())

    for window in [10, 25, 50]:
        window_freq = calculate_windowed_frequency(window_size=window, concurso_maximo=CONCURSO_MAXIMO_TESTE)
        if window_freq is not None:
            print(f"\n--- Frequência nos Últimos {window} Concursos ---")
            print(window_freq.to_string())

    cumulative_hist = calculate_cumulative_frequency_history(concurso_maximo=CONCURSO_MAXIMO_TESTE)
    if cumulative_hist is not None:
        print("\n--- Histórico de Frequência Acumulada (Últimos 5 Registros) ---")
        print(cumulative_hist.tail())

    logger.info("Análises de frequência concluídas.")
    logger.info("Aplicação Lotofacil Analysis finalizada.")


if __name__ == "__main__":
    RELOAD_FROM_EXCEL = False
    # *** CORREÇÃO APLICADA AQUI ***
    # Chamada correta para a função definida acima
    run_pipeline(reload_data=RELOAD_FROM_EXCEL)