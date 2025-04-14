# src/main.py

import logging
import pandas as pd
from src.config import logger, TABLE_NAME, DATABASE_PATH
from src.data_loader import load_and_clean_data
from src.database_manager import save_to_db, read_data_from_db
from src.analysis.frequency_analysis import (
    calculate_overall_frequency,
    calculate_windowed_frequency,
    calculate_cumulative_frequency_history
)
# Importa a nova função de análise de combinações
from src.analysis.combination_analysis import calculate_combination_frequency

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)


def run_pipeline(reload_data: bool = False):
    """
    Executa o pipeline completo: carrega do Excel (opcional), salva no BD (opcional),
    e executa as análises de frequência e combinações.
    """
    logger.info("Iniciando a aplicação Lotofacil Analysis")

    # --- Bloco de Carga/Salvamento (igual ao anterior) ---
    if reload_data:
        logger.info("Opção 'reload_data' ativa. Recarregando do Excel e salvando no BD.")
        cleaned_data = load_and_clean_data()
        if cleaned_data is not None:
            logger.info("Dados carregados e limpos com sucesso do Excel.")
            success = save_to_db(df=cleaned_data, if_exists='replace')
            if not success:
                logger.error("Falha ao salvar os dados no banco de dados.")
                return
        else:
            logger.error("Não foi possível carregar os dados do Excel.")
            return
    else:
        logger.info("Opção 'reload_data' inativa. Assumindo que os dados estão no BD.")
        test_read = read_data_from_db(columns=['concurso'], concurso_maximo=1)
        if test_read is None or test_read.empty:
             logger.error(f"Não foi possível ler dados do BD. Execute com reload_data=True ou verifique o BD.")
             return

    # --- Passo 3: Executar Análises de Frequência ---
    logger.info("Iniciando análises de frequência...")
    CONCURSO_MAXIMO_TESTE = None # Mantenha None para analisar tudo, ou defina um nº ex: 1500

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


    # --- Passo 4: Executar Análises de Combinação ---
    logger.info("Iniciando análises de combinação...")
    TOP_N_COMBINATIONS = 15 # Quantas combinações mais frequentes mostrar

    for size in [3, 4, 5, 6]: # Trios, Quartetos, Quintetos, Sextetos
         combo_name = {3: "Trios", 4: "Quartetos", 5: "Quintetos", 6: "Sextetos"}.get(size)
         logger.info(f"Calculando os {TOP_N_COMBINATIONS} {combo_name} mais frequentes...")
         top_combos = calculate_combination_frequency(
             combination_size=size,
             top_n=TOP_N_COMBINATIONS,
             concurso_maximo=CONCURSO_MAXIMO_TESTE
         )

         if top_combos:
             print(f"\n--- Top {TOP_N_COMBINATIONS} {combo_name} Mais Frequentes ---")
             # Formata a saída para melhor legibilidade
             for combo, count in top_combos:
                 # Converte a tupla de números em uma string formatada
                 combo_str = ", ".join(map(str, combo))
                 print(f"Combinação: ({combo_str}) - Frequência: {count}")
         else:
             logger.warning(f"Não foram encontradas combinações para {combo_name}.")

    logger.info("Análises de combinação concluídas.")
    logger.info("Aplicação Lotofacil Analysis finalizada.")


if __name__ == "__main__":
    RELOAD_FROM_EXCEL = False
    run_pipeline(reload_data=RELOAD_FROM_EXCEL)