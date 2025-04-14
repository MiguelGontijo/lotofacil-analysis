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
from src.analysis.combination_analysis import calculate_combination_frequency
# Importa a nova função de análise de ciclos
from src.analysis.cycle_analysis import identify_cycles

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)


def run_pipeline(reload_data: bool = False):
    """
    Executa o pipeline completo: carrega do Excel (opcional), salva no BD (opcional),
    e executa as análises de frequência, combinações e ciclos.
    """
    logger.info("Iniciando a aplicação Lotofacil Analysis")

    # --- Bloco de Carga/Salvamento (igual ao anterior) ---
    if reload_data:
        logger.info("Opção 'reload_data' ativa...") # Mensagens omitidas por brevidade
        cleaned_data = load_and_clean_data()
        if cleaned_data is not None:
            success = save_to_db(df=cleaned_data, if_exists='replace')
            if not success: return
        else: return
    else:
        logger.info("Opção 'reload_data' inativa...") # Mensagens omitidas por brevidade
        test_read = read_data_from_db(columns=['concurso'], concurso_maximo=1)
        if test_read is None or test_read.empty: return

    # --- Passo 3: Análises de Frequência ---
    logger.info("Iniciando análises de frequência...")
    CONCURSO_MAXIMO_TESTE = None
    # (Código das análises de frequência omitido para brevidade - é o mesmo de antes)
    # ...
    logger.info("Análises de frequência concluídas.")


    # --- Passo 4: Análises de Combinação ---
    logger.info("Iniciando análises de combinação...")
    TOP_N_COMBINATIONS = 15
    # (Código das análises de combinação omitido para brevidade - é o mesmo de antes)
    # for size in [3, 4, 5, 6]:
    #     ...
    logger.info("Análises de combinação concluídas.")


    # --- Passo 5: Análise de Ciclos ---
    logger.info("Iniciando análise de ciclos...")
    cycles_summary = identify_cycles()

    if cycles_summary is not None and not cycles_summary.empty:
        print("\n--- Resumo dos Ciclos Completos da Lotofácil ---")
        # Configura o índice para facilitar a visualização (opcional)
        # cycles_summary.set_index('numero_ciclo', inplace=True)
        print(cycles_summary.to_string(index=False)) # to_string para ver todas as linhas se forem muitas

        # Calcular estatísticas básicas sobre os ciclos
        avg_duration = cycles_summary['duracao'].mean()
        min_duration = cycles_summary['duracao'].min()
        max_duration = cycles_summary['duracao'].max()
        print(f"\nEstatísticas dos Ciclos:")
        print(f"- Número de ciclos completos: {len(cycles_summary)}")
        print(f"- Duração Média: {avg_duration:.2f} concursos")
        print(f"- Ciclo Mais Curto: {min_duration} concursos")
        print(f"- Ciclo Mais Longo: {max_duration} concursos")

    elif cycles_summary is not None and cycles_summary.empty:
         logger.info("Nenhum ciclo completo foi encontrado nos dados analisados.")
    else:
        logger.error("Falha ao executar a análise de ciclos.")


    logger.info("Análise de ciclos concluída.")
    logger.info("Aplicação Lotofacil Analysis finalizada.")


if __name__ == "__main__":
    # Se quiser recarregar do Excel: True. Caso contrário: False
    RELOAD_FROM_EXCEL = False
    run_pipeline(reload_data=RELOAD_FROM_EXCEL)