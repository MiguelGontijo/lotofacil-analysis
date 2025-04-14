# src/main.py

import logging
import argparse
import pandas as pd

# Importações de configuração e análise
from src.config import logger, TABLE_NAME, DATABASE_PATH
from src.data_loader import load_and_clean_data
from src.database_manager import save_to_db, read_data_from_db
from src.analysis.frequency_analysis import (
    calculate_frequency as calculate_period_frequency,
    calculate_windowed_frequency,
    calculate_cumulative_frequency_history
)
from src.analysis.combination_analysis import calculate_combination_frequency
from src.analysis.cycle_analysis import identify_cycles, run_cycle_frequency_analysis
from src.analysis.delay_analysis import calculate_current_delay
from src.analysis.number_properties_analysis import analyze_number_properties, summarize_properties

# Importa funções de plotagem com tratamento de erro
try:
    # Importa as novas funções de plot
    from src.visualization.plotter import (
        plot_frequency_bar, plot_distribution_bar,
        plot_cycle_duration_hist, plot_delay_bar,
        setup_plotting # Importa setup para chamar explicitamente se necessário
        )
    # Chama setup aqui para garantir que o diretório seja criado e tema aplicado
    setup_plotting()
    PLOTTING_ENABLED = True
except ImportError as e:
    logger.warning(f"Bibliotecas de plotagem (matplotlib/seaborn) não encontradas ou erro na importação: {e}. A opção --plot será ignorada.")
    PLOTTING_ENABLED = False

# Configurações do Pandas para exibição
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)


def run_pipeline(args: argparse.Namespace):
    """
    Executa o pipeline de análise da Lotofácil com base nos argumentos fornecidos.
    """
    logger.info("Iniciando a aplicação Lotofacil Analysis")

    # --- Bloco de Carga/Salvamento ---
    if args.reload:
        logger.info("Opção '--reload' ativa...")
        cleaned_data = load_and_clean_data()
        if cleaned_data is not None:
            success = save_to_db(df=cleaned_data, if_exists='replace')
            if not success: return
        else: return
    else:
        logger.info("Opção '--reload' inativa...")
        test_read = read_data_from_db(columns=['concurso'], concurso_maximo=1)
        if test_read is None or test_read.empty: return

    # Usa os argumentos parseados
    max_concurso = args.max_concurso
    should_plot = args.plot and PLOTTING_ENABLED

    # Determina quais análises rodar
    run_all = 'all' in args.analysis
    run_freq = run_all or 'freq' in args.analysis
    run_pair = run_all or 'pair' in args.analysis
    run_comb = run_all or 'comb' in args.analysis
    run_cycle = run_all or 'cycle' in args.analysis
    run_cycle_stats = run_all or 'cycle-stats' in args.analysis
    run_delay = run_all or 'delay' in args.analysis
    run_props = run_all or 'props' in args.analysis

    # --- Passo 3: Análises de Frequência ---
    if run_freq:
        logger.info(f"Iniciando análises de frequência...")
        overall_freq = calculate_period_frequency(concurso_maximo=max_concurso)
        if overall_freq is not None:
            print("\n--- Frequência Geral das Dezenas ---")
            print(overall_freq.to_string())
            if should_plot:
                plot_frequency_bar(overall_freq, "Frequência Geral das Dezenas", "freq_geral")
        # ... (loop de janelas e acumulada) ...
        logger.info("Análises de frequência concluídas.")


    # --- Passo 7: Análise de Pares ---
    if run_pair:
        # ... (código igual) ...
         logger.info("Análise de pares concluída.")


    # --- Passo 4: Análises de Combinação (Trios+) ---
    if run_comb:
        # ... (código igual) ...
        logger.info("Análises de combinação (Trios+) concluídas.")


    # --- Passo 5 e 11: Análise de Ciclos e Stats Dentro dos Ciclos ---
    cycles_summary = None
    if run_cycle or run_cycle_stats:
        logger.info(f"Iniciando identificação de ciclos...")
        cycles_summary = identify_cycles()
        if cycles_summary is None:
             logger.error("Falha na identificação de ciclos.")
        else:
            logger.info("Identificação de ciclos concluída.")

    if run_cycle and cycles_summary is not None:
        if not cycles_summary.empty:
            if max_concurso:
                cycles_summary_filtered = cycles_summary[cycles_summary['concurso_fim'] <= max_concurso].copy()
            else:
                cycles_summary_filtered = cycles_summary

            if not cycles_summary_filtered.empty:
                print("\n--- Resumo dos Ciclos Completos da Lotofácil ---")
                print(cycles_summary_filtered.to_string(index=False))
                # ... (impressão de stats min/max/avg) ...

                # *** PLOT DO HISTOGRAMA DE DURAÇÃO ***
                if should_plot:
                    plot_cycle_duration_hist(cycles_summary_filtered,
                                             "Distribuição da Duração dos Ciclos",
                                             "hist_duracao_ciclos")
            else: logger.info(f"Nenhum ciclo completo encontrado até o concurso {max_concurso}.")
        else: logger.info("Nenhum ciclo completo foi identificado nos dados.")

    if run_cycle_stats and cycles_summary is not None:
         run_cycle_frequency_analysis(cycles_summary)
         logger.info("Análise de stats por ciclo concluída.")
    elif run_cycle_stats and cycles_summary is None:
         logger.error("Não foi possível executar 'cycle-stats'.")


    # --- Passo 8: Análise de Atrasos ---
    if run_delay:
        logger.info(f"Iniciando análise de atraso atual...")
        current_delays = calculate_current_delay(concurso_maximo=max_concurso)
        if current_delays is not None:
            print("\n--- Atraso Atual das Dezenas ---")
            print(current_delays.to_string())
            # *** PLOT DAS BARRAS DE ATRASO ***
            if should_plot:
                plot_delay_bar(current_delays,
                               f"Atraso Atual das Dezenas (Ref: Concurso {max_concurso or 'Último'})",
                               "barras_atraso_atual")
        else: logger.error("Falha ao calcular o atraso atual.")
        logger.info("Análise de atraso atual concluída.")


    # --- Passo 9: Análise de Propriedades dos Números ---
    if run_props:
        logger.info(f"Iniciando análise de propriedades dos números...")
        properties_df = analyze_number_properties(concurso_maximo=max_concurso)
        if properties_df is not None and not properties_df.empty:
            prop_summaries = summarize_properties(properties_df)
            # ... (impressão das distribuições e chamadas de plot como antes) ...
            if 'par_impar' in prop_summaries:
                print("\n--- Frequência da Distribuição Pares/Ímpares ---")
                print(prop_summaries['par_impar'].to_string())
                if should_plot: plot_distribution_bar(prop_summaries['par_impar'], "Distribuição Pares/Ímpares", "dist_par_impar")
            # ... (primos e moldura/miolo) ...
        else: logger.error("Falha ao analisar as propriedades.")
        logger.info("Análise de propriedades concluída.")


    logger.info("Aplicação Lotofacil Analysis finalizada.")


if __name__ == "__main__":
    # Configuração do argparse COMPLETA
    parser = argparse.ArgumentParser(
        description="Analisa dados históricos da Lotofácil.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )

    parser.add_argument('--reload', action='store_true', help="Força recarga do Excel.")
    parser.add_argument('--max-concurso', type=int, default=None, metavar='NUMERO', help="Concurso máximo para análise.")
    parser.add_argument(
        '--analysis', nargs='+',
        choices=['freq', 'pair', 'comb', 'cycle', 'cycle-stats', 'delay', 'props', 'all'],
        default=['all'], metavar='TIPO', help="Análises a executar."
    )
    parser.add_argument('--top-n', type=int, default=15, metavar='N', help="Top N para pares/combinações.")
    parser.add_argument('--windows', type=str, default='10,25,50', metavar='W1,W2,...', help="Janelas de frequência.")
    parser.add_argument('--plot', action='store_true', help="Gera gráficos (requer matplotlib/seaborn).")

    args = parser.parse_args()

    if args.plot and not PLOTTING_ENABLED:
        logger.error("Opção --plot solicitada, mas matplotlib/seaborn não encontradas.")

    run_pipeline(args)