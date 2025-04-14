# src/main.py

import logging
import argparse
import pandas as pd

# Importações de configuração e análise
from src.config import logger, TABLE_NAME, DATABASE_PATH
from src.data_loader import load_and_clean_data
from src.database_manager import save_to_db, read_data_from_db
# Renomeamos o import aqui para clareza, pois 'calculate_frequency' é usado dentro de cycle_analysis
from src.analysis.frequency_analysis import (
    calculate_frequency as calculate_period_frequency,
    calculate_windowed_frequency,
    calculate_cumulative_frequency_history
)
from src.analysis.combination_analysis import calculate_combination_frequency
# Importa as funções de ciclo
from src.analysis.cycle_analysis import identify_cycles, run_cycle_frequency_analysis
from src.analysis.delay_analysis import calculate_current_delay
from src.analysis.number_properties_analysis import analyze_number_properties, summarize_properties

# Importa funções de plotagem com tratamento de erro
try:
    from src.visualization.plotter import plot_frequency_bar, plot_distribution_bar
    PLOTTING_ENABLED = True
except ImportError:
    logger.warning("Bibliotecas de plotagem (matplotlib/seaborn) não encontradas. A opção --plot será ignorada.")
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
        logger.info("Opção '--reload' ativa. Recarregando do Excel e salvando no BD.")
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
        logger.info("Opção '--reload' inativa. Assumindo que os dados estão no BD.")
        test_read = read_data_from_db(columns=['concurso'], concurso_maximo=1)
        if test_read is None or test_read.empty:
             logger.error(f"Não foi possível ler dados do BD. Execute com --reload ou verifique o BD.")
             return

    # Usa os argumentos parseados
    max_concurso = args.max_concurso
    should_plot = args.plot and PLOTTING_ENABLED

    # Determina quais análises rodar
    run_all = 'all' in args.analysis
    run_freq = run_all or 'freq' in args.analysis
    run_pair = run_all or 'pair' in args.analysis
    run_comb = run_all or 'comb' in args.analysis
    run_cycle = run_all or 'cycle' in args.analysis
    run_cycle_stats = run_all or 'cycle-stats' in args.analysis # Flag para stats por ciclo
    run_delay = run_all or 'delay' in args.analysis
    run_props = run_all or 'props' in args.analysis

    # --- Passo 3: Análises de Frequência ---
    if run_freq:
        logger.info(f"Iniciando análises de frequência (até concurso {max_concurso or 'último'})...")
        # Usamos a função renomeada aqui para evitar conflito com a importada em cycle_analysis
        overall_freq = calculate_period_frequency(concurso_maximo=max_concurso)
        if overall_freq is not None:
            print("\n--- Frequência Geral das Dezenas ---")
            print(overall_freq.to_string())
            if should_plot:
                plot_frequency_bar(overall_freq, "Frequência Geral das Dezenas", "freq_geral")

        try:
            window_sizes = [int(w.strip()) for w in args.windows.split(',') if w.strip()]
        except ValueError:
            logger.error(f"Formato inválido para --windows: '{args.windows}'.")
            window_sizes = []

        for window in window_sizes:
            window_freq = calculate_windowed_frequency(window_size=window, concurso_maximo=max_concurso)
            if window_freq is not None:
                print(f"\n--- Frequência nos Últimos {window} Concursos ---")
                print(window_freq.to_string())

        cumulative_hist = calculate_cumulative_frequency_history(concurso_maximo=max_concurso)
        if cumulative_hist is not None:
            print("\n--- Histórico de Frequência Acumulada (Últimos 5 Registros) ---")
            print(cumulative_hist.tail())
        logger.info("Análises de frequência concluídas.")


    # --- Passo 7: Análise de Pares ---
    if run_pair:
        logger.info(f"Iniciando análise de pares (até concurso {max_concurso or 'último'})...")
        top_n_pairs = args.top_n
        logger.info(f"Calculando os {top_n_pairs} Pares mais frequentes...")
        top_pairs_result = calculate_combination_frequency(
            combination_size=2, top_n=top_n_pairs, concurso_maximo=max_concurso
        )
        if top_pairs_result:
            print(f"\n--- Top {top_n_pairs} Pares Mais Frequentes ---")
            for pair, count in top_pairs_result:
                pair_str = ", ".join(map(str, pair))
                print(f"Par: ({pair_str}) - Frequência: {count}")
        else: logger.warning(f"Não foram encontrados pares.")
        logger.info("Análise de pares concluída.")


    # --- Passo 4: Análises de Combinação (Trios em diante) ---
    if run_comb:
        logger.info(f"Iniciando análises de combinação (Trios+) (até concurso {max_concurso or 'último'})...")
        top_n_comb = args.top_n
        for size in [3, 4, 5, 6]:
             combo_name = {3: "Trios", 4: "Quartetos", 5: "Quintetos", 6: "Sextetos"}.get(size)
             logger.info(f"Calculando os {top_n_comb} {combo_name} mais frequentes...")
             top_combos = calculate_combination_frequency(
                 combination_size=size, top_n=top_n_comb, concurso_maximo=max_concurso
             )
             if top_combos:
                 print(f"\n--- Top {top_n_comb} {combo_name} Mais Frequentes ---")
                 for combo, count in top_combos:
                     combo_str = ", ".join(map(str, combo))
                     print(f"Combinação: ({combo_str}) - Frequência: {count}")
             else: logger.warning(f"Não foram encontradas combinações para {combo_name}.")
        logger.info("Análises de combinação (Trios+) concluídas.")


    # --- Passo 5 e 11: Análise de Ciclos e Stats Dentro dos Ciclos ---
    # Precisa identificar os ciclos primeiro, se uma das análises for solicitada
    cycles_summary = None
    if run_cycle or run_cycle_stats:
        logger.info(f"Iniciando identificação de ciclos...")
        cycles_summary = identify_cycles()
        if cycles_summary is None:
             logger.error("Falha na identificação de ciclos. Análises de ciclo e stats por ciclo abortadas.")
        else:
            logger.info("Identificação de ciclos concluída.")

    # Executa a exibição do resumo se 'cycle' foi pedido
    if run_cycle and cycles_summary is not None:
        if not cycles_summary.empty:
            if max_concurso:
                cycles_summary_filtered = cycles_summary[cycles_summary['concurso_fim'] <= max_concurso].copy()
            else:
                cycles_summary_filtered = cycles_summary

            if not cycles_summary_filtered.empty:
                print("\n--- Resumo dos Ciclos Completos da Lotofácil ---")
                print(cycles_summary_filtered.to_string(index=False))
                avg_duration = cycles_summary_filtered['duracao'].mean()
                min_duration = cycles_summary_filtered['duracao'].min()
                max_duration = cycles_summary_filtered['duracao'].max()
                print(f"\nEstatísticas dos Ciclos (considerando até concurso {max_concurso or 'último'}):")
                print(f"- Número de ciclos completos: {len(cycles_summary_filtered)}")
                print(f"- Duração Média: {avg_duration:.2f} concursos")
                print(f"- Ciclo Mais Curto: {min_duration} concursos")
                print(f"- Ciclo Mais Longo: {max_duration} concursos")
                # Plot aqui se necessário
            else:
                 logger.info(f"Nenhum ciclo completo encontrado até o concurso {max_concurso}.")
        else:
            logger.info("Nenhum ciclo completo foi identificado nos dados.")

    # Executa a análise DENTRO dos ciclos se 'cycle-stats' foi pedido
    if run_cycle_stats and cycles_summary is not None:
         run_cycle_frequency_analysis(cycles_summary) # Chama a função que calcula e imprime
         logger.info("Análise de stats por ciclo concluída.")
    elif run_cycle_stats and cycles_summary is None:
         logger.error("Não foi possível executar 'cycle-stats' pois a identificação de ciclos falhou.")


    # --- Passo 8: Análise de Atrasos ---
    if run_delay:
        logger.info(f"Iniciando análise de atraso atual (até concurso {max_concurso or 'último'})...")
        current_delays = calculate_current_delay(concurso_maximo=max_concurso)
        if current_delays is not None:
            print("\n--- Atraso Atual das Dezenas ---")
            print(current_delays.to_string())
            # Plot aqui se necessário
        else:
            logger.error("Falha ao calcular o atraso atual das dezenas.")
        logger.info("Análise de atraso atual concluída.")


    # --- Passo 9: Análise de Propriedades dos Números ---
    if run_props:
        logger.info(f"Iniciando análise de propriedades dos números (até concurso {max_concurso or 'último'})...")
        properties_df = analyze_number_properties(concurso_maximo=max_concurso)
        if properties_df is not None and not properties_df.empty:
            prop_summaries = summarize_properties(properties_df)

            if 'par_impar' in prop_summaries:
                print("\n--- Frequência da Distribuição Pares/Ímpares ---")
                print(prop_summaries['par_impar'].to_string())
                if should_plot:
                    plot_distribution_bar(prop_summaries['par_impar'], "Distribuição Pares/Ímpares", "dist_par_impar")

            if 'primos' in prop_summaries:
                print("\n--- Frequência da Quantidade de Números Primos ---")
                print(prop_summaries['primos'].to_string())
                if should_plot:
                    plot_distribution_bar(prop_summaries['primos'], "Distribuição de Qtd. Primos", "dist_primos")

            if 'moldura_miolo' in prop_summaries:
                print("\n--- Frequência da Distribuição Moldura/Miolo ---")
                print(prop_summaries['moldura_miolo'].to_string())
                if should_plot:
                    plot_distribution_bar(prop_summaries['moldura_miolo'], "Distribuição Moldura/Miolo", "dist_moldura_miolo")
        else:
            logger.error("Falha ao analisar as propriedades dos números.")
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
        '--analysis',
        nargs='+',
        choices=['freq', 'pair', 'comb', 'cycle', 'cycle-stats', 'delay', 'props', 'all'], # Adicionado 'cycle-stats'
        default=['all'],
        metavar='TIPO',
        help="Análises a executar."
    )
    parser.add_argument('--top-n', type=int, default=15, metavar='N', help="Top N para pares/combinações.")
    parser.add_argument('--windows', type=str, default='10,25,50', metavar='W1,W2,...', help="Janelas de frequência.")
    parser.add_argument('--plot', action='store_true', help="Gera gráficos (requer matplotlib/seaborn).")

    args = parser.parse_args()

    if args.plot and not PLOTTING_ENABLED:
        logger.error("Opção --plot solicitada, mas matplotlib/seaborn não encontradas.")
        # Continua sem plotar

    run_pipeline(args)