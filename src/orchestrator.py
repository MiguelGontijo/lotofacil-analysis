# src/orchestrator.py

import argparse
import logging
import pandas as pd

# Importações de configuração e módulos de análise/plotagem
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
    from src.visualization.plotter import (
        plot_frequency_bar, plot_distribution_bar,
        plot_cycle_duration_hist, plot_delay_bar,
        setup_plotting
        )
    setup_plotting() # Configura plots ao importar
    PLOTTING_ENABLED = True
except ImportError as e:
    logger.warning(f"Bibliotecas de plotagem não encontradas ou erro: {e}. Opção --plot ignorada.")
    PLOTTING_ENABLED = False


class AnalysisOrchestrator:
    """
    Orquestra o pipeline de análise da Lotofácil, lidando com argumentos
    e chamando as funções de análise apropriadas.
    """
    def __init__(self):
        self.logger = logger # Usa o logger global configurado
        self.args = self._setup_parser().parse_args() # Parseia os argumentos na inicialização
        # Verifica se plotagem é possível logo no início
        self.plotting_available = PLOTTING_ENABLED
        if self.args.plot and not self.plotting_available:
             self.logger.error("Opção --plot solicitada, mas matplotlib/seaborn não encontradas.")
             # Decide se quer parar ou apenas continuar sem plotar. Vamos continuar.

    def _setup_parser(self) -> argparse.ArgumentParser:
        """ Configura e retorna o ArgumentParser. """
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
        return parser

    def _load_or_check_data(self) -> bool:
        """ Lida com a carga de dados ou verificação do BD. Retorna True se ok, False se falha. """
        if self.args.reload:
            self.logger.info("Opção '--reload' ativa. Recarregando do Excel e salvando no BD.")
            cleaned_data = load_and_clean_data()
            if cleaned_data is not None:
                self.logger.info("Dados carregados e limpos com sucesso do Excel.")
                success = save_to_db(df=cleaned_data, if_exists='replace')
                if not success:
                    self.logger.error("Falha ao salvar os dados no banco de dados.")
                    return False
            else:
                self.logger.error("Não foi possível carregar os dados do Excel.")
                return False
        else:
            self.logger.info("Opção '--reload' inativa. Assumindo que os dados estão no BD.")
            test_read = read_data_from_db(columns=['concurso'], concurso_maximo=1)
            if test_read is None or test_read.empty:
                 self.logger.error(f"Não foi possível ler dados do BD. Execute com --reload ou verifique o BD.")
                 return False
        return True

    def run(self):
        """ Executa o pipeline principal de análises. """
        self.logger.info("Iniciando a aplicação Lotofacil Analysis")

        if not self._load_or_check_data():
            self.logger.error("Pré-requisitos de dados não atendidos. Aplicação será encerrada.")
            return # Termina a execução se dados não estão ok

        # Argumentos agora estão em self.args
        max_concurso = self.args.max_concurso
        should_plot = self.args.plot and self.plotting_available

        # Determina quais análises rodar
        run_all = 'all' in self.args.analysis
        run_freq = run_all or 'freq' in self.args.analysis
        run_pair = run_all or 'pair' in self.args.analysis
        run_comb = run_all or 'comb' in self.args.analysis
        run_cycle = run_all or 'cycle' in self.args.analysis
        run_cycle_stats = run_all or 'cycle-stats' in self.args.analysis
        run_delay = run_all or 'delay' in self.args.analysis
        run_props = run_all or 'props' in self.args.analysis

        # --- Execução das Análises ---

        if run_freq:
            self.logger.info(f"Executando análises de frequência...")
            overall_freq = calculate_period_frequency(concurso_maximo=max_concurso)
            if overall_freq is not None:
                print("\n--- Frequência Geral das Dezenas ---")
                print(overall_freq.to_string())
                if should_plot: plot_frequency_bar(overall_freq, "Frequência Geral", "freq_geral")

            try:
                window_sizes = [int(w.strip()) for w in self.args.windows.split(',') if w.strip()]
            except ValueError: window_sizes = []
            for window in window_sizes:
                window_freq = calculate_windowed_frequency(window_size=window, concurso_maximo=max_concurso)
                if window_freq is not None:
                    print(f"\n--- Frequência nos Últimos {window} Concursos ---")
                    print(window_freq.to_string())

            cumulative_hist = calculate_cumulative_frequency_history(concurso_maximo=max_concurso)
            if cumulative_hist is not None:
                print("\n--- Histórico de Frequência Acumulada (Últimos 5 Registros) ---")
                print(cumulative_hist.tail())
            self.logger.info("Análises de frequência concluídas.")

        if run_pair:
            self.logger.info(f"Executando análise de pares...")
            top_pairs_result = calculate_combination_frequency(2, self.args.top_n, max_concurso)
            if top_pairs_result:
                print(f"\n--- Top {self.args.top_n} Pares Mais Frequentes ---")
                for pair, count in top_pairs_result: print(f"Par: ({', '.join(map(str, pair))}) - Frequência: {count}")
            self.logger.info("Análise de pares concluída.")

        if run_comb:
            self.logger.info(f"Executando análises de combinação (Trios+)...")
            for size in [3, 4, 5, 6]:
                combo_name = {3:"T", 4:"Q", 5:"QI", 6:"S"}.get(size) # Nomes curtos
                top_combos = calculate_combination_frequency(size, self.args.top_n, max_concurso)
                if top_combos:
                    print(f"\n--- Top {self.args.top_n} {combo_name} Mais Frequentes ---")
                    for combo, count in top_combos: print(f"Comb: ({', '.join(map(str, combo))}) - Freq: {count}")
            self.logger.info("Análises de combinação (Trios+) concluídas.")

        # Ciclos precisam ser identificados se 'cycle' ou 'cycle-stats' for pedido
        cycles_summary = None
        if run_cycle or run_cycle_stats:
            self.logger.info(f"Executando identificação de ciclos...")
            cycles_summary = identify_cycles()
            if cycles_summary is None: self.logger.error("Falha na identificação de ciclos.")
            else: self.logger.info("Identificação de ciclos concluída.")

        if run_cycle and cycles_summary is not None:
            if not cycles_summary.empty:
                cycles_summary_filtered = cycles_summary[cycles_summary['concurso_fim'] <= max_concurso].copy() if max_concurso else cycles_summary
                if not cycles_summary_filtered.empty:
                    print("\n--- Resumo dos Ciclos Completos ---")
                    print(cycles_summary_filtered.to_string(index=False))
                    stats = cycles_summary_filtered['duracao'].agg(['mean', 'min', 'max'])
                    print(f"\nStats Ciclos (até {max_concurso or 'último'}): {len(cycles_summary_filtered)} ciclos, Média {stats['mean']:.2f}, Min {stats['min']}, Max {stats['max']}")
                    if should_plot: plot_cycle_duration_hist(cycles_summary_filtered, "Duração dos Ciclos", "hist_duracao_ciclos")
                else: self.logger.info(f"Nenhum ciclo completo até {max_concurso}.")
            else: self.logger.info("Nenhum ciclo completo identificado.")

        if run_cycle_stats and cycles_summary is not None:
             run_cycle_frequency_analysis(cycles_summary)
             self.logger.info("Análise de stats por ciclo concluída.")
        elif run_cycle_stats and cycles_summary is None:
             self.logger.error("Não foi possível executar 'cycle-stats'.")

        if run_delay:
            self.logger.info(f"Executando análise de atraso atual...")
            current_delays = calculate_current_delay(concurso_maximo=max_concurso)
            if current_delays is not None:
                print("\n--- Atraso Atual das Dezenas ---")
                print(current_delays.to_string())
                if should_plot: plot_delay_bar(current_delays, f"Atraso Atual (Ref: {max_concurso or 'Último'})", "barras_atraso_atual")
            self.logger.info("Análise de atraso atual concluída.")

        if run_props:
            self.logger.info(f"Executando análise de propriedades...")
            properties_df = analyze_number_properties(concurso_maximo=max_concurso)
            if properties_df is not None and not properties_df.empty:
                prop_summaries = summarize_properties(properties_df)
                for key, series in prop_summaries.items():
                    title = key.replace('_', ' ').title()
                    print(f"\n--- Frequência {title} ---")
                    print(series.to_string())
                    if should_plot: plot_distribution_bar(series, title, f"dist_{key}")
            self.logger.info("Análise de propriedades concluída.")

        self.logger.info("Aplicação Lotofacil Analysis finalizada.")