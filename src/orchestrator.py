# src/orchestrator.py

import argparse
import logging
import pandas as pd
from typing import Optional # Necessário para type hints

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
from src.analysis.delay_analysis import calculate_current_delay, calculate_max_delay
from src.analysis.number_properties_analysis import analyze_number_properties, summarize_properties

try:
    from src.visualization.plotter import (
        plot_frequency_bar, plot_distribution_bar, plot_cycle_duration_hist,
        plot_delay_bar, setup_plotting
    )
    setup_plotting()
    PLOTTING_ENABLED = True
except ImportError as e:
    logger.warning(f"Libs de plotagem não encontradas: {e}. Opção --plot ignorada.")
    PLOTTING_ENABLED = False


class AnalysisOrchestrator:
    """
    Orquestra o pipeline de análise da Lotofácil, lidando com argumentos
    e chamando as funções de análise apropriadas.
    """
    def __init__(self):
        self.logger = logger
        self.args = self._setup_parser().parse_args()
        self.plotting_available = PLOTTING_ENABLED
        self.should_plot = self.args.plot and self.plotting_available # Define se deve plotar
        if self.args.plot and not self.plotting_available:
             self.logger.error("Opção --plot solicitada, mas libs não encontradas.")

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
            choices=['freq', 'pair', 'comb', 'cycle', 'cycle-stats', 'delay', 'max-delay', 'props', 'all'],
            default=['all'], metavar='TIPO', help="Análises a executar."
        )
        parser.add_argument('--top-n', type=int, default=15, metavar='N', help="Top N para pares/combinações.")
        parser.add_argument('--windows', type=str, default='10,25,50', metavar='W1,W2,...', help="Janelas de frequência.")
        parser.add_argument('--plot', action='store_true', help="Gera gráficos.")
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
                self.logger.info(f"Dados salvos com sucesso no banco de dados: {DATABASE_PATH}")
            else:
                self.logger.error("Não foi possível carregar os dados do Excel.")
                return False
        else:
            self.logger.info("Opção '--reload' inativa. Assumindo que os dados estão no BD.")
            test_read = read_data_from_db(columns=['concurso'], concurso_maximo=1)
            if test_read is None or test_read.empty:
                 self.logger.error(f"Não foi possível ler dados do BD ({DATABASE_PATH}, tabela {TABLE_NAME}). Execute com --reload ou verifique o BD.")
                 return False
            self.logger.info("Dados do BD acessíveis.")
        return True

    # --- Métodos privados para cada tipo de análise ---

    def _execute_frequency_analysis(self):
        """ Executa todas as análises de frequência. """
        self.logger.info(f"Executando análises de frequência...")
        max_c = self.args.max_concurso
        overall_freq = calculate_period_frequency(concurso_maximo=max_c)
        if overall_freq is not None:
            print("\n--- Frequência Geral das Dezenas ---")
            print(overall_freq.to_string())
            if self.should_plot: plot_frequency_bar(overall_freq, "Frequência Geral", "freq_geral")

        try:
            window_sizes = [int(w.strip()) for w in self.args.windows.split(',') if w.strip()]
        except ValueError: window_sizes = []
        for window in window_sizes:
            window_freq = calculate_windowed_frequency(window_size=window, concurso_maximo=max_c)
            if window_freq is not None:
                print(f"\n--- Frequência nos Últimos {window} Concursos ---")
                print(window_freq.to_string())
                # if self.should_plot: plot_frequency_bar(window_freq, f"Freq. Últimos {window}", f"freq_{window}")

        cumulative_hist = calculate_cumulative_frequency_history(concurso_maximo=max_c)
        if cumulative_hist is not None:
            print("\n--- Histórico de Frequência Acumulada (Últimos 5 Registros) ---")
            print(cumulative_hist.tail())
        self.logger.info("Análises de frequência concluídas.")

    def _execute_pair_analysis(self):
        """ Executa a análise de pares. """
        self.logger.info(f"Executando análise de pares...")
        top_n = self.args.top_n
        max_c = self.args.max_concurso
        top_pairs = calculate_combination_frequency(2, top_n, max_c)
        if top_pairs:
            print(f"\n--- Top {top_n} Pares Mais Frequentes ---")
            for pair, count in top_pairs: print(f"Par: ({', '.join(map(str, pair))}) - Frequência: {count}")
        self.logger.info("Análise de pares concluída.")

    def _execute_combination_analysis(self):
        """ Executa a análise de combinações (Trios+). """
        self.logger.info(f"Executando análises de combinação (Trios+)...")
        top_n = self.args.top_n
        max_c = self.args.max_concurso
        for size in [3, 4, 5, 6]:
            combo_name = {3:"Trios", 4:"Quartetos", 5:"Quintetos", 6:"Sextetos"}.get(size)
            top_combos = calculate_combination_frequency(size, top_n, max_c)
            if top_combos:
                print(f"\n--- Top {top_n} {combo_name} Mais Frequentes ---")
                for combo, count in top_combos: print(f"Comb: ({', '.join(map(str, combo))}) - Freq: {count}")
        self.logger.info("Análises de combinação (Trios+) concluídas.")

    def _execute_cycle_identification(self) -> Optional[pd.DataFrame]:
        """ Executa a identificação de ciclos e retorna o DataFrame. """
        self.logger.info(f"Executando identificação de ciclos...")
        cycles_summary = identify_cycles()
        if cycles_summary is None: self.logger.error("Falha na identificação de ciclos.")
        else: self.logger.info("Identificação de ciclos concluída.")
        return cycles_summary

    def _display_cycle_summary(self, cycles_summary: pd.DataFrame):
        """ Exibe o resumo e estatísticas dos ciclos. """
        max_c = self.args.max_concurso
        if not cycles_summary.empty:
            cycles_filtered = cycles_summary[cycles_summary['concurso_fim'] <= max_c].copy() if max_c else cycles_summary
            if not cycles_filtered.empty:
                print("\n--- Resumo dos Ciclos Completos ---")
                print(cycles_filtered.to_string(index=False))
                stats = cycles_filtered['duracao'].agg(['mean', 'min', 'max'])
                print(f"\nStats Ciclos (até {max_c or 'último'}): {len(cycles_filtered)} ciclos, Média {stats['mean']:.2f}, Min {stats['min']}, Max {stats['max']}")
                if self.should_plot: plot_cycle_duration_hist(cycles_filtered, "Duração dos Ciclos", "hist_duracao_ciclos")
            else: self.logger.info(f"Nenhum ciclo completo encontrado até o concurso {max_c}.")
        else: self.logger.info("Nenhum ciclo completo identificado nos dados.")

    def _execute_cycle_stats_analysis(self, cycles_summary: pd.DataFrame):
        """ Executa a análise de frequência dentro dos ciclos. """
        run_cycle_frequency_analysis(cycles_summary)
        self.logger.info("Análise de stats por ciclo concluída.")

    def _execute_delay_analysis(self):
        """ Executa a análise de atraso atual. """
        self.logger.info(f"Executando análise de atraso atual...")
        max_c = self.args.max_concurso
        current_delays = calculate_current_delay(concurso_maximo=max_c)
        if current_delays is not None:
            print("\n--- Atraso Atual das Dezenas ---")
            print(current_delays.to_string())
            if self.should_plot: plot_delay_bar(current_delays, f"Atraso Atual (Ref: {max_c or 'Último'})", "barras_atraso_atual")
        self.logger.info("Análise de atraso atual concluída.")

    def _execute_max_delay_analysis(self):
        """ Executa a análise de atraso máximo histórico. """
        self.logger.info(f"Executando análise de atraso máximo histórico...")
        max_c = self.args.max_concurso
        max_delays = calculate_max_delay(concurso_maximo=max_c)
        if max_delays is not None:
            print("\n--- Atraso Máximo Histórico das Dezenas ---")
            print(max_delays.to_string())
            if self.should_plot: plot_delay_bar(max_delays, f"Atraso Máximo Histórico (até {max_c or 'Último'})", "barras_atraso_maximo")
        self.logger.info("Análise de atraso máximo concluída.")

    def _execute_properties_analysis(self):
        """ Executa a análise de propriedades dos números. """
        self.logger.info(f"Executando análise de propriedades...")
        max_c = self.args.max_concurso
        properties_df = analyze_number_properties(concurso_maximo=max_c)
        if properties_df is not None and not properties_df.empty:
            prop_summaries = summarize_properties(properties_df)
            for key, series in prop_summaries.items():
                # Corrigido para remover hífens e aplicar title case corretamente
                title = key.replace('_', ' ').replace('par impar', 'Pares/Ímpares').replace('moldura miolo', 'Moldura/Miolo').title()
                print(f"\n--- Frequência {title} ---")
                print(series.to_string())
                if self.should_plot: plot_distribution_bar(series, title, f"dist_{key}")
        self.logger.info("Análise de propriedades concluída.")

    # --- Método Principal de Execução ---

    def run(self):
        """ Executa o pipeline principal de análises. """
        self.logger.info("Iniciando a aplicação Lotofacil Analysis - Orquestrador")

        if not self._load_or_check_data():
            self.logger.error("Pré-requisitos de dados não atendidos. Encerrando.")
            return

        # Determina quais análises rodar (usa self.args)
        run_all = 'all' in self.args.analysis
        run_freq = run_all or 'freq' in self.args.analysis
        run_pair = run_all or 'pair' in self.args.analysis
        run_comb = run_all or 'comb' in self.args.analysis
        run_cycle = run_all or 'cycle' in self.args.analysis
        run_cycle_stats = run_all or 'cycle-stats' in self.args.analysis
        run_delay = run_all or 'delay' in self.args.analysis
        run_max_delay = run_all or 'max-delay' in self.args.analysis
        run_props = run_all or 'props' in self.args.analysis

        # Chama os métodos de execução específicos
        if run_freq: self._execute_frequency_analysis()
        if run_pair: self._execute_pair_analysis()
        if run_comb: self._execute_combination_analysis()

        # Lida com ciclos (identifica se necessário, depois exibe/analisa)
        cycles_summary = None
        if run_cycle or run_cycle_stats:
            cycles_summary = self._execute_cycle_identification()

        if run_cycle and cycles_summary is not None:
             self._display_cycle_summary(cycles_summary)

        if run_cycle_stats and cycles_summary is not None:
             self._execute_cycle_stats_analysis(cycles_summary)
        elif run_cycle_stats and cycles_summary is None:
             self.logger.error("Não foi possível executar 'cycle-stats' pois a identificação de ciclos falhou ou não retornou dados.")


        if run_delay: self._execute_delay_analysis()
        if run_max_delay: self._execute_max_delay_analysis()
        if run_props: self._execute_properties_analysis()

        self.logger.info("Aplicação Lotofacil Analysis finalizada.")