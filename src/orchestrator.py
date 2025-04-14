# src/orchestrator.py

import argparse
import logging
import pandas as pd
from typing import Optional, Callable, Set, Dict

# Importações de config e data handling
from src.config import logger
from src.data_loader import load_and_clean_data
from src.database_manager import save_to_db, read_data_from_db, get_draw_numbers

# Importa as FUNÇÕES DE EXECUÇÃO dos pipeline steps
from src.pipeline_steps.execute_frequency import execute_frequency_analysis
from src.pipeline_steps.execute_pairs import execute_pair_analysis
from src.pipeline_steps.execute_combinations import execute_combination_analysis
from src.pipeline_steps.execute_cycles import execute_cycle_identification, display_cycle_summary
from src.pipeline_steps.execute_cycle_stats import execute_cycle_stats_analysis
from src.pipeline_steps.execute_delay import execute_delay_analysis
from src.pipeline_steps.execute_max_delay import execute_max_delay_analysis
from src.pipeline_steps.execute_properties import execute_properties_analysis

# Importa o runner do backtester e ESTRATÉGIAS
from src.backtester.runner import run_backtest
from src.strategies.frequency_strategies import (
    select_most_frequent_overall, select_least_frequent_overall, select_most_frequent_recent
)
from src.strategies.delay_strategies import select_most_delayed
# Importa o agregador e o NOVO SCORER
from src.analysis_aggregator import get_consolidated_analysis
from src.scorer import calculate_scores # <<< Importa a função de score

# Verifica plotagem
try:
    from src.visualization.plotter import setup_plotting, plot_backtest_summary
    setup_plotting()
    PLOTTING_ENABLED = True
except ImportError as e: logger.warning(f"Libs plot não encontradas: {e}. Plot ignorado.") ; PLOTTING_ENABLED = False

StrategyFuncType = Callable[[int], Optional[Set[int]]]

class AnalysisOrchestrator:
    def __init__(self):
        self.logger = logger
        self.args = self._setup_parser().parse_args()
        self.plotting_available = PLOTTING_ENABLED
        self.should_plot = self.args.plot and self.plotting_available
        if self.args.plot and not self.plotting_available: self.logger.warning("Plot solicitado, mas libs não encontradas.")

    def _setup_parser(self) -> argparse.ArgumentParser:
        # (Código idêntico ao anterior)
        parser = argparse.ArgumentParser(description="Analisa/Backtest Lotofácil.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        general_group = parser.add_argument_group('Opções Gerais')
        general_group.add_argument('--reload', action='store_true', help="Força recarga do Excel.")
        analysis_group = parser.add_argument_group('Modo Análise')
        analysis_group.add_argument('--max-concurso', type=int, default=None, metavar='NUM', help="Concurso máximo para análise.")
        analysis_group.add_argument('--analysis', nargs='+', choices=['freq','pair','comb','cycle','cycle-stats','delay','max-delay','props','all'], default=None, metavar='TIPO', help="Análises a executar.")
        analysis_group.add_argument('--top-n', type=int, default=15, metavar='N', help="Top N para combinações.")
        analysis_group.add_argument('--windows', type=str, default='10,25,50', metavar='W1,W2,...', help="Janelas de frequência.")
        analysis_group.add_argument('--plot', action='store_true', help="Gera gráficos.")
        backtest_group = parser.add_argument_group('Modo Backtest')
        backtest_group.add_argument('--backtest', action='store_true', help="Ativa modo backtesting.")
        backtest_group.add_argument('--strategy', type=str, choices=['most_freq', 'least_freq', 'most_freq_recent', 'most_delayed'], default='most_freq', help="Estratégia a testar.")
        backtest_group.add_argument('--strategy-window', type=int, default=25, metavar='W', help="Janela para estratégias recentes.")
        backtest_group.add_argument('--start', type=int, default=2000, metavar='CONC_INICIO', help="Concurso inicial backtest.")
        backtest_group.add_argument('--end', type=int, default=None, metavar='CONC_FIM', help="Concurso final backtest.")
        parser.add_argument('--predict-last', action='store_true', help="Roda análises até N-1, pontua, e compara com N.")
        return parser

    def _load_or_check_data(self) -> bool:
        # (Código idêntico ao anterior)
        if self.args.reload:
            cleaned_data = load_and_clean_data();
            if cleaned_data is None: return False
            if not save_to_db(df=cleaned_data, if_exists='replace'): return False
        else:
            if read_data_from_db(columns=['concurso'], concurso_maximo=1) is None: return False
        self.logger.info("Dados do BD acessíveis.")
        return True

    def _get_last_available_contest(self) -> Optional[int]:
        # (Código idêntico ao anterior)
        df = read_data_from_db(columns=['concurso'])
        if df is not None and not df.empty:
             max_c = df['concurso'].max(); return int(max_c) if not pd.isna(max_c) else None
        return None

    def _run_analysis_mode(self):
        # (Código idêntico ao anterior)
        self.logger.info("Executando em modo Análise...")
        analyses_to_run = self.args.analysis if self.args.analysis is not None else ['all']
        run_all = 'all' in analyses_to_run; run_freq = run_all or 'freq' in analyses_to_run; run_pair = run_all or 'pair' in analyses_to_run; run_comb = run_all or 'comb' in analyses_to_run; run_cycle = run_all or 'cycle' in analyses_to_run; run_cycle_stats = run_all or 'cycle-stats' in analyses_to_run; run_delay = run_all or 'delay' in analyses_to_run; run_max_delay = run_all or 'max-delay' in analyses_to_run; run_props = run_all or 'props' in analyses_to_run
        if run_freq: execute_frequency_analysis(self.args, self.should_plot)
        if run_pair: execute_pair_analysis(self.args)
        if run_comb: execute_combination_analysis(self.args)
        cycles_summary = None
        if run_cycle or run_cycle_stats: cycles_summary = execute_cycle_identification()
        if run_cycle and cycles_summary is not None: display_cycle_summary(cycles_summary, self.args, self.should_plot)
        if run_cycle_stats and cycles_summary is not None: execute_cycle_stats_analysis(cycles_summary)
        elif run_cycle_stats and cycles_summary is None: self.logger.warning("Não executar 'cycle-stats'.")
        if run_delay: execute_delay_analysis(self.args, self.should_plot)
        if run_max_delay: execute_max_delay_analysis(self.args, self.should_plot)
        if run_props: execute_properties_analysis(self.args, self.should_plot)

    def _run_backtest_mode(self):
        # (Código idêntico ao anterior)
        self.logger.info("Executando em modo Backtest...")
        strategy_map: Dict[str, StrategyFuncType] = { 'most_freq': lambda mc: select_most_frequent_overall(mc), 'least_freq': lambda mc: select_least_frequent_overall(mc), 'most_freq_recent': lambda mc: select_most_frequent_recent(mc, window=self.args.strategy_window), 'most_delayed': lambda mc: select_most_delayed(mc) }
        selected_strategy_func = strategy_map.get(self.args.strategy)
        if selected_strategy_func is None: self.logger.error(f"Estratégia '{self.args.strategy}' não reconhecida."); return
        end_contest = self.args.end or self._get_last_available_contest()
        if end_contest is None: self.logger.error("Não foi possível determinar último concurso."); return
        start_contest = self.args.start
        if start_contest >= end_contest or start_contest <= 1: self.logger.error(f"Concurso inicial ({start_contest}) inválido."); return
        backtest_summary = run_backtest(selected_strategy_func, self.args.strategy, start_contest, end_contest)
        if backtest_summary:
            title = f"Resumo Backtest: '{self.args.strategy}' ({start_contest}-{end_contest})"
            print(f"\n--- {title} ---"); [print(f"Acertos {h}: {backtest_summary.get(h, 0)} vezes") for h in range(15, 10, -1)]; print(f"Abaixo de 11: {backtest_summary.get('<11', 0)} vezes"); [print(f"Erros: {backtest_summary['errors']} vezes") for _ in range(1) if backtest_summary.get('errors', 0) > 0]
            if self.should_plot:
                plot_filename = f"backtest_{self.args.strategy}_{start_contest}_{end_contest}"
                # Verifica se a função de plot existe antes de chamar
                global PLOTTING_ENABLED # Precisa acessar a flag global
                if PLOTTING_ENABLED and 'plot_backtest_summary' in globals() or 'plot_backtest_summary' in locals(): plot_backtest_summary(backtest_summary, title, plot_filename)
                elif PLOTTING_ENABLED: self.logger.warning("Função plot_backtest_summary não importada corretamente.")
        else: self.logger.error("Backtest não retornou resultados.")

    # --- MÉTODO ATUALIZADO ---
    def _run_predict_last_mode(self):
        """ Executa análise N-1, pontua, seleciona e compara com N """
        self.logger.info("Executando em modo Predição do Último Concurso...")
        n = self._get_last_available_contest()
        if n is None or n <= 1: self.logger.error("Concursos insuficientes."); return
        n_minus_1 = n - 1
        self.logger.info(f"Último concurso (N): {n}. Analisando até (N-1): {n_minus_1}")

        # 1. Roda o agregador de análises até N-1
        analysis_results = get_consolidated_analysis(n_minus_1)
        if analysis_results is None: self.logger.error(f"Falha ao obter análises consolidadas."); return

        # 2. Calcula os scores usando o scorer
        scores = calculate_scores(analysis_results) # Usa config padrão por enquanto
        if scores is None: self.logger.error(f"Falha ao calcular scores."); return

        # 3. Seleciona os top 15 números
        selected_numbers: Optional[Set[int]] = None
        if not scores.empty:
             # Exibe top 5 e bottom 5 scores para inspeção
             print("\n--- Pontuação Calculada (Top 5) ---")
             print(scores.head(5).to_string())
             print("\n--- Pontuação Calculada (Bottom 5) ---")
             print(scores.tail(5).to_string())

             selected_numbers = set(scores.nlargest(15).index) # Pega os 15 maiores scores
             print(f"\n--- Dezenas Selecionadas (Baseado em Score até {n_minus_1}) ---")
             print(sorted(list(selected_numbers)))
        else:
             self.logger.error("Não foi possível calcular ou rankear scores.")

        # 4. Busca o resultado REAL do último concurso (N)
        actual_numbers = get_draw_numbers(n)
        if actual_numbers is None: self.logger.error(f"Não foi possível obter resultado real do concurso {n}."); return
        print(f"\n--- Resultado Real do Concurso {n} ---")
        print(sorted(list(actual_numbers)))

        # 5. Compara e mostra acertos
        if selected_numbers is not None:
             from src.backtester.evaluator import evaluate_hits
             hits = evaluate_hits(selected_numbers, actual_numbers)
             print(f"\n--- Comparação ---")
             print(f"Acertos da seleção (Score) vs. Concurso {n}: {hits} pontos")
        else: print("\n--- Comparação ---\nNão foi possível gerar a seleção.")


    def run(self):
        """ Método principal de execução """
        self.logger.info(f"Iniciando Lotofacil Analysis - Orquestrador v3")
        if not self._load_or_check_data(): self.logger.error("Pré-requisitos não atendidos."); return

        if self.args.backtest: self._run_backtest_mode()
        elif self.args.predict_last: self._run_predict_last_mode() # Chama o novo modo
        else: self._run_analysis_mode()

        self.logger.info("Aplicação Lotofacil Analysis finalizada.")