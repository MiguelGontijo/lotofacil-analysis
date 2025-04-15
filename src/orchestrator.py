# src/orchestrator.py

import argparse
import logging
import pandas as pd
import sqlite3
from typing import Optional, Callable, Set, Dict

# Importações de config (INCLUINDO DEFAULT_SNAPSHOT_INTERVALS)
from src.config import logger, TABLE_NAME, DATABASE_PATH, DEFAULT_SNAPSHOT_INTERVALS

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
from src.strategies.frequency_strategies import *
from src.strategies.delay_strategies import *
from src.strategies.scoring_strategies import *

# Importa o agregador, scorer e ATUALIZADORES DE TABELA
from src.analysis_aggregator import get_consolidated_analysis
from src.scorer import calculate_scores
from src.analysis.cycle_analysis import update_cycles_table
from src.table_updater import update_freq_geral_snap_table

# Verifica plotagem
try:
    from src.visualization.plotter import setup_plotting, plot_backtest_summary
    setup_plotting(); PLOTTING_ENABLED = True
except ImportError as e: logger.warning(f"Libs plot não encontradas: {e}."); PLOTTING_ENABLED = False

StrategyFuncType = Callable[[int], Optional[Set[int]]]

class AnalysisOrchestrator:
    def __init__(self):
        self.logger = logger
        self.args = self._setup_parser().parse_args()
        self.plotting_available = PLOTTING_ENABLED
        self.should_plot = self.args.plot and self.plotting_available
        if self.args.plot and not self.plotting_available: self.logger.warning("Plot solicitado, mas libs não encontradas.")
        self._last_contest_in_db = None

    def _setup_parser(self) -> argparse.ArgumentParser:
        # (Definição dos argumentos idêntica à anterior)
        parser = argparse.ArgumentParser(description="Analisa/Backtest Lotofácil.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        general_group = parser.add_argument_group('Opções Gerais e de Atualização'); general_group.add_argument('--reload', action='store_true', help="Força recarga do Excel e reconstrói tabelas."); general_group.add_argument('--update-cycles', action='store_true', help="Atualiza tabela 'ciclos'."); general_group.add_argument('--force-rebuild-cycles', action='store_true', help="Reconstrói tabela 'ciclos'."); general_group.add_argument('--update-freq-snaps', action='store_true', help="Atualiza tabela 'freq_geral_snap'."); general_group.add_argument('--force-rebuild-freq-snaps', action='store_true', help="Reconstrói tabela 'freq_geral_snap'.")
        analysis_group = parser.add_argument_group('Modo Análise'); analysis_group.add_argument('--max-concurso', type=int, default=None, metavar='NUM', help="Concurso máximo."); analysis_group.add_argument('--analysis', nargs='+', choices=['freq','pair','comb','cycle','cycle-stats','delay','max-delay','props','all'], default=None, metavar='TIPO', help="Análises a executar."); analysis_group.add_argument('--top-n', type=int, default=15, metavar='N', help="Top N combinações."); from src.config import DEFAULT_CMD_WINDOWS; analysis_group.add_argument('--windows', type=str, default=DEFAULT_CMD_WINDOWS, metavar='W1,...', help=f"Janelas freq. Padrão: {DEFAULT_CMD_WINDOWS}"); analysis_group.add_argument('--plot', action='store_true', help="Gera gráficos.")
        backtest_group = parser.add_argument_group('Modo Backtest'); backtest_group.add_argument('--backtest', action='store_true', help="Ativa backtesting."); backtest_group.add_argument('--strategy', type=str, choices=['most_freq', 'least_freq', 'most_freq_recent', 'most_delayed', 'top_score'], default='most_freq', help="Estratégia."); backtest_group.add_argument('--strategy-window', type=int, default=25, metavar='W', help="Janela p/ est. recente."); backtest_group.add_argument('--start', type=int, default=None, metavar='CONC_INICIO', help="Início backtest."); backtest_group.add_argument('--end', type=int, default=None, metavar='CONC_FIM', help="Fim backtest.")
        parser.add_argument('--predict-last', action='store_true', help="Analisa N-1 e compara com N.")
        return parser

    def _ensure_data_loaded(self) -> bool:
        if self._last_contest_in_db is None:
             if not self._load_or_check_data(): self.logger.error("Falha carregar/verificar dados."); return False
        return True

    def _load_or_check_data(self) -> bool:
        if self.args.reload:
            cleaned_data = load_and_clean_data();
            if cleaned_data is None: return False
            if not save_to_db(df=cleaned_data, table_name=TABLE_NAME, if_exists='replace'): return False
            try:
                 with sqlite3.connect(DATABASE_PATH) as conn: conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_concurso ON {TABLE_NAME} (concurso);")
            except Exception as e: self.logger.error(f"Erro criar índice {TABLE_NAME}: {e}")
            self._last_contest_in_db = self._get_last_available_contest(force_read=True)
            if self._last_contest_in_db is None: return False
            self.logger.info("Forçando reconstrução tabelas auxiliares após reload...")
            update_cycles_table(force_rebuild=True)
            update_freq_geral_snap_table(force_rebuild=True)
        else:
            if read_data_from_db(columns=['concurso'], concurso_maximo=1) is None: return False
            if self._last_contest_in_db is None: self._last_contest_in_db = self._get_last_available_contest()
            if self._last_contest_in_db is None: return False
        self.logger.info("Dados do BD acessíveis."); return True

    def _get_last_available_contest(self, force_read: bool = False) -> Optional[int]:
        if not force_read and hasattr(self, '_last_contest_in_db') and self._last_contest_in_db is not None: return self._last_contest_in_db
        df = read_data_from_db(columns=['concurso']);
        if df is not None and not df.empty: max_c = df['concurso'].max(); self._last_contest_in_db = int(max_c) if not pd.isna(max_c) else None; return self._last_contest_in_db
        self.logger.error("Não ler dados BD."); return None

    def _run_analysis_mode(self):
        # (Código idêntico ao anterior)
        self.logger.info("Executando em modo Análise...")
        analyses_to_run = self.args.analysis if self.args.analysis is not None else ['all']
        run_all = 'all' in analyses_to_run; run_freq = run_all or 'freq' in analyses_to_run; run_pair = run_all or 'pair' in analyses_to_run; run_comb = run_all or 'comb' in analyses_to_run; run_cycle = run_all or 'cycle' in analyses_to_run; run_cycle_stats = run_all or 'cycle-stats' in analyses_to_run; run_delay = run_all or 'delay' in analyses_to_run; run_max_delay = run_all or 'max-delay' in analyses_to_run; run_props = run_all or 'props' in analyses_to_run
        if run_freq: execute_frequency_analysis(self.args, self.should_plot)
        if run_pair: execute_pair_analysis(self.args)
        if run_comb: execute_combination_analysis(self.args)
        cycles_summary = None
        if run_cycle or run_cycle_stats: cycles_summary = execute_cycle_identification(self.args)
        if run_cycle and cycles_summary is not None: display_cycle_summary(cycles_summary, self.args, self.should_plot)
        if run_cycle_stats and cycles_summary is not None: execute_cycle_stats_analysis(cycles_summary, self.args)
        elif run_cycle_stats and cycles_summary is None: self.logger.warning("Não executar 'cycle-stats'.")
        if run_delay: execute_delay_analysis(self.args, self.should_plot)
        if run_max_delay: execute_max_delay_analysis(self.args, self.should_plot)
        if run_props: execute_properties_analysis(self.args, self.should_plot)

    def _run_backtest_mode(self):
        # (Código idêntico ao anterior)
        self.logger.info("Executando em modo Backtest...")
        strategy_map: Dict[str, StrategyFuncType] = { 'most_freq': lambda mc: select_most_frequent_overall(mc), 'least_freq': lambda mc: select_least_frequent_overall(mc), 'most_freq_recent': lambda mc: select_most_frequent_recent(mc, window=self.args.strategy_window), 'most_delayed': lambda mc: select_most_delayed(mc), 'top_score': lambda mc: select_top_scored(mc) }
        selected_strategy_func = strategy_map.get(self.args.strategy);
        if selected_strategy_func is None: self.logger.error(f"Estratégia '{self.args.strategy}' inválida."); return
        last_contest_db = self._last_contest_in_db;
        if last_contest_db is None: self.logger.error("Último concurso BD não determinado."); return
        end_contest = self.args.end if self.args.end is not None and self.args.end <= last_contest_db else last_contest_db
        if self.args.end is not None and self.args.end > last_contest_db: self.logger.warning(f"End {self.args.end} > último no BD ({last_contest_db}). Usando {last_contest_db}.")
        start_contest = self.args.start if self.args.start is not None else 500
        if start_contest >= end_contest or start_contest <= 1: self.logger.error(f"Período backtest inválido: {start_contest}-{end_contest}."); return
        if self.args.strategy == 'top_score' and (end_contest - start_contest > 200): self.logger.warning(f"Backtest 'top_score' LENTO para {end_contest - start_contest + 1} concursos.")
        backtest_summary = run_backtest(selected_strategy_func, self.args.strategy, start_contest, end_contest)
        if backtest_summary:
            title = f"Resumo Backtest: '{self.args.strategy}' ({start_contest}-{end_contest})"; print(f"\n--- {title} ---"); [print(f"Acertos {h}: {backtest_summary.get(h, 0)}x") for h in range(15, 10, -1)]; print(f"<11 Acertos: {backtest_summary.get('<11', 0)}x"); [print(f"Erros: {backtest_summary['errors']}x") for _ in range(1) if backtest_summary.get('errors', 0) > 0]
            if self.should_plot:
                plot_filename = f"backtest_{self.args.strategy}_{start_contest}_{end_contest}"; global PLOTTING_ENABLED
                if PLOTTING_ENABLED:
                    try: from src.visualization.plotter import plot_backtest_summary; plot_backtest_summary(backtest_summary, title, plot_filename)
                    except ImportError: self.logger.warning("Função plot_backtest_summary não encontrada.")
        else: self.logger.error("Backtest não retornou resultados.")

    def _run_predict_last_mode(self):
        # (Código idêntico ao anterior)
        self.logger.info("Executando Predição Último Concurso (Scorer V5)...")
        n = self._last_contest_in_db;
        if n is None or n <= 1: self.logger.error("Concursos insuficientes."); return
        n_minus_1 = n - 1; self.logger.info(f"N={n}, N-1={n_minus_1}")
        analysis_results = get_consolidated_analysis(n_minus_1);
        if analysis_results is None: self.logger.error(f"Falha análises consolidadas."); return
        scores = calculate_scores(analysis_results);
        if scores is None: self.logger.error(f"Falha calcular scores."); return
        selected_numbers: Optional[Set[int]] = None
        if not scores.empty: print("\n--- Pontuação Calculada V5 (Top 5) ---"); print(scores.head(5).to_string()); print("\n--- Pontuação Calculada V5 (Bottom 5) ---"); print(scores.tail(5).to_string()); selected_numbers = set(scores.nlargest(15).index); print(f"\n--- Dezenas Selecionadas (Score V5 até {n_minus_1}) ---"); print(sorted(list(selected_numbers)))
        else: self.logger.error("Scores vazios.")
        actual_numbers = get_draw_numbers(n);
        if actual_numbers is None: self.logger.error(f"Falha resultado real {n}."); return
        print(f"\n--- Resultado Real do Concurso {n} ---"); print(sorted(list(actual_numbers)))
        if selected_numbers is not None: from src.backtester.evaluator import evaluate_hits; hits = evaluate_hits(selected_numbers, actual_numbers); print(f"\n--- Comparação ---\nAcertos: {hits} pontos")
        else: print("\n--- Comparação ---\nSeleção não gerada.")

    # --- MÉTODO run() ATUALIZADO ---
    def run(self):
        """ Método principal: Atualiza tabelas OU executa modo principal """
        self.logger.info(f"Iniciando Lotofacil Analysis - Orquestrador v5")

        # Ação 1: Atualizar Tabelas Auxiliares (se pedido)
        run_update_action = False
        # Prioridade para rebuilds completos
        if self.args.force_rebuild_cycles or self.args.force_rebuild_freq_snaps:
             self.logger.info("Opção de rebuild detectada. Verificando/Carregando dados base primeiro...")
             if not self._ensure_data_loaded(): self.logger.error("Dados base necessários p/ rebuild."); return
             if self.args.force_rebuild_cycles:
                  self.logger.info("Executando --force-rebuild-cycles...")
                  update_cycles_table(force_rebuild=True)
             if self.args.force_rebuild_freq_snaps:
                  self.logger.info("Executando --force-rebuild-freq-snaps...")
                  # Usa DEFAULT_SNAPSHOT_INTERVALS importado do config
                  update_freq_geral_snap_table(intervals=DEFAULT_SNAPSHOT_INTERVALS, force_rebuild=True)
             run_update_action = True
        # Se não for rebuild, verifica updates incrementais
        elif self.args.update_cycles or self.args.update_freq_snaps:
             if not self._ensure_data_loaded(): self.logger.error("Dados base necessários p/ update."); return
             if self.args.update_cycles:
                  self.logger.info("Executando --update-cycles...")
                  update_cycles_table(force_rebuild=False)
             if self.args.update_freq_snaps:
                  self.logger.info("Executando --update-freq-snaps...")
                  # Usa DEFAULT_SNAPSHOT_INTERVALS importado do config
                  update_freq_geral_snap_table(intervals=DEFAULT_SNAPSHOT_INTERVALS, force_rebuild=False)
             run_update_action = True

        # Se alguma ação de update/rebuild foi executada, termina aqui.
        if run_update_action:
             self.logger.info("Ação de atualização de tabela auxiliar concluída. Encerrando.")
             return

        # Ação 2: Executar Modos Principais (Análise, Backtest, Predict)
        if not self._ensure_data_loaded(): self.logger.error("Pré-requisitos não atendidos."); return

        if self.args.backtest: self._run_backtest_mode()
        elif self.args.predict_last: self._run_predict_last_mode()
        elif self.args.analysis is not None: self._run_analysis_mode()
        else: self.logger.info("Nenhuma ação. Use --analysis, --backtest, --predict-last ou --update-*."); # Default removido

        self.logger.info("Aplicação Lotofacil Analysis finalizada.")