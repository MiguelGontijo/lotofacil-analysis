# src/orchestrator.py

import argparse
import logging
import pandas as pd
from typing import Optional, Callable, Set, Dict

# Importações de config e data handling
from src.config import logger, TABLE_NAME # Removido DATABASE_PATH e TABLE_NAME se não usados diretamente aqui
from src.data_loader import load_and_clean_data
# Removido get_draw_numbers se só for usado pelo backtester/agregador
from src.database_manager import save_to_db, read_data_from_db, get_draw_numbers

# Importa as FUNÇÕES DE EXECUÇÃO dos módulos específicos
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
# Importa o agregador e o scorer
from src.analysis_aggregator import get_consolidated_analysis
from src.scorer import calculate_scores
# Importa a função para ATUALIZAR a tabela de ciclos
from src.analysis.cycle_analysis import update_cycles_table

# Verifica plotagem
try:
    # Importa apenas o setup e as funções realmente usadas aqui (se houver)
    # ou importa diretamente nos pipeline_steps que usam
    from src.visualization.plotter import setup_plotting #, plot_backtest_summary (Removido por enquanto)
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
        self._last_contest_in_db = None # Será carregado se necessário

    def _setup_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Analisa/Backtest Lotofácil.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        general_group = parser.add_argument_group('Opções Gerais')
        general_group.add_argument('--reload', action='store_true', help="Força recarga do Excel e reconstrói ciclos.") # Esclarecido
        general_group.add_argument('--update-cycles', action='store_true', help="Atualiza tabela 'ciclos'.")
        general_group.add_argument('--force-rebuild-cycles', action='store_true', help="Reconstrói tabela 'ciclos'.")
        analysis_group = parser.add_argument_group('Modo Análise')
        analysis_group.add_argument('--max-concurso', type=int, default=None, metavar='NUM', help="Concurso máximo para análise.")
        analysis_group.add_argument('--analysis', nargs='+', choices=['freq','pair','comb','cycle','cycle-stats','delay','max-delay','props','all'], default=None, metavar='TIPO', help="Análises a executar.")
        analysis_group.add_argument('--top-n', type=int, default=15, metavar='N', help="Top N combinações.")
        analysis_group.add_argument('--windows', type=str, default='10,25,50', metavar='W1,W2,...', help="Janelas frequência.")
        analysis_group.add_argument('--plot', action='store_true', help="Gera gráficos.")
        backtest_group = parser.add_argument_group('Modo Backtest')
        backtest_group.add_argument('--backtest', action='store_true', help="Ativa modo backtesting.")
        backtest_group.add_argument('--strategy', type=str, choices=['most_freq', 'least_freq', 'most_freq_recent', 'most_delayed', 'top_score'], default='most_freq', help="Estratégia.")
        backtest_group.add_argument('--strategy-window', type=int, default=25, metavar='W', help="Janela p/ estratégia recente.")
        backtest_group.add_argument('--start', type=int, default=None, metavar='CONC_INICIO', help="Início backtest.")
        backtest_group.add_argument('--end', type=int, default=None, metavar='CONC_FIM', help="Fim backtest.")
        parser.add_argument('--predict-last', action='store_true', help="Analisa N-1 e compara com N.")
        return parser

    def _ensure_data_loaded(self) -> bool:
        """ Garante que os dados básicos foram carregados/verificados e _last_contest_in_db está setado. """
        if self._last_contest_in_db is None: # Só carrega/verifica uma vez se necessário
             if not self._load_or_check_data():
                 self.logger.error("Falha ao carregar/verificar dados do BD.")
                 return False
        return True # Dados já foram checados ou carregados com sucesso

    def _load_or_check_data(self) -> bool:
        """ Lida com a carga inicial ou verificação do BD e atualiza _last_contest_in_db. """
        # Esta função é chamada internamente por _ensure_data_loaded ou no início de run()
        if self.args.reload:
            cleaned_data = load_and_clean_data();
            if cleaned_data is None: return False
            # Salva sorteios SEM índice automático
            if not save_to_db(df=cleaned_data, table_name=TABLE_NAME, if_exists='replace'): return False
            # Cria índice específico para sorteios aqui
            try:
                 with sqlite3.connect(DATABASE_PATH) as conn:
                     idx_name = f"idx_{TABLE_NAME}_concurso"
                     conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {TABLE_NAME} (concurso);")
                     self.logger.info(f"Índice {idx_name} criado/verificado para {TABLE_NAME}.")
            except Exception as e: self.logger.error(f"Erro ao criar índice para {TABLE_NAME}: {e}")

            self._last_contest_in_db = self._get_last_available_contest(force_read=True)
            if self._last_contest_in_db is None: return False
            self.logger.info("Forçando reconstrução da tabela de ciclos após reload...")
            update_cycles_table(force_rebuild=True) # Atualiza ciclos após reload
        else:
            if read_data_from_db(columns=['concurso'], concurso_maximo=1) is None: return False
            if self._last_contest_in_db is None: self._last_contest_in_db = self._get_last_available_contest()
            if self._last_contest_in_db is None: return False

        self.logger.info("Dados do BD acessíveis.")
        return True

    def _get_last_available_contest(self, force_read: bool = False) -> Optional[int]:
        # (Código igual anterior)
        if not force_read and hasattr(self, '_last_contest_in_db') and self._last_contest_in_db is not None: return self._last_contest_in_db
        self.logger.debug("Buscando último concurso disponível no BD...")
        df = read_data_from_db(columns=['concurso']);
        if df is not None and not df.empty: max_c = df['concurso'].max(); self._last_contest_in_db = int(max_c) if not pd.isna(max_c) else None; return self._last_contest_in_db
        self.logger.error("Não foi possível ler dados do BD."); return None

    def _run_analysis_mode(self):
        # (Código idêntico ao anterior)
        self.logger.info("Executando em modo Análise...")
        analyses_to_run = self.args.analysis if self.args.analysis is not None else ['all']
        run_all = 'all' in analyses_to_run; run_freq = run_all or 'freq' in analyses_to_run; run_pair = run_all or 'pair' in analyses_to_run; run_comb = run_all or 'comb' in analyses_to_run; run_cycle = run_all or 'cycle' in analyses_to_run; run_cycle_stats = run_all or 'cycle-stats' in analyses_to_run; run_delay = run_all or 'delay' in analyses_to_run; run_max_delay = run_all or 'max-delay' in analyses_to_run; run_props = run_all or 'props' in analyses_to_run
        if run_freq: execute_frequency_analysis(self.args, self.should_plot)
        if run_pair: execute_pair_analysis(self.args)
        if run_comb: execute_combination_analysis(self.args)
        cycles_summary = None
        if run_cycle or run_cycle_stats: cycles_summary = execute_cycle_identification(self.args) # Lê da tabela
        if run_cycle and cycles_summary is not None: display_cycle_summary(cycles_summary, self.args, self.should_plot) # Exibe
        if run_cycle_stats and cycles_summary is not None: execute_cycle_stats_analysis(cycles_summary, self.args) # Analisa/Imprime
        elif run_cycle_stats and cycles_summary is None: self.logger.warning("Não executar 'cycle-stats'.")
        if run_delay: execute_delay_analysis(self.args, self.should_plot)
        if run_max_delay: execute_max_delay_analysis(self.args, self.should_plot)
        if run_props: execute_properties_analysis(self.args, self.should_plot)

    def _run_backtest_mode(self):
        # (Código idêntico ao anterior, com correção do período e plot)
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
                plot_filename = f"backtest_{self.args.strategy}_{start_contest}_{end_contest}";
                # A verificação de plot_backtest_summary foi removida do try/except global
                try:
                    from src.visualization.plotter import plot_backtest_summary # Tenta importar aqui
                    plot_backtest_summary(backtest_summary, title, plot_filename)
                except ImportError:
                     self.logger.warning("Função plot_backtest_summary não encontrada.")
        else: self.logger.error("Backtest não retornou resultados.")

    def _run_predict_last_mode(self):
        # (Código idêntico ao anterior)
        self.logger.info("Executando Predição Último Concurso...")
        n = self._last_contest_in_db;
        if n is None or n <= 1: self.logger.error("Concursos insuficientes."); return
        n_minus_1 = n - 1; self.logger.info(f"N={n}, N-1={n_minus_1}")
        analysis_results = get_consolidated_analysis(n_minus_1);
        if analysis_results is None: self.logger.error(f"Falha análises consolidadas."); return
        scores = calculate_scores(analysis_results);
        if scores is None: self.logger.error(f"Falha calcular scores."); return
        selected_numbers: Optional[Set[int]] = None
        if not scores.empty: print("\n--- Scores (Top 5) ---"); print(scores.head(5).to_string()); print("\n--- Scores (Bottom 5) ---"); print(scores.tail(5).to_string()); selected_numbers = set(scores.nlargest(15).index); print(f"\n--- Seleção (Score V2 até {n_minus_1}) ---"); print(sorted(list(selected_numbers)))
        else: self.logger.error("Scores vazios.")
        actual_numbers = get_draw_numbers(n);
        if actual_numbers is None: self.logger.error(f"Falha resultado real {n}."); return
        print(f"\n--- Resultado Real {n} ---"); print(sorted(list(actual_numbers)))
        if selected_numbers is not None: from src.backtester.evaluator import evaluate_hits; hits = evaluate_hits(selected_numbers, actual_numbers); print(f"\n--- Comparação ---\nAcertos: {hits} pontos")
        else: print("\n--- Comparação ---\nSeleção não gerada.")


    # --- MÉTODO run() ATUALIZADO ---
    def run(self):
        """ Método principal: Atualiza ciclos OU executa modo principal """
        self.logger.info(f"Iniciando Lotofacil Analysis - Orquestrador v4")

        # Ação 1: Atualizar Tabela de Ciclos (se pedido)
        # Roda *antes* de verificar/carregar dados principais se for rebuild
        if self.args.force_rebuild_cycles:
             self.logger.info("Executando --force-rebuild-cycles...")
             # Precisa garantir que dados base existem para rebuild
             if not self._load_or_check_data():
                 self.logger.error("Não é possível reconstruir ciclos sem dados base no BD.")
                 return
             update_cycles_table(force_rebuild=True)
             self.logger.info("Reconstrução da tabela de ciclos concluída. Encerrando.")
             return # Para aqui após reconstruir

        if self.args.update_cycles:
            self.logger.info("Executando --update-cycles...")
             # Precisa garantir que dados base existem para update
            if not self._ensure_data_loaded(): # Garante que dados foram checados/carregados
                 self.logger.error("Não é possível atualizar ciclos sem dados base no BD.")
                 return
            update_cycles_table(force_rebuild=False)
            self.logger.info("Atualização da tabela de ciclos concluída. Encerrando.")
            return # Para aqui após atualizar

        # Ação 2: Executar Modos Principais (Análise, Backtest, Predict)
        # Garante que os dados estão carregados/verificados para estes modos
        if not self._ensure_data_loaded():
            self.logger.error("Pré-requisitos de dados não atendidos para modo principal. Encerrando.")
            return

        if self.args.backtest:
            self._run_backtest_mode()
        elif self.args.predict_last:
            self._run_predict_last_mode()
        elif self.args.analysis is not None: # Roda análise se explicitamente pedido
            self._run_analysis_mode()
        else:
            # Comportamento padrão se NENHUMA ação for especificada
            self.logger.info("Nenhuma ação principal especificada (--analysis, --backtest, --predict-last, --update-cycles). Executando análise padrão ('all').")
            # Define args.analysis para 'all' se estava None e nenhum outro modo foi ativo
            self.args.analysis = ['all']
            self._run_analysis_mode()


        self.logger.info("Aplicação Lotofacil Analysis finalizada.")