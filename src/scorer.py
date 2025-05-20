# src/scorer.py
import importlib
import inspect
import os
import pkgutil # Usado para uma forma mais robusta de descobrir módulos
from typing import List, Optional, Dict, Type, Any
import pandas as pd

# Importações dos nossos componentes centrais
# Ajuste os caminhos de importação conforme a estrutura exata do seu projeto
from .strategies.base_strategy import BaseStrategy
from .database_manager import DatabaseManager
from .analysis_aggregator import AnalysisAggregator
from src.config import config_obj # Importar o config_obj global

# Importar todas as classes de estratégia para que possam ser descobertas.
# Uma alternativa à descoberta dinâmica é registrar explicitamente as estratégias.
# Para descoberta dinâmica, precisamos garantir que os módulos em src/strategies sejam "visíveis".
# Vamos tentar uma descoberta baseada em pkgutil e inspect.

# Path para o diretório de estratégias (relativo a src/)
STRATEGIES_PACKAGE_PATH = os.path.join(os.path.dirname(__file__), 'strategies')
STRATEGIES_PYTHON_MODULE_PATH = 'src.strategies' # Como o Python importa (se src for raiz do PYTHONPATH)


class ScorerManager:
    """
    Gerencia e executa diferentes estratégias de pontuação e seleção de dezenas.
    Descobre automaticamente estratégias que herdam de BaseStrategy.
    """

    def __init__(self,
                 db_manager: DatabaseManager,
                 analysis_aggregator: AnalysisAggregator,
                 config_dict: Optional[Dict[str, Any]] = None): # Recebe o dict de configuração
        """
        Construtor do ScorerManager.

        Args:
            db_manager: Instância do DatabaseManager.
            analysis_aggregator: Instância do AnalysisAggregator.
            config_dict: Dicionário de configuração global do aplicativo.
        """
        self.db_manager = db_manager
        self.analysis_aggregator = analysis_aggregator
        self.config = config_dict if config_dict is not None else app_config
        
        self._strategy_classes: Dict[str, Type[BaseStrategy]] = self._discover_strategies()
        self._strategy_instances: Dict[str, BaseStrategy] = {} # Cache para instâncias com params default

        if not self._strategy_classes:
            print("AVISO (ScorerManager): Nenhuma estratégia foi descoberta. "
                  "Verifique a pasta 'src/strategies' e se as classes herdam de BaseStrategy.")
        else:
            print(f"INFO (ScorerManager): Estratégias descobertas: {list(self._strategy_classes.keys())}")

    def _discover_strategies(self) -> Dict[str, Type[BaseStrategy]]:
        """
        Descobre classes de estratégia no pacote 'src.strategies'.
        Considera apenas classes concretas que herdam de BaseStrategy.
        Usa o nome da classe como chave de registro.
        """
        discovered_strategies: Dict[str, Type[BaseStrategy]] = {}
        
        # Tentar importar o pacote de estratégias
        try:
            strategy_module_root = importlib.import_module(STRATEGIES_PYTHON_MODULE_PATH)
        except ImportError as e:
            print(f"ERRO (ScorerManager): Não foi possível importar o pacote de estratégias '{STRATEGIES_PYTHON_MODULE_PATH}': {e}")
            print("Verifique se 'src' está no PYTHONPATH ou se o caminho está correto.")
            return discovered_strategies

        # Iterar sobre os módulos dentro do pacote de estratégias
        for _, module_name, _ in pkgutil.iter_modules(strategy_module_root.__path__):
            if module_name == 'base_strategy': # Pular o arquivo da classe base
                continue
            try:
                # Montar o nome completo do módulo para importação
                full_module_path = f"{STRATEGIES_PYTHON_MODULE_PATH}.{module_name}"
                module = importlib.import_module(full_module_path)
                
                # Inspecionar membros do módulo importado
                for name, cls in inspect.getmembers(module, inspect.isclass):
                    # Verificar se é uma subclasse de BaseStrategy, não é a própria BaseStrategy,
                    # e não é uma classe abstrata (caso haja outras bases abstratas no futuro)
                    if issubclass(cls, BaseStrategy) and cls is not BaseStrategy and not inspect.isabstract(cls):
                        if name in discovered_strategies:
                            print(f"AVISO (ScorerManager): Nome de classe de estratégia '{name}' duplicado. "
                                  f"Módulo: {full_module_path}. Usando a primeira descoberta.")
                        else:
                            discovered_strategies[name] = cls # Usa o nome da classe como ID
                            # print(f"DEBUG (ScorerManager): Estratégia '{name}' descoberta em {full_module_path}")

            except ImportError as e:
                print(f"ERRO (ScorerManager): Falha ao importar o módulo de estratégia '{module_name}' de '{STRATEGIES_PYTHON_MODULE_PATH}': {e}")
        
        return discovered_strategies

    def get_available_strategy_names(self) -> List[str]:
        """
        Retorna uma lista dos nomes das classes de estratégias descobertas.
        Estes nomes são usados para instanciar e executar estratégias.
        """
        return list(self._strategy_classes.keys())

    def get_strategy_instance(self,
                              strategy_class_name: str,
                              strategy_specific_params: Optional[Dict[str, Any]] = None,
                              use_cache_if_no_specific_params: bool = True) -> Optional[BaseStrategy]:
        """
        Obtém ou cria uma instância de uma estratégia específica.

        Args:
            strategy_class_name: O nome da classe da estratégia (chave do dict _strategy_classes).
            strategy_specific_params: Parâmetros específicos para instanciar/configurar a estratégia.
                                      Se fornecidos, uma nova instância é sempre criada (ou uma existente
                                      reconfigurada, dependendo da implementação da estratégia).
            use_cache_if_no_specific_params: Se True e strategy_specific_params for None,
                                             tenta reutilizar uma instância já criada com parâmetros default.

        Returns:
            Optional[BaseStrategy]: A instância da estratégia, ou None se não encontrada/erro.
        """
        if strategy_class_name not in self._strategy_classes:
            print(f"ERRO (ScorerManager): Estratégia com nome de classe '{strategy_class_name}' não reconhecida.")
            return None

        strategy_cls = self._strategy_classes[strategy_class_name]
        
        # Se parâmetros específicos são fornecidos, não usar cache simples baseado no nome da classe.
        # Ou, a chave do cache precisaria incluir uma representação dos parâmetros.
        # Por simplicidade, se strategy_specific_params for dado, criamos nova instância.
        if strategy_specific_params or not use_cache_if_no_specific_params:
            try:
                # print(f"DEBUG (ScorerManager): Criando nova instância de '{strategy_class_name}' com params: {strategy_specific_params}")
                instance = strategy_cls(
                    db_manager=self.db_manager,
                    config=self.config, # Passa o config global
                    analysis_aggregator=self.analysis_aggregator,
                    **(strategy_specific_params or {}) # Desempacota params específicos da estratégia
                )
                return instance
            except Exception as e:
                print(f"ERRO (ScorerManager): Falha ao instanciar a estratégia '{strategy_class_name}' com params: {e}")
                return None
        
        # Usar cache se não houver parâmetros específicos e cache permitido
        if strategy_class_name not in self._strategy_instances:
            try:
                # print(f"DEBUG (ScorerManager): Criando e cacheando instância default de '{strategy_class_name}'")
                self._strategy_instances[strategy_class_name] = strategy_cls(
                    db_manager=self.db_manager,
                    config=self.config,
                    analysis_aggregator=self.analysis_aggregator
                    # **{} # Sem parâmetros específicos para a instância default cacheada
                )
            except Exception as e:
                print(f"ERRO (ScorerManager): Falha ao instanciar e cachear estratégia default '{strategy_class_name}': {e}")
                return None
        
        return self._strategy_instances.get(strategy_class_name)


    def generate_scores_for_strategy(self,
                                     strategy_class_name: str,
                                     latest_draw_id: Optional[int] = None,
                                     strategy_specific_params: Optional[Dict[str, Any]] = None
                                     ) -> Optional[pd.DataFrame]:
        """
        Gera scores para uma estratégia específica.

        Args:
            strategy_class_name: Nome da classe da estratégia.
            latest_draw_id: ID do último concurso para os cálculos.
            strategy_specific_params: Parâmetros específicos para a estratégia.

        Returns:
            Optional[pd.DataFrame]: DataFrame de scores ou None em caso de erro.
        """
        strategy_instance = self.get_strategy_instance(
            strategy_class_name, 
            strategy_specific_params,
            # Se params são dados, não faz sentido usar cache da instância default.
            # A instância criada será usada para esta chamada.
            use_cache_if_no_specific_params=(strategy_specific_params is None) 
        )
        
        if strategy_instance:
            try:
                # print(f"INFO (ScorerManager): Gerando scores para estratégia '{strategy_instance.get_name()}' (latest_draw_id={latest_draw_id})")
                return strategy_instance.generate_scores(latest_draw_id)
            except Exception as e:
                print(f"ERRO (ScorerManager): Falha ao gerar scores para a estratégia '{strategy_instance.get_name()}': {e}")
                return None
        return None

    def select_numbers_for_strategy(self,
                                    strategy_class_name: str,
                                    latest_draw_id: Optional[int] = None,
                                    num_to_select: int = 15,
                                    strategy_specific_params: Optional[Dict[str, Any]] = None,
                                    selection_extra_params: Optional[Dict[str, Any]] = None
                                    ) -> Optional[List[int]]:
        """
        Seleciona números usando uma estratégia específica.

        Args:
            strategy_class_name: Nome da classe da estratégia.
            latest_draw_id: ID do último concurso para os cálculos.
            num_to_select: Número de dezenas a selecionar.
            strategy_specific_params: Parâmetros para a instanciação da estratégia.
            selection_extra_params: Parâmetros extras para o método `select_numbers` da estratégia.

        Returns:
            Optional[List[int]]: Lista de dezenas selecionadas ou None em caso de erro.
        """
        scores_df = self.generate_scores_for_strategy(
            strategy_class_name, 
            latest_draw_id, 
            strategy_specific_params
        )
        
        if scores_df is not None:
            # Precisamos da instância da estratégia para chamar select_numbers.
            # Reutilizar a lógica de get_strategy_instance.
            strategy_instance = self.get_strategy_instance(
                strategy_class_name, 
                strategy_specific_params,
                use_cache_if_no_specific_params=(strategy_specific_params is None)
            )
            if strategy_instance:
                try:
                    # print(f"INFO (ScorerManager): Selecionando números para estratégia '{strategy_instance.get_name()}'")
                    return strategy_instance.select_numbers(
                        scores_df, 
                        num_to_select, 
                        selection_params=selection_extra_params
                    )
                except Exception as e:
                    print(f"ERRO (ScorerManager): Falha ao selecionar números para a estratégia '{strategy_instance.get_name()}': {e}")
                    return None
        return None

# Exemplo de como o ScorerManager poderia ser usado (ex: no main.py ou runner.py)
# if __name__ == '__main__':
#     # 1. Inicializar dependências (assumindo que elas existem e são configuradas)
#     # Estas são apenas instanciações placeholder para o exemplo
#     db_m = DatabaseManager(db_path='lotofacil.db', config=app_config) # app_config importado de config.py
#     # block_agg = BlockAggregator(db_manager=db_m, config=app_config) # Se usado
#     analysis_agg = AnalysisAggregator(db_manager=db_m, config_dict=app_config) # Passando config_dict

#     # Conectar ao DB se o construtor do DBManager não o fizer
#     # db_m.connect() 

#     # 2. Criar o ScorerManager
#     scorer_mgr = ScorerManager(db_manager=db_m, analysis_aggregator=analysis_agg, config_dict=app_config)

#     # 3. Ver estratégias disponíveis
#     available_strategies = scorer_mgr.get_available_strategy_names()
#     print(f"\nEstratégias Disponíveis: {available_strategies}")

#     if available_strategies:
#         # 4. Escolher uma estratégia para executar (ex: a primeira da lista)
#         # Para estratégias que usam nomes de colunas do aggregator que são construídos dinamicamente
#         # (como SimpleRecencyAndDelayStrategy com `recent_frequency_window_SUFFIX`),
#         # o sufixo (ex: "10") é passado como parte dos strategy_specific_params.
        
#         # Exemplo para SimpleRecencyAndDelayStrategy
#         strategy_name_to_run = 'SimpleRecencyAndDelayStrategy' # Use o nome exato da classe
#         if strategy_name_to_run in available_strategies:
#             print(f"\n--- Executando Estratégia: {strategy_name_to_run} ---")
#             params_for_simple_strategy = {
#                 'target_recent_window_suffix': "10", # Ex: para usar 'recent_frequency_window_10'
#                 'delay_weight': 0.6,
#                 'frequency_weight': 0.4
#             }
#             selected_numbers = scorer_mgr.select_numbers_for_strategy(
#                 strategy_class_name=strategy_name_to_run,
#                 latest_draw_id=None, # Usará o mais recente do DB
#                 num_to_select=15,
#                 strategy_specific_params=params_for_simple_strategy
#             )
#             if selected_numbers:
#                 print(f"Dezenas Selecionadas por {strategy_name_to_run}: {selected_numbers}")
#             else:
#                 print(f"Não foi possível selecionar dezenas para {strategy_name_to_run}.")
        
#         # Exemplo para TrendAndRecurrenceStrategy (sem params específicos extras no __init__ além dos defaults)
#         strategy_name_trend = 'TrendAndRecurrenceStrategy'
#         if strategy_name_trend in available_strategies:
#             print(f"\n--- Executando Estratégia: {strategy_name_trend} ---")
#             selected_numbers_trend = scorer_mgr.select_numbers_for_strategy(
#                 strategy_class_name=strategy_name_trend,
#                 latest_draw_id=None
#             )
#             if selected_numbers_trend:
#                 print(f"Dezenas Selecionadas por {strategy_name_trend}: {selected_numbers_trend}")
#             else:
#                 print(f"Não foi possível selecionar dezenas para {strategy_name_trend}.")

#     # Fechar conexão com DB se o DBManager não usar context manager no uso principal
#     # db_m.close()