# src/strategies/base_strategy.py
import abc
from typing import List, Optional, Dict, Any
import pandas as pd

# Importações de componentes do projeto (ajuste os caminhos se necessário)
# Se os arquivos estiverem em src/
from ..database_manager import DatabaseManager
from ..analysis_aggregator import AnalysisAggregator
from src.config import config_obj # Importar o config_obj global

class BaseStrategy(abc.ABC):
    """
    Classe base abstrata para todas as estratégias de seleção de dezenas.

    Atributos:
        db_manager (DatabaseManager): Instância para interagir com o banco de dados.
        config (Dict[str, Any]): Dicionário de configuração do projeto.
        analysis_aggregator (AnalysisAggregator): Instância para buscar dados agregados das análises.
        strategy_specific_params (Dict[str, Any]): Parâmetros específicos para a estratégia filha.
    """

    def __init__(self,
                 db_manager: DatabaseManager,
                 config: Dict[str, Any], # Config global do app
                 analysis_aggregator: AnalysisAggregator,
                 **strategy_params: Any): # Parâmetros específicos da estratégia
        """
        Construtor para a BaseStrategy.

        Args:
            db_manager: Gerenciador de conexão com o banco de dados.
            config: Configurações globais do projeto (geralmente o dict app_config).
            analysis_aggregator: Agregador de dados de análises.
            **strategy_params: Parâmetros adicionais específicos para a estratégia.
        """
        self.db_manager = db_manager
        self.config = config 
        self.analysis_aggregator = analysis_aggregator
        self.strategy_specific_params = strategy_params
        # _all_dezenas_list pode ser útil para as estratégias filhas
        self._all_dezenas_list = self.config.get('todas_dezenas', list(range(1, 26)))


    @abc.abstractmethod
    def get_name(self) -> str:
        """
        Retorna o nome único e identificável da estratégia.
        Este nome pode incluir valores de parâmetros chave para distinguir
        variações da mesma estratégia base.

        Returns:
            str: O nome da estratégia.
        """
        pass

    @abc.abstractmethod
    def get_description(self) -> str:
        """
        Fornece uma breve descrição da lógica da estratégia.

        Returns:
            str: A descrição da estratégia.
        """
        pass

    @abc.abstractmethod
    def generate_scores(self, latest_draw_id: Optional[int] = None) -> pd.DataFrame:
        """
        Gera pontuações para todas as dezenas com base na lógica da estratégia.

        A pontuação deve refletir o quão "promissora" uma dezena é segundo
        os critérios da estratégia. Opcionalmente, considera dados até
        o 'latest_draw_id'. Se None, usa todos os dados disponíveis (ou o mais recente).

        Args:
            latest_draw_id (Optional[int]): O ID do último concurso a ser considerado
                                             para os cálculos. Se None, o AnalysisAggregator
                                             geralmente usará o concurso mais recente no banco.

        Returns:
            pd.DataFrame: DataFrame com as colunas ['dezena', 'score', 'ranking_strategy'],
                          onde 'dezena' é o número (1-25), 'score' é a pontuação
                          atribuída pela estratégia (maior é melhor), e 'ranking_strategy'
                          é a classificação da dezena dentro desta estratégia.
                          O DataFrame deve estar ordenado por 'score' descendente.
        """
        pass

    def select_numbers(self,
                       scores_df: pd.DataFrame,
                       num_to_select: int = 15, # Padrão Lotofácil
                       selection_params: Optional[Dict[str, Any]] = None) -> List[int]:
        """
        Seleciona um conjunto de dezenas com base nos scores gerados.

        A implementação padrão seleciona as 'num_to_select' dezenas com os maiores scores.
        Estratégias filhas podem sobrescrever este método para implementar lógicas
        de seleção mais complexas (ex: garantir diversidade, aplicar filtros
        baseados em 'selection_params').

        Args:
            scores_df (pd.DataFrame): DataFrame retornado por `generate_scores`.
                                      Deve conter colunas 'dezena' e 'score'.
            num_to_select (int): Número de dezenas a serem selecionadas.
            selection_params (Optional[Dict[str, Any]]): Parâmetros adicionais para
                                                        guiar o processo de seleção.

        Returns:
            List[int]: Uma lista de 'num_to_select' dezenas selecionadas, ordenadas.
                       Retorna lista vazia se scores_df for None ou não tiver dezenas suficientes.
        """
        if scores_df is None or scores_df.empty:
            print(f"AVISO (BaseStrategy:{self.get_name()}): DataFrame de scores vazio ou None. Retornando lista vazia.")
            return []
        
        if not all(col in scores_df.columns for col in ['dezena', 'score']):
            raise ValueError("scores_df deve ser um DataFrame com colunas 'dezena' e 'score'.")
        
        # Garante que está ordenado pelo score para pegar as N melhores
        # e depois ordena as dezenas selecionadas para consistência.
        sorted_scores_df = scores_df.sort_values(by='score', ascending=False)
        
        selected_dezenas = sorted_scores_df['dezena'].head(num_to_select).tolist()
        
        # Garante que o número correto de dezenas seja retornado, mesmo que scores_df tenha menos linhas.
        # Se tiver menos, retorna o que tem.
        if len(selected_dezenas) < num_to_select:
            print(f"AVISO (BaseStrategy:{self.get_name()}): Menos de {num_to_select} dezenas disponíveis com scores. "
                  f"Retornando {len(selected_dezenas)} dezenas.")
            
        return sorted(selected_dezenas) # Retorna as dezenas selecionadas ordenadas numericamente