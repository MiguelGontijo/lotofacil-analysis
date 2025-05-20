# run_demo.py (ou integre em src/main.py)
import pandas as pd
import logging
from typing import Optional, List

# Ajuste os caminhos de importação conforme a estrutura do seu projeto
# Se este script estiver na raiz do projeto e 'src' for um pacote:
from src.database_manager import DatabaseManager
from src.analysis_aggregator import AnalysisAggregator
from src.scorer import ScorerManager
from src.config import config as app_config # Importa o dicionário de configuração

# Configuração básica do logging para vermos os outputs dos nossos componentes
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger("DemoRun")

# Configurações do Pandas para melhor visualização de DataFrames no console
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', 50)


def run_full_demo(latest_concurso_id_param: Optional[int] = None,
                  run_aggregator_test: bool = True,
                  run_scorer_test: bool = True,
                  strategies_to_test: Optional[List[str]] = None): # Lista de nomes de classes de estratégia
    """
    Executa uma demonstração do AnalysisAggregator e do ScorerManager.
    """
    logger.info("Iniciando demonstração do pipeline de agregação e scoring...")

    # 1. Inicializar dependências
    # (Assumindo que config.py define 'db_path' ou detalhes de conexão)
    db_path = app_config.get('database', {}).get('path', 'Data/lotofacil.db') # Exemplo de caminho do config
    
    db_manager = None
    analysis_aggregator = None
    scorer_manager = None

    try:
        db_manager = DatabaseManager(db_path=db_path, config=app_config)
        # db_manager.connect() # Removido se o __enter__ cuidar disso ou se as chamadas conectam sob demanda

        # O BlockAggregator é opcional no AnalysisAggregator, então não precisamos instanciá-lo aqui
        # a menos que queiramos testar especificamente essa integração.
        analysis_aggregator = AnalysisAggregator(db_manager=db_manager, config_dict=app_config)
        
        scorer_manager = ScorerManager(db_manager=db_manager,
                                       analysis_aggregator=analysis_aggregator,
                                       config_dict=app_config)

        # Determinar o latest_concurso_id para o teste
        if latest_concurso_id_param is None:
            # Se o agregador tiver um método para pegar o último ID, use-o.
            # Caso contrário, o método get_historical_metrics_for_dezenas deve tratar None.
            # latest_concurso_id_to_use = analysis_aggregator._get_latest_concurso_id_from_db() # Se quiser pegar explicitamente
            latest_concurso_id_to_use = None # Deixa o agregador resolver se for None
            logger.info("Nenhum 'latest_concurso_id' específico fornecido, o Aggregator usará o mais recente do DB.")
        else:
            latest_concurso_id_to_use = latest_concurso_id_param
            logger.info(f"Usando 'latest_concurso_id' específico: {latest_concurso_id_to_use}")


        # --- Teste do AnalysisAggregator ---
        if run_aggregator_test and analysis_aggregator:
            logger.info("\n--- Testando AnalysisAggregator ---")
            df_aggregated_metrics = analysis_aggregator.get_historical_metrics_for_dezenas(
                latest_concurso_id=latest_concurso_id_to_use
            )
            if not df_aggregated_metrics.empty:
                logger.info(f"DataFrame Agregado retornado (até concurso {latest_concurso_id_to_use if latest_concurso_id_to_use else 'mais recente'}):")
                logger.info(f"Shape: {df_aggregated_metrics.shape}")
                logger.info(f"Colunas: {df_aggregated_metrics.columns.tolist()}")
                logger.info("Primeiras 5 linhas do DataFrame Agregado:\n" + df_aggregated_metrics.head().to_string())
                
                # Verificar se as colunas esperadas pelas estratégias estão presentes
                # Exemplo para SimpleRecencyAndDelayStrategy
                # (O nome da coluna de frequência recente é construído com base no default do aggregator)
                default_window = app_config.get('aggregator_default_recent_window', 10)
                expected_freq_col = f"recent_frequency_window_{default_window}"
                
                missing_cols_for_simple_strategy = []
                if 'current_delay' not in df_aggregated_metrics.columns:
                    missing_cols_for_simple_strategy.append('current_delay')
                if expected_freq_col not in df_aggregated_metrics.columns:
                     missing_cols_for_simple_strategy.append(expected_freq_col)
                
                if missing_cols_for_simple_strategy:
                    logger.warning(f"Colunas FALTANTES para SimpleRecencyAndDelayStrategy no Aggregator: {missing_cols_for_simple_strategy}")
                else:
                    logger.info(f"Colunas OK para SimpleRecencyAndDelayStrategy ('current_delay', '{expected_freq_col}').")

            else:
                logger.warning("AnalysisAggregator retornou um DataFrame vazio.")
        
        # --- Teste do ScorerManager e Estratégias ---
        if run_scorer_test and scorer_manager:
            logger.info("\n--- Testando ScorerManager e Estratégias ---")
            available_strategy_names = scorer_manager.get_available_strategy_names()
            logger.info(f"Estratégias disponíveis descobertas pelo ScorerManager: {available_strategy_names}")

            if not available_strategy_names:
                logger.warning("Nenhuma estratégia disponível para teste.")
                return

            strategies_to_run = strategies_to_test if strategies_to_test else available_strategy_names

            for strategy_name in strategies_to_run:
                if strategy_name not in available_strategy_names:
                    logger.warning(f"Estratégia '{strategy_name}' solicitada para teste não foi descoberta. Pulando.")
                    continue

                logger.info(f"\n--- Executando Estratégia: {strategy_name} ---")
                
                # Parâmetros específicos podem ser definidos por estratégia aqui
                strategy_specific_params = {}
                if strategy_name == "SimpleRecencyAndDelayStrategy":
                    strategy_specific_params = {
                        'target_recent_window_suffix': str(app_config.get('aggregator_default_recent_window', 10)),
                        'delay_weight': 0.5,
                        'frequency_weight': 0.5
                    }
                elif strategy_name == "CombinationAndPropertiesStrategy":
                     strategy_specific_params = {
                         'candidate_pool_size': 25, # Exemplo de override
                         'max_combinations_to_evaluate': 1000 # Reduzir para demo rápida
                     }
                # Adicionar outros 'elif' para parâmetros de outras estratégias se necessário

                # 1. Testar generate_scores
                logger.info(f"Gerando scores para '{strategy_name}' (até concurso {latest_concurso_id_to_use if latest_concurso_id_to_use else 'mais recente'})...")
                scores_df = scorer_manager.generate_scores_for_strategy(
                    strategy_class_name=strategy_name,
                    latest_draw_id=latest_concurso_id_to_use,
                    strategy_specific_params=strategy_specific_params
                )
                if scores_df is not None and not scores_df.empty:
                    logger.info(f"Scores gerados por '{strategy_name}' (primeiras 5 linhas):\n" + scores_df.head().to_string())
                elif scores_df is not None and scores_df.empty:
                     logger.warning(f"'{strategy_name}' gerou um DataFrame de scores vazio (nenhuma dezena qualificada?).")
                else:
                    logger.error(f"Falha ao gerar scores para '{strategy_name}'.")
                    continue # Pula para a próxima estratégia se os scores falharem

                # 2. Testar select_numbers
                logger.info(f"Selecionando números para '{strategy_name}'...")
                selected_numbers = scorer_manager.select_numbers_for_strategy(
                    strategy_class_name=strategy_name,
                    latest_draw_id=latest_concurso_id_to_use,
                    num_to_select=15,
                    strategy_specific_params=strategy_specific_params,
                    # selection_extra_params pode ser usado se o select_numbers da estratégia o suportar
                )
                if selected_numbers:
                    logger.info(f"Dezenas Selecionadas por '{strategy_name}': {selected_numbers}")
                else:
                    logger.warning(f"Não foi possível selecionar dezenas para '{strategy_name}'.")

    except Exception as e:
        logger.error(f"Ocorreu um erro geral na demonstração: {e}", exc_info=True)
    # finally:
        # Removido o db_manager.close() daqui, pois o __exit__ do DatabaseManager
        # (se implementado e usado com 'with') ou o chamador principal cuidaria disso.
        # Se não usar 'with', o fechamento explícito é necessário no final do escopo de uso.
        # logger.info("Demonstração concluída.")


if __name__ == '__main__':
    # Para executar a demonstração:
    # Você pode definir um concurso específico para testar, ou deixar None para usar o mais recente.
    # Ex: test_concurso_id = 3000 # Um concurso que já ocorreu e tem dados de análise
    test_concurso_id = None 

    # Você pode escolher testar apenas algumas estratégias pelo nome da classe:
    # test_only_these_strategies = ["SimpleRecencyAndDelayStrategy", "TrendAndRecurrenceStrategy"]
    test_only_these_strategies = None # None para testar todas as descobertas

    run_full_demo(latest_concurso_id_param=test_concurso_id,
                  strategies_to_test=test_only_these_strategies)

    # Para salvar a saída em um arquivo CSV, você modificaria a impressão dos DataFrames:
    # if df_aggregated_metrics is not None and not df_aggregated_metrics.empty:
    #     df_aggregated_metrics.to_csv("aggregator_output.csv", index=False)
    # if scores_df is not None and not scores_df.empty:
    #     scores_df.to_csv(f"scores_{strategy_name}.csv", index=False)