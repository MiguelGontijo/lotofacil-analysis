# src/orchestrator.py
import logging
from typing import List, Dict, Callable, Any, Optional
import time # Para medir o tempo de execução das etapas

from src.database_manager import DatabaseManager # Importação correta

# Não é necessário importar nada de src.config aqui para o logger ou DB_PATH
# Cada módulo deve configurar seu próprio logger.

logger = logging.getLogger(__name__) # Logger padrão para este módulo

class Orchestrator:
    """
    Orquestra a execução de um pipeline de análise definido por uma lista de configurações de etapas.
    Cada etapa é uma função que pode requerer argumentos do contexto compartilhado.
    """
    def __init__(self, pipeline: List[Dict[str, Any]], db_manager: DatabaseManager):
        """
        Inicializa o Orchestrator.

        Args:
            pipeline: Uma lista de dicionários, onde cada dicionário configura uma etapa.
                      Formato esperado da configuração da etapa:
                      {
                          "name": "Nome da Etapa (para logging)",
                          "func": funcao_da_etapa (Callable),
                          "args": ["chave_contexto1", "chave_contexto2"], # Nomes das chaves no shared_context
                          "kwargs": {"param_explicito": valor} # kwargs diretos para a função
                      }
            db_manager: Instância do DatabaseManager, que será adicionada ao contexto compartilhado.
        """
        self.pipeline = pipeline
        # self.db_manager = db_manager # db_manager já está no shared_context
        self.shared_context: Dict[str, Any] = {"db_manager": db_manager}
        logger.info(f"Orchestrator inicializado com {len(pipeline)} etapas no pipeline.")

    def set_shared_context(self, key: str, value: Any):
        """
        Adiciona ou atualiza um item no contexto compartilhado entre as etapas do pipeline.

        Args:
            key: Chave para o item no contexto.
            value: Valor do item a ser armazenado.
        """
        self.shared_context[key] = value
        logger.debug(f"Contexto compartilhado atualizado: Chave='{key}', Tipo do Valor='{type(value).__name__}'")

    def _prepare_step_arguments(self, step_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepara os argumentos posicionais e nomeados para uma etapa do pipeline,
        buscando valores do contexto compartilhado conforme definido em 'args'.

        Args:
            step_config: Dicionário de configuração da etapa.

        Returns:
            Dicionário de argumentos nomeados para serem passados para a função da etapa.
        
        Raises:
            KeyError: Se uma chave especificada em 'args' não for encontrada no shared_context.
        """
        prepared_args: Dict[str, Any] = {}
        
        # Argumentos posicionais (passados como nomeados baseados nos nomes das chaves)
        # A função da etapa deve aceitar estes como argumentos nomeados.
        arg_names_from_context = step_config.get("args", [])
        for arg_name in arg_names_from_context:
            if arg_name not in self.shared_context:
                error_msg = (
                    f"Erro na configuração da etapa '{step_config.get('name', 'Desconhecida')}': "
                    f"Argumento/chave de contexto '{arg_name}' não encontrado no contexto compartilhado. "
                    f"Chaves disponíveis: {list(self.shared_context.keys())}"
                )
                logger.error(error_msg)
                raise KeyError(error_msg)
            prepared_args[arg_name] = self.shared_context[arg_name]
            logger.debug(f"Para etapa '{step_config.get('name', 'Desconhecida')}', argumento '{arg_name}' obtido do contexto.")

        # Argumentos nomeados explícitos (kwargs)
        explicit_kwargs = step_config.get("kwargs", {})
        prepared_args.update(explicit_kwargs) # kwargs explícitos podem sobrescrever os do contexto se tiverem o mesmo nome
        if explicit_kwargs:
            logger.debug(f"Para etapa '{step_config.get('name', 'Desconhecida')}', kwargs explícitos adicionados: {explicit_kwargs}")
            
        return prepared_args

    def run_step(self, step_config: Dict[str, Any]):
        """
        Executa uma única etapa do pipeline.

        Args:
            step_config: Dicionário de configuração da etapa.
        """
        step_name = step_config.get("name", "Etapa Desconhecida")
        step_func: Optional[Callable] = step_config.get("func")

        if not step_func or not callable(step_func):
            logger.error(f"Configuração inválida para a etapa '{step_name}': 'func' não definida ou não é uma função. Pulando etapa.")
            return

        logger.info(f"--- Iniciando etapa: {step_name} ---")
        start_time = time.time()
        
        try:
            step_kwargs = self._prepare_step_arguments(step_config)
            logger.debug(f"Argumentos preparados para '{step_name}': {list(step_kwargs.keys())}") # Log apenas das chaves por brevidade
            
            result = step_func(**step_kwargs) # Desempacota os argumentos preparados
            
            # Opcional: Adicionar resultado ao contexto compartilhado se a etapa retornar algo
            # e se houver uma configuração para isso (ex: "output_key": "nome_da_chave_no_contexto")
            output_key = step_config.get("output_key")
            if output_key:
                self.set_shared_context(output_key, result)
                logger.info(f"Resultado da etapa '{step_name}' salvo no contexto como '{output_key}'.")
            
            if isinstance(result, bool) and not result:
                 logger.warning(f"Etapa '{step_name}' concluída, mas retornou False (indicando possível falha parcial ou condição não atendida).")
            else:
                logger.info(f"Etapa '{step_name}' concluída com sucesso.")

        except KeyError as e: # Erro já logado em _prepare_step_arguments
            logger.error(f"Não foi possível executar a etapa '{step_name}' devido a argumento faltante no contexto: {e}. Pulando etapa.")
        except Exception as e:
            logger.error(f"Erro ao executar a etapa '{step_name}': {e}", exc_info=True)
            # Decide se o pipeline deve parar em caso de erro ou continuar
            # Por enquanto, continua com as próximas etapas.
        finally:
            end_time = time.time()
            logger.info(f"--- Etapa '{step_name}' finalizada. Duração: {end_time - start_time:.2f} segundos ---")


    def run(self):
        """
        Executa todas as etapas definidas no pipeline na sequência.
        """
        logger.info(f"Iniciando execução do pipeline com {len(self.pipeline)} etapas.")
        total_start_time = time.time()

        if not self.pipeline:
            logger.warning("Pipeline está vazio. Nenhuma etapa para executar.")
            return

        for i, step_config in enumerate(self.pipeline):
            step_number = i + 1
            step_name = step_config.get("name", f"Etapa Anônima {step_number}")
            logger.info(f"Processando Etapa {step_number}/{len(self.pipeline)}: {step_name}")
            self.run_step(step_config)
            logger.info("-" * 50) # Separador visual entre etapas

        total_end_time = time.time()
        logger.info(f"Execução completa do pipeline finalizada. Duração total: {total_end_time - total_start_time:.2f} segundos.")