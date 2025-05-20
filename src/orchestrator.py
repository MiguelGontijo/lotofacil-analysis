# src/orchestrator.py
import logging
from typing import List, Dict, Callable, Any, Optional
import time
import pandas as pd # Adicionado para type check em resultado

# DatabaseManager não precisa ser importado aqui se a instância é injetada
# from src.database_manager import DatabaseManager 

logger = logging.getLogger(__name__)

class Orchestrator:
    def __init__(self, pipeline: List[Dict[str, Any]], db_manager: Any): # db_manager pode ser Any ou DatabaseManager
        self.pipeline = pipeline
        # Contexto compartilhado inicializado com dependências essenciais
        self.shared_context: Dict[str, Any] = {"db_manager": db_manager}
        # O config_obj e all_data_df serão adicionados via set_shared_context pelo main.py
        logger.info(f"Orchestrator inicializado com {len(pipeline)} etapas no pipeline.")

    def set_shared_context(self, key: str, value: Any):
        """Adiciona ou atualiza um item no contexto compartilhado."""
        self.shared_context[key] = value
        # Para DataFrames grandes, logar apenas o shape ou tipo
        if isinstance(value, pd.DataFrame):
            logger.debug(f"Contexto compartilhado: Chave='{key}', Tipo=DataFrame, Shape={value.shape if value is not None else 'None'}")
        else:
            logger.debug(f"Contexto compartilhado: Chave='{key}', Tipo='{type(value).__name__}'")

    def get_shared_context_value(self, key: str, default: Any = None) -> Any:
        """Recupera um valor do contexto compartilhado, com um default opcional."""
        return self.shared_context.get(key, default)

    def _prepare_step_arguments(self, step_config: Dict[str, Any]) -> Dict[str, Any]:
        """Prepara os argumentos para uma etapa, buscando do shared_context."""
        prepared_args: Dict[str, Any] = {}
        step_name = step_config.get("name", "Desconhecida")
        
        arg_keys_from_config = step_config.get("args", [])
        for arg_key in arg_keys_from_config:
            if arg_key not in self.shared_context:
                error_msg = (
                    f"Erro na configuração da etapa '{step_name}': "
                    f"Argumento/chave de contexto '{arg_key}' não encontrado no contexto compartilhado. "
                    f"Chaves disponíveis: {list(self.shared_context.keys())}"
                )
                logger.error(error_msg)
                raise KeyError(error_msg) # Levanta erro para parar se um arg essencial estiver faltando
            prepared_args[arg_key] = self.shared_context[arg_key]
            logger.debug(f"Para etapa '{step_name}', argumento '{arg_key}' obtido do contexto.")

        # Adiciona kwargs explícitos da configuração da etapa
        explicit_kwargs = step_config.get("kwargs", {})
        if explicit_kwargs:
            prepared_args.update(explicit_kwargs)
            logger.debug(f"Para etapa '{step_name}', kwargs explícitos adicionados: {explicit_kwargs}")
            
        return prepared_args

    def run_step(self, step_config: Dict[str, Any]) -> bool:
        """Executa uma única etapa do pipeline."""
        step_name = step_config.get("name", "Etapa Desconhecida")
        step_func: Optional[Callable] = step_config.get("func")
        step_succeeded = False # Assume falha até prova em contrário

        if not step_func or not callable(step_func):
            logger.error(f"Configuração inválida para '{step_name}': 'func' ausente ou não é uma função. Pulando.")
            return False

        logger.info(f"--- Iniciando etapa: {step_name} ---")
        start_time = time.time()
        
        try:
            step_kwargs = self._prepare_step_arguments(step_config)
            # Logar apenas chaves para não poluir com valores grandes de DataFrames
            logger.debug(f"Argumentos preparados para '{step_name}': {list(step_kwargs.keys())}")
            
            result = step_func(**step_kwargs) # Desempacota os argumentos
            
            output_key = step_config.get("output_key")
            if output_key:
                self.set_shared_context(output_key, result) # set_shared_context já lida com logging
            
            if isinstance(result, bool):
                step_succeeded = result
                if not result:
                     logger.warning(f"Etapa '{step_name}' concluída, mas retornou False (indicando falha ou condição não atendida).")
                else:
                    logger.info(f"Etapa '{step_name}' concluída com sucesso (retornou True).")
            else: # Se não retorna bool, assume sucesso se não houver exceção
                step_succeeded = True
                logger.info(f"Etapa '{step_name}' concluída (sem retorno booleano explícito, sucesso assumido).")

        except KeyError as e: # Erro já logado em _prepare_step_arguments
            logger.error(f"Falha ao executar '{step_name}': Argumento essencial não encontrado no contexto ({e}). Etapa pulada.")
            # step_succeeded já é False
        except Exception as e:
            logger.error(f"Erro inesperado ao executar a etapa '{step_name}': {e}", exc_info=True)
            # step_succeeded já é False
        finally:
            end_time = time.time()
            logger.info(f"--- Etapa '{step_name}' finalizada. Duração: {end_time - start_time:.2f} segundos. Sucesso: {step_succeeded} ---")
        return step_succeeded

    def run(self) -> None:
        """Executa todas as etapas definidas no pipeline."""
        logger.info(f"Iniciando execução do pipeline com {len(self.pipeline)} etapas.")
        total_start_time = time.time()

        if not self.pipeline:
            logger.warning("Pipeline está vazio. Nenhuma etapa para executar.")
            return

        for i, step_config in enumerate(self.pipeline):
            step_number = i + 1
            step_name = step_config.get("name", f"Etapa Anônima {step_number}")
            logger.info(f"Processando Etapa {step_number}/{len(self.pipeline)}: {step_name}")
            
            success = self.run_step(step_config)
            
            # Opção: parar o pipeline se uma etapa crítica falhar
            # if not success and step_config.get('critical', False): # Adicionar 'critical': True na config da etapa
            #     logger.error(f"Etapa crítica '{step_name}' falhou. Interrompendo o pipeline.")
            #     break 
            
            logger.info("-" * 50) 

        total_end_time = time.time()
        logger.info(f"Execução completa do pipeline finalizada. Duração total: {total_end_time - total_start_time:.2f} segundos.")