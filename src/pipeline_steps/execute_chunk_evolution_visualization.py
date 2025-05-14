# src/pipeline_steps/execute_chunk_evolution_visualization.py
import logging
from pathlib import Path
from typing import Optional, Any, Dict, List # Adicionado List para DEFAULT_DEZENAS

# from src.database_manager import DatabaseManager # Para type hint
# from src.config import Config # Para type hint
from src.visualization.plotter import plot_chunk_metric_evolution
# As constantes de config são usadas diretamente pela função de plotagem ou aqui
# Se plot_chunk_metric_evolution as importa diretamente, ótimo.
# Senão, este step precisa pegá-las do config_obj e passá-las.
# Pelo código que você forneceu, este step importa as CONSTANTES do config.py
from src.config import (
    DEFAULT_CHUNK_TYPE_FOR_PLOTTING,
    DEFAULT_CHUNK_SIZE_FOR_PLOTTING,
    DEFAULT_DEZENAS_FOR_CHUNK_EVOLUTION_PLOT,
    # PLOT_DIR_CONFIG # Usaremos config.PLOT_DIR passado via shared_context
)

logger = logging.getLogger(__name__)

# RENOMEADO PARA CORRESPONDER AO __init__.py e main.py
def run_chunk_evolution_visualization_step(
    db_manager: Any, # Tipo real: DatabaseManager
    config: Any, # Tipo real: Config
    shared_context: Dict[str, Any], # Para consistência
    **kwargs
) -> bool:
    """
    Executa a visualização da evolução de métricas de chunks para configurações padrão.
    Os gráficos são salvos no diretório especificado por config.PLOT_DIR.

    Args:
        db_manager: Instância do DatabaseManager.
        config: Objeto de configuração (contém PLOT_DIR e defaults de plotagem).
        shared_context: Dicionário de contexto compartilhado (não usado diretamente aqui).
        **kwargs: Argumentos adicionais do pipeline.

    Returns:
        True se a visualização foi gerada com sucesso, False caso contrário.
    """
    step_name = "Chunk Evolution Visualization"
    logger.info(f"Iniciando etapa do pipeline: {step_name}.")
    
    try:
        output_dir_to_use = Path(config.PLOT_DIR) # Usando config.PLOT_DIR da instância config_obj
        output_dir_to_use.mkdir(parents=True, exist_ok=True)
        logger.info(f"Gráficos de evolução de chunk serão salvos em: {output_dir_to_use}")

        # Obter parâmetros de plotagem do config_obj, que por sua vez os obtém
        # das constantes de nível de módulo (que podem vir de .env)
        chunk_type_to_plot: str = config.DEFAULT_CHUNK_TYPE_FOR_PLOTTING
        chunk_size_to_plot: int = config.DEFAULT_CHUNK_SIZE_FOR_PLOTTING
        dezenas_to_plot_list: List[int] = config.DEFAULT_DEZENAS_FOR_CHUNK_EVOLUTION_PLOT
        
        # Exemplo: plotar a evolução da frequência absoluta
        # Você pode querer iterar por várias métricas ou torná-las configuráveis
        metric_to_plot = "Frequencia Absoluta" 

        logger.info(f"Gerando gráfico para: Tipo='{chunk_type_to_plot}', "
                    f"Tamanho={chunk_size_to_plot}, Métrica='{metric_to_plot}', "
                    f"Dezenas={dezenas_to_plot_list}")
        
        plot_chunk_metric_evolution(
            db_manager=db_manager,
            chunk_type=chunk_type_to_plot,
            chunk_size=chunk_size_to_plot,
            metric_to_plot=metric_to_plot, # Ou itere/configure outras métricas
            dezenas_to_plot=dezenas_to_plot_list,
            output_dir=str(output_dir_to_use)
        )
        
        # Você poderia adicionar mais chamadas a plot_chunk_metric_evolution para outras métricas aqui
        # Ex: metric_to_plot = "Atraso Medio no Bloco"
        # plot_chunk_metric_evolution(...)
        
        logger.info(f"Etapa do pipeline: {step_name} concluída com sucesso.")
        return True
    except AttributeError as e:
        logger.error(f"Erro na etapa {step_name}: Atributo de configuração ausente. Verifique seu config.py. Detalhes: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Erro ao executar a etapa {step_name}: {e}", exc_info=True)
        return False