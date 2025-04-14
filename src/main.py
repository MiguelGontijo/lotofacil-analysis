# src/main.py

# Importa apenas o necessário para iniciar o orquestrador
from src.orchestrator import AnalysisOrchestrator
import logging # Ainda podemos querer configurar logging básico aqui se necessário

if __name__ == "__main__":
    # Configuração básica de logging (opcional, pode ser feita no config.py ou orchestrator.py)
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Cria a instância do orquestrador
    orchestrator = AnalysisOrchestrator()

    # Executa o pipeline através do orquestrador
    orchestrator.run()