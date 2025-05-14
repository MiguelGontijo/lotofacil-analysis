# inspect_database.py
import pandas as pd
from pathlib import Path
import sys

# Ajuste para garantir que o diretório 'src' seja encontrado
project_root = Path(__file__).resolve().parent
src_path = project_root / 'src'
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

try:
    from config import Config # Importa a CLASSE Config
    from database_manager import DatabaseManager
except ImportError as e:
    print(f"Erro ao importar módulos. Certifique-se que 'inspect_database.py' está na raiz do projeto.")
    print(f"Detalhe do erro: {e}")
    sys.exit(1)


# Configurar pandas para melhor visualização
pd.set_option('display.max_rows', 50) 
pd.set_option('display.max_columns', None) 
pd.set_option('display.width', 1000)       
pd.set_option('display.colheader_justify', 'left') 
pd.set_option('display.precision', 2) 

def inspect_db():
    cfg = Config() 
    
    # cfg.DB_PATH já é um objeto Path pela definição da classe Config.
    # Esta linha garante que estamos usando este objeto Path consistentemente.
    # CORREÇÃO AQUI: Usar o nome da variável consistentemente.
    db_path_object = Path(cfg.DB_PATH) 

    # CORRIGIDO: Usa a variável correta 'db_path_object'
    if not db_path_object.exists(): 
        print(f"Arquivo do banco de dados não encontrado em: {db_path_object}")
        print("Execute o pipeline principal (python -m src.main --run-steps all_analysis OU --force-reload) primeiro.")
        return

    print(f"Inspecionando banco de dados: {db_path_object}\n")
    # DatabaseManager espera uma string para o path
    db_mngr = DatabaseManager(db_path=str(db_path_object))

    table_names = db_mngr.get_table_names()

    if not table_names:
        print("Nenhuma tabela encontrada no banco de dados.")
        db_mngr.close() 
        return

    print("Tabelas encontradas no banco de dados:")
    for i, name in enumerate(table_names):
        print(f"{i+1}. {name}")
    
    print("\n" + "="*50 + "\n")

    while True:
        try:
            choice = input("Digite o NÚMERO da tabela para ver as primeiras linhas (ou 'sair' para terminar): ")
            if choice.lower() == 'sair':
                break
            
            table_index = int(choice) - 1
            if 0 <= table_index < len(table_names):
                selected_table_name = table_names[table_index]
                print(f"\n--- Conteúdo da tabela: {selected_table_name} ---")
                
                df = db_mngr.load_dataframe(selected_table_name) 
                
                if df is not None:
                    if not df.empty:
                        print(f"Primeiras 5 linhas de '{selected_table_name}':")
                        print(df.head())
                        print(f"\nInformações da tabela '{selected_table_name}':")
                        df.info(verbose=True, show_counts=True)
                        print(f"\nTotal de {len(df)} linhas em '{selected_table_name}'.")
                    else:
                        print(f"A tabela '{selected_table_name}' está vazia.")
                else:
                    print(f"Não foi possível carregar dados da tabela '{selected_table_name}'. Verifique os logs do console.")
                print("\n" + "="*50 + "\n")
            else:
                print("Número inválido. Tente novamente.")
        except ValueError:
            print("Entrada inválida. Por favor, digite um número ou 'sair'.")
        except Exception as e:
            print(f"Ocorreu um erro inesperado: {e}")
    
    db_mngr.close() 

if __name__ == "__main__":
    inspect_db()