# inspect_database.py
import pandas as pd
from pathlib import Path

# Ajuste os imports para encontrar seus módulos src.config e src.database_manager
# Se executar este script da raiz do projeto, você pode precisar adicionar src ao sys.path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent / 'src'))

try:
    from config import DATA_DIR, DB_FILE_NAME
    from database_manager import DatabaseManager
except ImportError as e:
    print(f"Erro ao importar módulos. Certifique-se que está executando da raiz do projeto e que src está acessível: {e}")
    sys.exit(1)


# Configurar pandas para melhor visualização
pd.set_option('display.max_rows', 50) # Mostrar até 50 linhas
pd.set_option('display.max_columns', 30) # Mostrar até 30 colunas
pd.set_option('display.width', 150) # Largura do display

def inspect_db():
    db_filepath = DATA_DIR / DB_FILE_NAME
    
    if not db_filepath.exists():
        print(f"Arquivo do banco de dados não encontrado em: {db_filepath}")
        print("Execute o pipeline principal (python -m src.main) primeiro para gerar o banco de dados e as tabelas.")
        return

    print(f"Inspecionando banco de dados: {db_filepath}\n")
    db_mngr = DatabaseManager(db_path=str(db_filepath))

    table_names = db_mngr.get_table_names()

    if not table_names:
        print("Nenhuma tabela encontrada no banco de dados.")
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
                df = db_mngr.load_dataframe_from_db(selected_table_name)
                if df is not None:
                    if not df.empty:
                        print(f"Primeiras 5 linhas de '{selected_table_name}':")
                        print(df.head())
                        print(f"\nInformações da tabela '{selected_table_name}':")
                        df.info()
                        print(f"\nTotal de {len(df)} linhas em '{selected_table_name}'.")
                    else:
                        print(f"A tabela '{selected_table_name}' está vazia.")
                else:
                    # load_dataframe_from_db já loga um aviso se a tabela não existe,
                    # mas isso não deveria acontecer aqui pois estamos iterando sobre table_names.
                    # Isso pode acontecer se houver um erro ao ler a tabela.
                    print(f"Não foi possível carregar dados da tabela '{selected_table_name}'. Verifique os logs.")
                print("\n" + "="*50 + "\n")
            else:
                print("Número inválido. Tente novamente.")
        except ValueError:
            print("Entrada inválida. Por favor, digite um número ou 'sair'.")
        except Exception as e:
            print(f"Ocorreu um erro: {e}")

if __name__ == "__main__":
    inspect_db()