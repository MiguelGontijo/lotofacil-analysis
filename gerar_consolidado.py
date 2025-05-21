import os
import datetime

def create_consolidated_source_file(
    project_root_dir: str,
    output_file_name: str = "all_project_sources.txt",
    excluded_dirs: list[str] = None,
    excluded_file_extensions: list[str] = None,
    explicitly_excluded_files: list[str] = None
) -> None:
    """
    Varre um diretório de projeto, coleta o conteúdo de todos os arquivos .py
    e os consolida em um único arquivo de texto.

    Args:
        project_root_dir (str): O caminho para o diretório raiz do projeto.
        output_file_name (str): O nome do arquivo .txt de saída.
        excluded_dirs (list[str], optional): Lista de nomes de diretórios a serem ignorados.
                                            Padrões: ['.git', '__pycache__', 'venv', '.venv',
                                                      'Logs', 'Data', 'Plots', 'docs', 'tests_output',
                                                      '.pytest_cache', '.mypy_cache', 'build', 'dist', 'archive']
        excluded_file_extensions (list[str], optional): Lista de extensões de arquivo a serem ignoradas
                                                        (além de não ser .py). Padrão: [].
                                                        Arquivos não .py já são ignorados.
        explicitly_excluded_files (list[str], optional): Lista de nomes de arquivos específicos a serem ignorados.
                                                        Padrão: [output_file_name].
    """

    if excluded_dirs is None:
        excluded_dirs = [
            '.git', '__pycache__', 'venv', '.venv', 'Logs', 'Data', 'Plots',
            'docs', 'tests_output', '.pytest_cache', '.mypy_cache',
            'build', 'dist', 'archive', 'notebooks' # Adicionando notebooks como sugestão
        ]
    if excluded_file_extensions is None: # Note: o script já foca apenas em .py
        excluded_file_extensions = []
    if explicitly_excluded_files is None:
        explicitly_excluded_files = [output_file_name]
    else:
        if output_file_name not in explicitly_excluded_files:
            explicitly_excluded_files.append(output_file_name)

    # Normaliza o caminho do diretório raiz
    project_root_dir = os.path.abspath(project_root_dir)
    output_file_path = os.path.join(project_root_dir, output_file_name)

    print(f"Iniciando a consolidação dos arquivos .py do projeto: {project_root_dir}")
    print(f"Arquivo de saída será: {output_file_path}")
    print(f"Diretórios a serem ignorados: {excluded_dirs}")
    print(f"Arquivos explicitamente ignorados: {explicitly_excluded_files}")

    source_contents: list[tuple[str, str]] = [] # Lista para armazenar (caminho_relativo, conteudo)

    for root, dirs, files in os.walk(project_root_dir, topdown=True):
        # Remove diretórios excluídos da lista de 'dirs' para que os.walk não entre neles
        dirs[:] = [d for d in dirs if d not in excluded_dirs and not d.startswith('.')]

        for file_name in files:
            if file_name in explicitly_excluded_files:
                continue

            if file_name.endswith(".py"):
                # Verifica se alguma parte do caminho contém um diretório excluído
                # Isso é uma checagem adicional, pois dirs[:] já deveria ter lidado com isso
                current_dir_relative_path = os.path.relpath(root, project_root_dir)
                if any(excluded_dir in current_dir_relative_path.split(os.sep) for excluded_dir in excluded_dirs if excluded_dir != '.'):
                    continue

                file_path = os.path.join(root, file_name)
                relative_file_path = os.path.relpath(file_path, project_root_dir)

                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    source_contents.append((relative_file_path, content))
                    print(f"Coletado: {relative_file_path}")
                except Exception as e:
                    print(f"Erro ao ler o arquivo {file_path}: {e}")

    # Ordena os arquivos por caminho para uma ordem consistente
    source_contents.sort(key=lambda x: x[0])

    try:
        with open(output_file_path, 'w', encoding='utf-8') as outfile:
            outfile.write(f"# Arquivos fonte do projeto: {os.path.basename(project_root_dir)}\n")
            outfile.write(f"# Gerado em: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            outfile.write(f"# Total de arquivos .py consolidados: {len(source_contents)}\n")
            outfile.write("=" * 80 + "\n\n")

            for relative_path, content in source_contents:
                outfile.write("-" * 80 + "\n")
                outfile.write(f"# Arquivo: {relative_path}\n")
                outfile.write("-" * 80 + "\n")
                outfile.write(content)
                outfile.write("\n\n") # Adiciona duas linhas em branco entre os arquivos

        print(f"\nConsolidação concluída! {len(source_contents)} arquivos .py foram salvos em '{output_file_path}'.")
    except Exception as e:
        print(f"Erro ao escrever o arquivo de saída {output_file_path}: {e}")

if __name__ == "__main__":
    # --- Configuração ---
    # Coloque o caminho para a pasta raiz do seu projeto aqui
    # Exemplo: se este script (gerador.py) estiver na raiz do projeto Lotofacil_Analysis,
    # o caminho seria "."
    # Se este script estiver em uma subpasta, ajuste conforme necessário.
    project_directory = "." # Assume que o script está na raiz do projeto

    # Nome do arquivo de saída
    output_filename = "all_project_sources.txt"

    # Você pode customizar as exclusões aqui se necessário:
    custom_excluded_dirs = [
        '.git', '__pycache__', 'venv', '.venv',
        'Logs', 'Data', 'Plots',  # Pastas do seu projeto que não contêm código Python executável
        'docs', 'tests_output', '.pytest_cache', '.mypy_cache',
        'build', 'dist', 'archive', 'notebooks',
        # Adicione outras pastas que você queira ignorar
        # Ex: 'old_code', 'experimental'
    ]
    # Arquivos não .py já são ignorados, então esta lista é para extensões
    # que você queira LOGAR que foram ignoradas, mas o script atual só pega .py
    custom_excluded_extensions = ['.csv', '.db', '.pkl', '.md', '.xlsx', '.log', '.ipynb']

    # Arquivos específicos para excluir (além do próprio arquivo de saída)
    custom_explicitly_excluded_files = [
        output_filename,
        # "nome_de_outro_arquivo_especifico.py" # se houver
    ]

    create_consolidated_source_file(
        project_root_dir=project_directory,
        output_file_name=output_filename,
        excluded_dirs=custom_excluded_dirs,
        # excluded_file_extensions=custom_excluded_extensions, # Descomente se quiser usar
        explicitly_excluded_files=custom_explicitly_excluded_files
    )