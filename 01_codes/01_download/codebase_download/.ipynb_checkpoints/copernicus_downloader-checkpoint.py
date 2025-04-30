# copernicus_downloader.py

# Importa bibliotecas necessárias
import os  # interação com sistema operacional, manipulação de diretórios e arquivos
import requests  # requisições HTTP para comunicação com APIs
import pandas as pd  # manipulação e análise de dados estruturados (não utilizado explicitamente)
from tqdm import tqdm  # barra de progresso visual para acompanhar downloads
import time  # controle de tempo, usado para pausas entre tentativas
from eodag import EODataAccessGateway, setup_logging #

# Classe para interagir com a API do Copernicus
class CopernicusDownloader:
    # URLs para obtenção do token e informações de produtos da API do Copernicus
    TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    PRODUCT_INFO_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    PRODUCT_DOWNLOAD_URL = "https://zipper.dataspace.copernicus.eu/odata/v1/Products"

    # Método para buscar produtos na API dado intervalo de datas, coordenadas e tipo de produto
    def search_product(self, start_date, end_date, coordinates, productType):
        """
        start_date: string no formato 'YYYY-MM-DD' indicando a data inicial da busca
        end_date: string no formato 'YYYY-MM-DD' indicando a data final da busca
        coordinates: lista com 2 (ponto) ou 4 (polígono) valores float de coordenadas geográficas
        productType: string indicando o tipo de produto a ser buscado (ex: 'S2_MSI_L1C', 'S3_EFR')
        """
        dag = EODataAccessGateway()  # Instancia o gateway para acesso aos dados

        # Verifica o tipo de coordenadas fornecidas (ponto ou polígono)
        if len(coordinates) == 2:
            # Se for ponto, repete as coordenadas para formar um quadrado mínimo
            lonmin = lonmax = coordinates[0]
            latmin = latmax = coordinates[1]
        elif len(coordinates) == 4:
            # Se for polígono, atribui os limites diretamente
            lonmin, latmin, lonmax, latmax = coordinates
        else:
            # Erro caso coordenadas não estejam no formato correto
            raise ValueError("Lista de coordenadas inválida. Use 2 valores (ponto) ou 4 valores (polígono).")

        list_ids = []  # Inicializa a lista de IDs de produtos encontrados

        # Loop enquanto a data inicial for anterior à data final
        while parser.parse(start_date) < parser.parse(end_date):
            # Busca os produtos usando o gateway com os parâmetros fornecidos
            search_results = dag.search(
                productType=productType,
                geom={'lonmin': coordinates[0], 'latmin': coordinates[1], 'lonmax': coordinates[2], 'latmax': coordinates[3]},
                start=start_date,
                end=end_date,
                provider='creodias',
            )

            # Se não houver resultados, interrompe o loop
            if not search_results:
                print("Nenhum novo resultado encontrado. Encerrando...")
                break

            # Adiciona os títulos dos resultados encontrados à lista
            for scene in search_results:
                list_ids.append(scene.properties['title'])

            # Obtém a data do último produto encontrado
            last_date_str = str(search_results[-1].properties['startTimeFromAscendingNode'][0:10])

            # Trata diferentes formatos de data
            if '-' not in last_date_str:
                if len(last_date_str) == 6:
                    last_date = datetime.datetime.strptime(last_date_str, '%y%m%d')
                elif len(last_date_str) == 8:
                    last_date = datetime.datetime.strptime(last_date_str, '%Y%m%d')
                else:
                    raise ValueError("Formato de data não reconhecido")
            else:
                last_date = datetime.datetime.strptime(last_date_str, '%Y-%m-%d')

            # Incrementa a data inicial para a próxima busca
            start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        print(f"Total de produtos encontrados: {len(list_ids)}")  # Exibe o total de produtos encontrados
        return list_ids  # Retorna a lista de IDs encontrados

    # Construtor da classe
    def __init__(self):
        self.access_token = None  # Inicializa o token de acesso como None

    # Método para obter o token de acesso utilizando usuário e senha
    def get_access_token(self, username, password):
        """
        username: string com o nome de usuário para autenticação
        password: string com a senha correspondente
        """
        data = {  # Dados necessários para requisição do token
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        }
        response = requests.post(self.TOKEN_URL, data=data)  # Realiza a requisição para obter o token
        response.raise_for_status()  # Verifica se a resposta foi bem-sucedida
        self.access_token = response.json()["access_token"]  # Armazena o token de acesso

    # Método para obter o ID do produto dado o nome do produto
    def get_product_id(self, product_name):
        """
        product_name: string com o nome exato do produto no catálogo
        """
        response = requests.get(
            f"{self.PRODUCT_INFO_URL}?$filter=Name eq '{product_name}'"
        )
        response.raise_for_status()  # Verifica se a resposta foi bem-sucedida
        json_data = response.json()  # Obtém a resposta em formato JSON
        if not json_data["value"]:  # Verifica se o produto foi encontrado
            raise ValueError(f"No product found with name {product_name}")
        return json_data["value"][0]["Id"]  # Retorna o ID do produto

    # Método para realizar o download do produto
    def download_product(self, username, password, product_name, download_directory, output_name=None):
        """
        username: string com o nome de usuário para autenticação
        password: string com a senha do usuário
        product_name: string com o nome do produto a ser baixado
        download_directory: string com o caminho do diretório onde salvar o arquivo
        output_name: string opcional com o nome do arquivo de saída (sem .zip será adicionado)
        """
        self.get_access_token(username, password)  # Obtém o token de acesso

        product_id = self.get_product_id(product_name)  # Obtém o ID do produto
        url = f"{self.PRODUCT_DOWNLOAD_URL}({product_id})/$value"  # URL para download
        headers = {"Authorization": f"Bearer {self.access_token}"}  # Headers com autorização

        # Tentativas múltiplas em caso de falhas
        for attempt in range(3):
            try:
                response = requests.get(url, headers=headers, stream=True)  # Requisição para download
                response.raise_for_status()  # Verifica se a resposta foi bem-sucedida

                file_size = int(response.headers.get("content-length", 0))  # Tamanho do arquivo
                block_size = 1024  # Define tamanho do bloco para download

                output_name = output_name or f"{product_name}.zip"  # Nome do arquivo final
                output_path = os.path.join(download_directory, output_name)  # Caminho para salvar arquivo

                # Realiza download mostrando barra de progresso
                with open(output_path, "wb") as file, tqdm(
                    desc=f"Downloading {output_name}",
                    total=file_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} {unit}",
                    dynamic_ncols=True,
                    colour="BLUE",
                ) as bar:
                    for chunk in response.iter_content(chunk_size=block_size):
                        if chunk:
                            file.write(chunk)
                            bar.update(len(chunk))
                return
            except Exception as e:
                print(f"Erro na tentativa {attempt + 1} de baixar {product_name}: {e}")
                time.sleep(5)  # Aguarda antes de nova tentativa
                if attempt == 2:
                    raise

    # Método para baixar vários produtos em lote
    def batch_download(self, username, password, list_ids, download_directory):
        """
        username: string com o nome de usuário para autenticação
        password: string com a senha do usuário
        list_ids: lista de strings com os nomes dos produtos a serem baixados
        download_directory: string com o caminho do diretório onde salvar os arquivos
        """
        download_count = 0  # Contador de downloads concluídos
        id_downloaded = [f[:-4] for f in os.listdir(download_directory) if f.endswith('.zip')]  # Verifica arquivos já baixados

        for ID in list_ids:
            if ID in id_downloaded:  # Verifica se o arquivo já foi baixado anteriormente
                print(f"{ID} já foi baixado anteriormente")
                continue

            try:
                print(ID)
                self.download_product(username, password, ID, download_directory)  # Baixa o produto
                download_count += 1

            except Exception as e:
                print(f"{ID} falhou no download: {e}")
                file_path = os.path.join(download_directory, f"{ID}.zip")
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        print(f"Arquivo {file_path} excluído devido a erro no download.")
                    except Exception as delete_error:
                        print(f"Falha ao tentar excluir {file_path}: {delete_error}")