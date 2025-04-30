# copernicus_downloader.py

"""
Universidade Federal do Rio de Janeiro (UFRJ)
Centro de Ciências Matemáticas e da Natureza (CCMN)
Instituto de Geociências (IGEO)
Departamento de Meteorologia - Laboratório de Aplicações de Satélites Ambientais (LASA)

Descrição: Código base que realiza a busca e o download de imagens dos produtos Sentinel's (1, 2, 3, 5)
Autor: Luiz Felipe Machado Faria de Sousa
Versão: 1.0.0
"""

# Importa bibliotecas necessárias
import os  # interação com sistema operacional, manipulação de diretórios e arquivos
import requests  # requisições HTTP para comunicação com APIs
import pandas as pd  # manipulação e análise de dados estruturados (não utilizado explicitamente)
from tqdm import tqdm  # barra de progresso visual para acompanhar downloads
import time  # controle de tempo, usado para pausas entre tentativas
from eodag import EODataAccessGateway, setup_logging  # EODataAccessGateway permite buscar produtos de satélite
from dateutil import parser  # Usado para analisar datas em string para objetos datetime
import datetime  # Usado para manipulação de datas e cálculo de intervalos de tempo

# Instancia o gateway para acesso aos dados
dag = EODataAccessGateway()  

# Classe para interagir com a API do Copernicus
class CopernicusDownloader:
    # URLs para obtenção do token, informações e download de produtos
    TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    PRODUCT_INFO_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    PRODUCT_DOWNLOAD_URL = "https://zipper.dataspace.copernicus.eu/odata/v1/Products"

    # Método para buscar produtos no catálogo da API
    def search_product(self, start_date, end_date, coordinates, productType):
        """
        start_date: data inicial da busca no formato 'YYYY-MM-DD'
        end_date: data final da busca no mesmo formato
        coordinates: lista com 2 (ponto) ou 4 (retângulo) coordenadas geográficas
        productType: tipo do produto Copernicus a ser buscado (ex: 'S2_MSI_L1C')
        """
        # Verifica se as coordenadas correspondem a um ponto (2 valores)
        if len(coordinates) == 2:
            # Define os extremos como iguais ao ponto (lon/lat)
            lonmin = lonmax = coordinates[0]
            latmin = latmax = coordinates[1]
        # Verifica se é um retângulo (4 valores)
        elif len(coordinates) == 4:
            # Atribui diretamente os limites do polígono
            lonmin, latmin, lonmax, latmax = coordinates
        else:
            # Lança erro se o número de coordenadas for inválido
            raise ValueError("Lista de coordenadas inválida. Use 2 (ponto) ou 4 (polígono).")

        # Inicializa a lista de IDs de produtos encontrados
        list_ids = []

        # Loop até que a data inicial ultrapasse a final
        while parser.parse(start_date) < parser.parse(end_date):
            # Realiza a busca no gateway com os parâmetros definidos
            search_results = dag.search(
                productType=productType,
                geom={'lonmin': coordinates[0], 'latmin': coordinates[1], 'lonmax': coordinates[2], 'latmax': coordinates[3]},
                start=start_date,
                end=end_date,
                provider='creodias',
            )

            # Se nenhum resultado for retornado, interrompe a busca
            if not search_results:
                print("Nenhum novo resultado encontrado. Encerrando...")
                break

            # Itera sobre os resultados encontrados
            for scene in search_results:
                # Adiciona o título do produto à lista de IDs
                list_ids.append(scene.properties['title'])

            # Extrai a data do último produto encontrado
            last_date_str = str(search_results[-1].properties['startTimeFromAscendingNode'][0:10])

            # Trata diferentes formatos de data recebidos
            if '-' not in last_date_str:
                if len(last_date_str) == 6:
                    last_date = datetime.datetime.strptime(last_date_str, '%y%m%d')
                elif len(last_date_str) == 8:
                    last_date = datetime.datetime.strptime(last_date_str, '%Y%m%d')
                else:
                    raise ValueError("Formato de data não reconhecido")
            else:
                last_date = datetime.datetime.strptime(last_date_str, '%Y-%m-%d')

            # Avança um dia para continuar a busca no próximo loop
            start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        # Mostra a quantidade total de produtos encontrados
        print(f"Total de produtos encontrados para as coordenadas inseridas: {len(list_ids)}")
        # Retorna a lista com os títulos dos produtos
        return list_ids


    # Construtor da classe, inicializa o token como None
    def __init__(self):
        self.access_token = None


    # Método para autenticar e obter token de acesso via login/senha
    def get_access_token(self, username, password):
        """
        username: nome de usuário cadastrado
        password: senha correspondente
        """
        # Dados para requisição do token via protocolo OpenID
        data = {
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        }

        # Faz requisição POST para obter o token
        response = requests.post(self.TOKEN_URL, data=data)

        # Lança exceção se resposta for inválida
        response.raise_for_status()

        # Armazena o token de acesso obtido
        self.access_token = response.json()["access_token"]


    # Método para obter o ID do produto com base no nome
    def get_product_id(self, product_name):
        """product_name: nome exato do produto no catálogo"""

        # Requisição GET com filtro pelo nome do produto
        response = requests.get(
            f"{self.PRODUCT_INFO_URL}?$filter=Name eq '{product_name}'"
        )

        # Lança erro se houver falha na requisição
        response.raise_for_status()

        # Converte a resposta para JSON
        json_data = response.json()

        # Verifica se o produto foi encontrado
        if not json_data["value"]:
            raise ValueError(f"No product found with name {product_name}")

        # Retorna o ID do primeiro produto encontrado
        return json_data["value"][0]["Id"]


    # Método para realizar o download do produto com barra de progresso
    def download_product(self, username, password, product_name, download_directory, output_name=None):
        """
        username: login para autenticação
        password: senha de acesso
        product_name: nome do produto a ser baixado
        download_directory: pasta onde o arquivo será salvo
        output_name: nome opcional para o arquivo .zip
        """
        # Obtém o token de acesso
        self.get_access_token(username, password)

        # Busca o ID do produto com base no nome
        product_id = self.get_product_id(product_name)

        # Monta a URL de download usando o ID
        url = f"{self.PRODUCT_DOWNLOAD_URL}({product_id})/$value"

        # Define os headers com o token
        headers = {"Authorization": f"Bearer {self.access_token}"}

        # Tenta fazer o download até 3 vezes
        for attempt in range(3):
            try:
                # Requisição GET com stream para baixar o arquivo
                response = requests.get(url, headers=headers, stream=True)

                # Verifica se a resposta foi bem-sucedida
                response.raise_for_status()

                # Tamanho total do arquivo
                file_size = int(response.headers.get("content-length", 0))
                # Tamanho de cada bloco de download
                block_size = 1024

                # Define nome de saída (se não for fornecido)
                output_name = output_name or f"{product_name}.zip"
                # Caminho completo do arquivo final
                output_path = os.path.join(download_directory, output_name)

                # Abre o arquivo e inicia a barra de progresso
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
                    # Escreve os blocos baixados no arquivo
                    for chunk in response.iter_content(chunk_size=block_size):
                        if chunk:
                            file.write(chunk)
                            bar.update(len(chunk))
                # Sai do método após download bem-sucedido
                return
            except Exception as e:
                # Mensagem de erro e espera antes da próxima tentativa
                print(f"Erro na tentativa {attempt + 1} de baixar {product_name}: {e}")
                time.sleep(5)
                # Na última tentativa, lança exceção
                if attempt == 2:
                    raise


    # Método para baixar vários produtos de uma vez
    def batch_download(self, username, password, list_ids, download_directory):
        """
        username: login para autenticação
        password: senha
        list_ids: lista de nomes dos produtos a serem baixados
        download_directory: pasta onde os arquivos serão salvos
        """
        # Conta quantos arquivos foram baixados com sucesso
        download_count = 0

        # Verifica quais arquivos .zip já existem no diretório
        id_downloaded = [f[:-4] for f in os.listdir(download_directory) if f.endswith('.zip')]

        # Itera sobre todos os IDs fornecidos
        for ID in list_ids:
            # Se o produto já estiver baixado, pula
            if ID in id_downloaded:
                print(f"{ID} já foi baixado anteriormente")
                continue

            try:
                # Tenta baixar o produto
                self.download_product(username, password, ID, download_directory)
                # Incrementa contador de sucesso
                download_count += 1

            except Exception as e:
                # Mensagem de erro e tentativa de limpeza do arquivo corrompido
                print(f"{ID} falhou no download: {e}")
                file_path = os.path.join(download_directory, f"{ID}.zip")
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        print(f"Arquivo {file_path} excluído devido a erro no download.")
                    except Exception as delete_error:
                        print(f"Falha ao tentar excluir {file_path}: {delete_error}")

