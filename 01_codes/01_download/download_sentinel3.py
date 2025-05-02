"""
Universidade Federal do Rio de Janeiro (UFRJ)
Centro de Ciências Matemáticas e da Natureza (CCMN)
Instituto de Geociências (IGEO)
Departamento de Meteorologia - Laboratório de Aplicações de Satélites Ambientais (LASA)

Descrição: Rotina de download para imagens do sensor Ocean and Land Colour Instrument (OLCI) a bordo do satélite Sentinel-3.

Metodologia: A procura da cena a partir de um ponto ou polígono é realizada através da biblioteca Eodag (Earth Observation Data Access Gateway).
Para cada cena encontrada, o nome da identificação da cena é salvo em uma lista. A partir desta lista, o download das cenas é realizado 
por meio da API fornecida pela plataforma Copernicus, sob administração da ESA (European Space Agency).

Autor: Luiz Felipe Machado Faria de Sousa
Versão: 1.0.0
"""

# Importa funcionalidades do sistema para manipulação de caminhos e pacotes
import sys  # Usado para adicionar diretórios ao sys.path e permitir importações de outros diretórios
sys.path.append('./codebase_download')  # Adiciona o diretório onde está localizado o módulo copernicus_downloader.py

# Importa a classe CopernicusDownloader do arquivo copernicus_downloader.py
from copernicus_downloader import CopernicusDownloader  # Classe principal usada para buscar e baixar produtos da API do Copernicus

# Data inicial da busca
start_date = '2021-04-01'

# Data final da busca
end_date = '2021-05-01'

# Coordenadas geográficas da região a ser pesquisada. As coorndenadas devem ser uma lista com duas coordenadas (lat, lon) ou,
# Quatro coordenadas (lonmín, latmín, lonmax, latmax) valores float de coordenadas geográficas (graus decimais)
polygon = [-44.06262124827551, -23.108686697869885, -43.59295572093176, -22.89505336946246] #Em caso de um polígono
#point = [-44.06262124827551, -23.108686697869885] #Em caso de um ponto

#Escolha o tipo de produto: neste exemplo estamos buscando o produto full resolution do OLCI em nível L1 (Topo da Atmosfera).
productType: 'S3_EFR'

#Buscando as cenas que são intersectadas pelo polígono (ou ponto)
list_scenes = CopernicusDownloader().search_product(start_date, end_date, polygon, 'S3_EFR')

# Realizando o download em massa: A linha de código abaixo irá baixar cada cena com seus respectivos ID's a partir da lista de cenas encontradas anteriormente. 
# IMPORTANTE: É necessário ter cadastro em: https://www.copernicus.eu/en
username = "username" #login
password = "password" #senha

#Diretório para onde os arquivos baixados ficarão armazenados. Se nenhum diretório for inserido, as imagens serão destinadas o diretório atual.
output = "/home/de-sousa/Documents/lasa_alarmes_agua/02_raw_scenes/sentinel3" #obs: Ao mudar o diretório, certifique-se de inserir o caminho relativo à sua máquina (ex: home/fulano/raw_scenes)

#Realizando o download das imagens
CopernicusDownloader().batch_download(username, password, list_scenes, output)
