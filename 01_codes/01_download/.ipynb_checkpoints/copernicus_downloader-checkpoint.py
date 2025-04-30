# copernicus_downloader.py

class CopernicusDownloader:
    TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    PRODUCT_INFO_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
    PRODUCT_DOWNLOAD_URL = "https://zipper.dataspace.copernicus.eu/odata/v1/Products"

    def search_product(self, start_date, end_date, coordinates, productType):
        dag = EODataAccessGateway()
        
        # Verifica o tipo de coordenadas: ponto (2 valores) ou polígono (4 valores)
        if len(coordinates) == 2:
            # Caso seja um ponto, repete as coordenadas para formar um quadrado mínimo
            lonmin = lonmax = coordinates[0]
            latmin = latmax = coordinates[1]
        elif len(coordinates) == 4:
            # Caso seja um polígono, atribui os limites diretamente
            lonmin, latmin, lonmax, latmax = coordinates
        else:
            # Exceção em caso de coordenadas inválidas
            raise ValueError("Lista de coordenadas inválida. Use 2 valores (ponto) ou 4 valores (polígono).")

        # Lista que armazenará os IDs dos produtos encontrados
        list_ids = []

        while parser.parse(start_date) < parser.parse(end_date):
            search_results = dag.search(
                productType=productType,
                geom={'lonmin': coordinates[0], 'latmin': coordinates[1], 'lonmax': coordinates[2], 'latmax': coordinates[3]},
                start=start_date,
                end=end_date,
                provider='creodias',
            )

            if not search_results:
                print("Nenhum novo resultado encontrado. Encerrando...")
                break

            for scene in search_results:
                list_ids.append(scene.properties['title'])

            last_date_str = str(search_results[-1].properties['startTimeFromAscendingNode'][0:10])

            if '-' not in last_date_str:
                if len(last_date_str) == 6:
                    last_date = datetime.datetime.strptime(last_date_str, '%y%m%d')
                elif len(last_date_str) == 8:
                    last_date = datetime.datetime.strptime(last_date_str, '%Y%m%d')
                else:
                    raise ValueError("Formato de data não reconhecido")
            else:
                last_date = datetime.datetime.strptime(last_date_str, '%Y-%m-%d')

            start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        print(f"Total de produtos encontrados: {len(list_ids)}")
        return list_ids
        
    def __init__(self):
        self.access_token = None

    def get_access_token(self, username, password):
        data = {
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        }
        response = requests.post(self.TOKEN_URL, data=data)
        response.raise_for_status()
        self.access_token = response.json()["access_token"]

    def get_product_id(self, product_name):
        response = requests.get(
            f"{self.PRODUCT_INFO_URL}?$filter=Name eq '{product_name}'"
        )
        response.raise_for_status()
        json_data = response.json()
        if not json_data["value"]:
            raise ValueError(f"No product found with name {product_name}")
        return json_data["value"][0]["Id"]

    def download_product(self, username, password, product_name, download_directory, output_name=None):
        self.get_access_token(username, password)

        product_id = self.get_product_id(product_name)
        url = f"{self.PRODUCT_DOWNLOAD_URL}({product_id})/$value"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        for attempt in range(3):
            try:
                response = requests.get(url, headers=headers, stream=True)
                response.raise_for_status()

                file_size = int(response.headers.get("content-length", 0))
                block_size = 1024

                output_name = output_name or f"{product_name}.zip"
                output_path = os.path.join(download_directory, output_name)

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
                time.sleep(5)
                if attempt == 2:
                    raise

    def batch_download(self, username, password, list_ids, download_directory):
        download_count = 0
        id_downloaded = [f[:-4] for f in os.listdir(download_directory) if f.endswith('.zip')]  # Lista de cenas contidas no diretorio de download

        for ID in list_ids:
            if ID in id_downloaded:
                print(f"{ID} já foi baixado anteriormente")
                continue

            if download_count == len(list_ids):
                print(f"Encerrando sessão após baixar todos os produtos da lista de cenas: {download_count} downloads")
                break

            try:
                print(ID)
                self.download_product(username, password, ID, download_directory)
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