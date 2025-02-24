from etl.extract import WebScraper 
from etl.transform import TransformData
from etl.load import DataFrameToSQLServer

def main():
    # Dados para execução
    years = ['2021', '2022', '2023']
    for year in years:
        url = "https://web3.antaq.gov.br/ea/sense/download.html#pt"
        download_path = f"../data/zip/{year}.zip"
        unzip_dir = f"../data/unzip/{year}"

        # Criação do objeto WebScraper
        scraper = WebScraper(url, year, download_path, unzip_dir)

        # Executa as etapas
        scraper.setup_driver()
        scraper.open_page()
        scraper.select_year()
        download_link = scraper.find_download_link()
        print(f"Link encontrado: {download_link}")

        # Baixa e descompacta o arquivo
        scraper.download_file(download_link)
        scraper.unzip_file()

        # Fecha o WebDriver
        scraper.close_driver()

        transform = TransformData()

        atracacao_fato = transform.transform_atracacao_fato(year, unzip_dir)

        carga_fato = transform.transform_carga_fato(year, unzip_dir)

        load = DataFrameToSQLServer()

        load.save_to_sql(atracacao_fato, "atracacao_fato", "append")
        load.save_to_sql(carga_fato, "carga_fato", "append")

if __name__ == "__main__":
    main()

