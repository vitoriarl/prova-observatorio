import requests
import zipfile
import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options

class WebScraper:
    def __init__(self, url, year, download_path, unzip_dir):
        self.url = url
        self.year = year
        self.download_path = download_path
        self.unzip_dir = unzip_dir
        self.driver = None

    def setup_driver(self):
        """Configura o WebDriver do Selenium em modo headless"""
        options = Options()
        options.add_argument('--headless')
        self.driver = webdriver.Chrome(options=options)

    def open_page(self):
        """Acessa o site e aguarda o carregamento"""
        self.driver.get(self.url)
        time.sleep(3)  # Pode ser ajustado ou substituído por WebDriverWait

    def select_year(self):
        """Seleciona o ano na caixinha de seleção"""
        year_select = Select(self.driver.find_element(By.ID, "anotxt"))
        year_select.select_by_visible_text(self.year)
        time.sleep(10)  # Espera a página carregar os dados do ano

    def find_download_link(self):
        """Localiza e retorna o link do arquivo a ser baixado"""
        xpath_link = "//*[@id='lista_arquivosea']/table/tbody/tr[8]/td[3]/a"
        link_element = self.driver.find_element(By.XPATH, xpath_link)
        return link_element.get_attribute("href")

    def download_file(self, url):
        """Faz o download do arquivo ZIP"""
        response = requests.get(url)
        if response.status_code == 200:
            with open(self.download_path, "wb") as f:
                f.write(response.content)
            print(f"Arquivo ZIP {self.year} baixado com sucesso!")
        else:
            print(f"Falha no download. Código de status: {response.status_code}")

    def unzip_file(self):
        """Descompacta o arquivo ZIP baixado"""
        if not os.path.exists(self.unzip_dir):
            os.makedirs(self.unzip_dir)
        with zipfile.ZipFile(self.download_path, 'r') as zip_ref:
            zip_ref.extractall(self.unzip_dir)
        print(f"Arquivos extraídos para: {self.unzip_dir}")

    def close_driver(self):
        """Fecha o WebDriver"""
        if self.driver:
            self.driver.quit()
