# Prova Prática - Observatório

1) Autoavaliação
Tópicos de Conhecimento: 
● Ferramentas de visualização de dados (Power BI, Qlik Sense e outros): 3 
● Manipulação e tratamento de dados com Python: 6 
● Manipulação e tratamento de dados com Pyspark: 6 
● Desenvolvimento de data workflows em Ambiente Azure com databricks: 5 
● Desenvolvimento de data workflows com Airflow: 2
● Manipulação de bases de dados NoSQL: 5
● Web crawling e web scraping para mineração de dados: 5 
● Construção de APIs: REST, SOAP e Microservices: 1

Respostas
a) Olhando para todos os dados disponíveis na fonte citada acima, em qual 
estrutura de dados você orienta guardá-los? Data Lake, SQL ou NoSQL? 
Discorra sobre sua orientação. (1 pts) 
Data lake, pois suporta tanto dados estruturados (vindo de banco de dados relacional como o SQL Server) e semi-estruturados (NoSQL, como o MongoDB)

b) Nosso cliente estipulou que necessita de informações apenas sobre as 
atracações e cargas contidas nessas atracações dos últimos 3 anos (2021- 
2023). Logo, o time de especialistas de dados, em conjunto com você, 
analisaram e decidiram que os dados devem constar no data lake do 
observatório e em duas tabelas do SQL Server, uma para atracação e outra 
para carga. 
Assim, desenvolva script(s) em Python e Spark que extraia os dados do 
anuário, transforme-os e grave os dados tanto no data lake, quanto nas duas 
tabelas do SQL Server, sendo atracacao_fato e carga_fato, com as respectivas 
colunas abaixo. Os scripts de criação das tabelas devem constar no código 
final. 
Lembrando que os dados têm periodicidade mensal, então script’s 
automatizados e robustos ganham pontos extras. (2 pontos + 1 ponto para 
solução automatizada e elegante).

O script main.py executa todo o processo de extração, transformação e carregamento.


c) Essas duas tabelas ficaram guardadas no nosso Banco SQL SERVER. Nossos 
economistas gostaram tanto dos dados novos que querem escrever uma 
publicação sobre eles. Mais especificamente sobre o tempo de espera dos 
navios para atracar. Mas eles não sabem consultar o nosso banco e apenas 
usam o Excel. Nesse caso, pediram a você para criar uma consulta (query) 
otimizada em sql em que eles vão rodar no excel e por isso precisa ter o menor 
número    de     linhas     possível     para     não     travar     o     programa. Eles 
querem uma tabela com dados do Ceará, Nordeste e Brasil contendo número 
de atracações, para cada localidade, bem como tempo de espera para atracar 
e tempo atracado por meses nos anos de 2021 e 2023. Segundo tabela abaixo: 
(2pts) 

Resposta no arquivo query.sql

3) Criação de ambiente de desenvolvimento com Linux e Docker. 
 
Finalmente, este processo deverá ser automatizado usando a ferramenta de orquestração de 
workflow Apache Airflow + Docker. Escreva uma DAG para a base ANTAQ levando em conta 
as características e etapas de ETL para esta base de dados considerando os repositórios de 
data lake e banco de dados. Esta também podperá conter operadores para enviar avisos por 
e-mail, realizar checagens quando necessário (e.g.: caso os dados não sejam encontrados, 
quando o processo for finalizado, etc). Todos os passos do processo ETL devem ser listados 
como tasks e orquestrados de forma otimizada. (3 pts + 1 pts) 

Não consegui terminar a tempo a orquestração do airflow (incompleto).
