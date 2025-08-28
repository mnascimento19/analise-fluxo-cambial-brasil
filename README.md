Análise de Fluxo Cambial Brasileiro

Projeto de portfólio de análise de dados de ponta a ponta (end-to-end) que explora a evolução do fluxo cambial no Brasil, utilizando dados públicos do Banco Central.

Objetivo

O objetivo deste projeto foi construir um pipeline de dados completo para extrair, transformar, carregar e visualizar dados sobre o balanço de capital comercial e financeiro do Brasil, respondendo à pergunta: qual dos dois fluxos tem maior impacto no saldo cambial do país?

Tecnologias Utilizadas

Linguagem:Python 

Bibliotecas Principais: Pandas, Requests, PyODBC

Banco de Dados: Microsoft SQL Server

Ferramenta de BI: Power BI

Estrutura

O script captura_dados_bcb.py automatiza todo o processo de ETL:

Conecta-se à API de Séries Temporais do Banco Central.

Realiza o tratamento e a limpeza dos dados.

Carrega os dados limpos em uma tabela no SQL Server.

O dashboard desenvolvido no Power BI conecta-se a este banco de dados para a análise visual.

Dashboard Final

Uma prévia do dashboard interativo criado:
<img width="954" height="533" alt="image" src="https://github.com/user-attachments/assets/7293e7f9-c518-4ee6-8568-870fd000bbb7" />
