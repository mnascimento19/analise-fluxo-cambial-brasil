import pandas as pd
import requests
import time
import pyodbc 


codigos_sgs = {
    'fluxo_comercial': 2270,
    'fluxo_financeiro': 2510
}
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}
df_final = pd.DataFrame()

print("Iniciando a captura dos dados do Banco Central...")
for nome_serie, codigo in codigos_sgs.items():
    
    print(f"Buscando a série: {nome_serie} (Código: {codigo})")
    url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        try:
            dados_json = response.json()
            if dados_json:
                df_temp = pd.DataFrame(dados_json)
                df_temp['data'] = pd.to_datetime(df_temp['data'], dayfirst=True)
                df_temp['valor'] = pd.to_numeric(df_temp['valor'])
                df_temp.rename(columns={'valor': nome_serie}, inplace=True)
                if df_final.empty:
                    df_final = df_temp
                else:
                    df_final = pd.merge(df_final, df_temp, on='data', how='inner')
        except requests.exceptions.JSONDecodeError:
            print(f"Erro de decodificação de JSON para a série {nome_serie}.")
    else:
        print(f"Erro na requisição para a série {nome_serie}. Status Code: {response.status_code}")
    time.sleep(1)

if not df_final.empty:
    print("\nCalculando o fluxo total...")
    df_final['fluxo_total'] = df_final['fluxo_comercial'] + df_final['fluxo_financeiro']
    df_final.sort_values(by='data', inplace=True)
    df_final.reset_index(drop=True, inplace=True)
    print("\nCaptura finalizada com sucesso!")
    print(df_final.head())

    # ==============================================================================
    # 2. CARGA DE DADOS NO SQL SERVER
    # ==============================================================================
    
    
    server_name = 'MATHEUSNASC\SQLEXPRESS' 
    database_name = 'PORTFOLIOBCB'
    
    # String de conexão para Autenticação do Windows
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server_name};'
        f'DATABASE={database_name};'
        f'Trusted_Connection=yes;'
    )

    try:
        print("\nConectando ao SQL Server...")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("Conexão bem-sucedida!")

        # Limpando a tabela antes da carga
        print("Limpando a tabela FatoFluxoCambial...")
        cursor.execute("TRUNCATE TABLE FatoFluxoCambial")
        
        print("Iniciando a carga dos dados...")
        # Prepara o comando de inserção
        sql_insert = """
            INSERT INTO FatoFluxoCambial (Data, FluxoComercial, FluxoFinanceiro, FluxoTotal)
            VALUES (?, ?, ?, ?);
        """

        # Converte o DataFrame para uma lista de tuplas para inserção
        dados_para_inserir = [tuple(x) for x in df_final.to_numpy()]

        # Executa a inserção de forma eficiente
        cursor.executemany(sql_insert, dados_para_inserir)
        
        # Confirma a transação
        conn.commit()

        print(f"\nCarga finalizada com sucesso! {len(dados_para_inserir)} linhas foram inseridas na tabela.")

    except Exception as e:
        print(f"\nOcorreu um erro ao conectar ou inserir os dados: {e}")

    finally:
        # Fecha a conexão
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
            print("Conexão com o SQL Server fechada.")

else:
    print("\nNenhum dado foi capturado. A carga no banco de dados foi cancelada.")
