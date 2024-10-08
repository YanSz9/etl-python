# README - Projeto ETL com Python e PostgreSQL

## Visão Geral

Este projeto implementa um processo ETL (Extração, Transformação e Carga) utilizando Python e PostgreSQL. O objetivo é extrair dados de uma API pública, transformá-los conforme um mapeamento predefinido e carregá-los em um banco de dados relacional.

## Estrutura do Projeto

- **Python**: Linguagem usada para a lógica ETL.
- **PostgreSQL**: Banco de dados relacional para armazenar os dados transformados.
- **APIs**: O projeto extrai dados das seguintes rotas da API https://demodata.grapecity.com 
  - Produtos (`/products`)
  - Pedidos de Venda (`/salesOrders`)
  - Detalhes dos Pedidos de Venda (`/salesOrderDetails`)
  - Pessoas(`/persons`)
  - Cliente(`/customers`)

## Processo ETL

1. **Extração**: Os dados são extraídos das APIs utilizando requisições `GET` paginadas.
2. **Transformação**: Cada dado é mapeado para os campos da tabela do banco de dados, conforme os dicionários de mapeamento.
3. **Carga**: Os dados transformados são inseridos nas tabelas do PostgreSQL (`d_product`, `f_sales`, `d_sales_order_details`, `d_persons`, `d_customers`) com a política de upsert (`ON CONFLICT`) para garantir que dados duplicados sejam atualizados em vez de criados novamente.

## Estrutura do Código

- **Conexão com o Banco**: Utiliza a biblioteca `psycopg2` para conectar e executar queries no PostgreSQL.
- **Extração**: Função `extract_data` que itera sobre as páginas da API até que todos os dados sejam coletados.
- **Transformação**: Função `transform_data` que converte o formato bruto da API para o formato utilizado no banco de dados.
- **Carga**: Função `load_data` que insere os dados nas respectivas tabelas com tratamento de conflitos.
  
## Execução

### 1. Instalação das Dependências
Execute o comando abaixo para instalar as bibliotecas necessárias:
```bash
pip install -r requirements.txt
```
## StarSchema
![image](https://github.com/user-attachments/assets/a6f016a4-af08-41a6-b71f-238d54584ca0)

