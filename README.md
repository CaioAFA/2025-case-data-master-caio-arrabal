# Data Master - Case Cientista de Dados

Link do Kaggle: https://www.kaggle.com/datasets/gcenachi/case-data-master-2024

<br>

## Tecnologias utilizadas

Desafio com solução de Aprendizado Supervisionado:

**Python3**
- Pandas:
    - Leitura / escrita de bases de dados
    - Data prepare

- Scikit-Learn: fácil prototipagem, bom para testar hipóteses e diferentes algoritmos para solução do problema

- Conda: gerenciamento de pacotes

- Matplotlib + Seaborn: gráficos para análise de dados

- Joblib: salvar / ler modelo treinado

- Pyarrow: leitura de arquivos Parquet

<br>

**DuckDB**
- Consultas dos dados em formato SQL

- Possibilidade de trabalhar com uma grande quantidade de dados com maior eficiência de memória

- Data prepare

<br>

## Instalação do projeto
Instalando as dependências necessárias:
```bash
conda env create -p ./env -f ./environment.yml
```

<br>

Exportando os requirements.txt:
```bash
conda env export > ./environment.yml
```

<br>

## Executando o código
Execute o código por ordem numérica:
- Notebooks 01 -> Análise dos dados
- Notebook 02 -> Upload dos dados para o DuckDB
- Notebook 03 -> Prepara os dados para o treinamento
- Notebook 04 -> Treina o modelo
- Notebook 05 -> Realiza as previsões de Churn
- Notebooks 06 -> Análise pós previsões
- Notebook 07 -> Análise do impacto financeiro da ação

<br>

## Dicionário de Dados
### **transactions.parquet**
Transações de usuários (até 31/03/2017)
| Campo                  | Descrição                                                           |
|------------------------|---------------------------------------------------------------------|
| msno                   | ID do usuário                                                       |
| payment_method_id      | ID do método de pagamento                                           |
| payment_plan_days      | Duração do plano em dias                                            |
| plan_list_price        | Valor do plano (em dólar taiwanês novo (NTD))                       |
| actual_amount_paid     | Valor pago (em dólar taiwanês novo (NTD))                           |
| is_auto_renew          | É auto-renovável                                                    |
| transaction_date       | Data da transação (formato %Y%m%d)                                  |
| membership_expire_date | Data de expiração (formato %Y%m%d)                                  |
| is_cancel              | Usuário cancelou ou não a associação nesta transação                |

<br>

### **members.parquet**
Informações de usuários. Nem todos os usuários estão disponíveis no conjunto de dados.
| Campo                  | Descrição                                                              |
|------------------------|------------------------------------------------------------------------|
| msno                   | ID do usuário                                                          |
| city                   | Cidade                                                                 |
| bd                     | Data de nascimento (há valores entre -7000 e 2015, ajuste os outliers) |
| gender                 | Gênero                                                                 |
| registered_via         | Método de cadastro                                                     |
| registration_init_time | Data do cadastro (formato %Y%m%d)                                      |


<br>

### **user_logs.parquet**
| Campo                  | Descrição                                                           |
|------------------------|---------------------------------------------------------------------|
| msno                   | ID do usuário                                                       |
| date                   | Data do registro (formato %Y%m%d)                                   |
| num_25                 | Nº de músicas tocadas por menos de 25% da duração                   |
| num_50                 | Nº de músicas tocadas por menos de 50% da duração                   |
| num_75                 | Nº de músicas tocadas por menos de 75% da duração                   |
| num_985                | Nº de músicas tocadas por menos de 98.5% da duração                 |
| num_100                | Nº de músicas tocadas por mais de 98.5% da duração                  |
| num_unq                | Nº de músicas únicas tocadas                                        |
| total_secs             | Total de segundos tocados                                           |

### **treated_churn_dataset_{YYYY}_{MM}_{DD}_{hh}h{mm}m**
OBS: Nem todos os dados serão usados no treinamento do modelo!
> Veja quais foram usados no arquivo de configurações `project_config.py`

| Campo | Descrição |
|-|-|
| msno | ID do usuário |
| safra | Data do registro (formato %Y%m%d) |
| num_25 | Nº de músicas tocadas por menos de 25% da duração |
| num_50 | Nº de músicas tocadas por menos de 50% da duração |
| num_75 | Nº de músicas tocadas por menos de 75% da duração |
| num_985 | Nº de músicas tocadas por menos de 98.5% da duração |
| num_100 | Nº de músicas tocadas por mais de 98.5% da duração |
| num_unq | Nº de músicas únicas tocadas |
| total_secs | Total de segundos tocados |
| total_hours | Total de horas tocadas |
| msno_1 | ID do usuário (campo repetido devido aos joins) |
| payment_method_id | ID do método de pagamento |
| payment_plan_days | Duração do plano em dias |
| plan_list_price | Valor do plano (em dólar taiwanês novo (NTD)) |
| actual_amount_paid | Valor pago (em dólar taiwanês novo (NTD)) |
| is_auto_renew | É auto-renovável |
| is_cancel | Usuário cancelou ou não a associação nesta transação |
| safra_1 | Data do registro (campo repetido devido aos joins) |
| transaction_date_year | Ano da transação |
| transaction_date_month | Mês da transação |
| transaction_date_day | Dia da transação |
| transaction_date_day_of_week | Dia da semana da transação |
| transaction_date_day_of_year | Dia do ano da transação |
| membership_expire_date_year | Ano da data de expiração |
| membership_expire_date_month | Mês da data de expiração |
| membership_expire_date_day | Dia da data de expiração |
| membership_expire_date_day_of_week | Dia da semana da data de expiração |
| membership_expire_date_day_of_year | Dia do ano da data de expiração |
| discount | Desconto |
| price_per_month | Preço por mês |
| members_msno | ID do usuário (campo repetido devido aos joins) |
| members_safra | ID do usuário (campo repetido devido aos joins) |
| city | Cidade |
| registered_via | Método de cadastro |
| is_active | Usuário ativo? |
| registration_init_time_year | Ano de registro do usuário |
| registration_init_time_month | Mês de registro do usuário |
| registration_init_time_day | Dia do registro do usuário |
| registration_init_time_day_of_week | Dia da semana de registro do usuário |
| registration_init_time_day_of_year | Dia do ano de registro do usuário |
| _filled_out_members_info | Coluna de controle: preenchimento forçado das colunas de membros |
| is_churn | É churn? |
| no_churn_information | Possui informações de Churn? |
| num_unq-2M | Valor de num_unq (2 meses antes) |
| num_unq-1M | Valor de num_unq (1 mês antes) |
| total_secs-2M | Valor de total_secs (2 meses antes) |
| total_secs-1M | Valor de total_secs (1 mês antes) |
| num_25-2M | Valor de num_25 (2 meses antes) |
| num_25-1M | Valor de num_25 (1 mês antes) |
| num_50-2M | Valor de num_50 (2 meses antes) |
| num_50-1M | Valor de num_50 (1 mês antes) |
| num_75-2M | Valor de num_75 (2 meses antes) |
| num_75-1M | Valor de num_75 (1 mês antes) |
| num_985-2M | Valor de num_985 (2 meses antes) |
| num_985-1M | Valor de num_985 (1 mês antes) |
| num_100-2M | Valor de num_100 (2 meses antes) |
| num_100-1M | Valor de num_100 (1 mês antes) |

### **predicted_dataset_{YYYY}_{MM}_{DD}_{hh}h{mm}m**
| Campo                  | Descrição |
|-|-|
| (`Todas as colunas da tabela treated_churn_dataset_{YYYY}_{MM}_{DD}_{hh}h{mm}m`) | + |
| predicted_is_churn | Previsão de Churn |
| predicted_is_churn_proba_false | Probabilidade da previsão ser False |
| predicted_is_churn_proba_true | Probabilidade da previsão ser True |
| predict_certain | Certeza a respeito da previsão |
