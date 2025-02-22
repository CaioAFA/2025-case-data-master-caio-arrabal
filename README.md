# Data Master - Case Cientista de Dados

Link do Kaggle: https://www.kaggle.com/datasets/gcenachi/case-data-master-2024

<br>

## Tecnologias utilizadas

Desafio com solução de Aprendizado Supervisionado:
Python3
- Pandas: utilizado para:
    - Leitura / escrita de bases de dados
    - Data preparation
- Scikit-Learn: fácil prototipagem, bom para testar hipóteses e diferentes algoritmos para solução do problema
- Conda: gerenciamento de pacotes

Considerações:
- Utilizar uma implementação que possa ser utilizada em hardwares com menos memória


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



### TODO
- Criar variáveis para custo de mês-1, mês-2
    - Utilizar groupby com Shift
    - Limpeza dos dados com essas colunas vazias
- Criar método para buscar dados de transactions e de members