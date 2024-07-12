# Teste Engenheiro de Dados - Melhor Envio
## Instruções

Neste teste, o objetivo é transformar e carregar os dados contidos no arquivo zip (presente neste repositório nomeado como `data.zip`) para um banco de dados PostgreSQL.

Os arquivos contêm informações sobre códigos e eventos de rastreamento de compras em plataformas de e-commerce.

Cada arquivo representa um batch de transações a serem executadas em um banco de dados, onde cada linha é uma transação diferente, identificada pela coluna "Op". Essas transações podem incluir operações de INSERT (identificada pela letra I) ou UPDATE (identificada pela letra U).

O processo envolve a extração dos dados dos arquivos do zip e o carregamento desses dados para tabelas apropriadas no banco de dados PostgreSQL.

Utilize a seguinte docker image do PostgreSQL: [postgres:12.16](https://hub.docker.com/layers/library/postgres/12.16/images/sha256-a97fd76ab09599e2ddc15c90a87f9a4a2a60551d99f8e7397f12a1d606d7f0ab?context=explore)

## Requisitos

- Elaborar um processo de ETL que realize extração, transformação e carregamento dos dados presentes nos arquivos fornecidos, conforme as transações especificadas em cada linha.

- Realizar a normalização e modelagem do banco de dados, bem como suas tabelas, conforme for apropriado.

- O processo de ETL deve ser desenvolvido para ser executado utilizando o Docker, garantindo que todas as etapas sejam executadas dentro de containers.

- **IMPORTANTE**: Em um cenário real, os arquivos seriam disponibilizados à medida em que são criados. Portanto, é **obrigatório** que os arquivos sejam extraídos, transformados e carregados para o banco de dados no máximo de 5 em 5 por vez, isto é, o processamento direto de todos os dados não é permitido.

- Documentar o passo a passo de como executar a pipeline, bem como pontos importantes do desenvolvimento, funcionalidades e decisões de implementação.

- Disponibilizar o código do projeto em um repositório git público e enviar o link para avaliação do teste.

- Desenvolver 3 queries como forma de relatório para validação dos dados:
	- Total de rastreamentos criados por minuto
```sql

```
	- Total de eventos por código de rastreamento
```sql
select oid_id,
on2.order_number as numero_da_orden,
count(*) as total 
from order_main om
left join order_number on2 on om.oid_id = on2.id
group by oid_id, on2.order_number
order by count(*) desc
```

	- TOP 10 descrições de eventos mais comuns e seus totais
```sql
select upper(order_description) as order_description, 
count(*) as total
from order_main om
where order_description is not null
group by order_description
order by 2 desc
```

## Sobre os dados

- O arquivo zip contém diversos arquivos `.csv` representando os dados de transações as quais precisam ser processadas pela ETL.

- Os arquivos possuem o seguinte padrão de nome: `YYYMMDD-HHMMSSsss`. O nome do arquivo representa a data e horário de criação do arquivo e deve ser levado em consideração para que os dados produzidos pela ETL sejam consistentes.

- Podem ocorrer casos onde uma transação venha com o tipo de operação de UPDATE porém o id da mesma não possua uma operação de INSERT anterior representada nesse conjunto de dados. Quando isso ocorrer, a linha deve apenas ser inserida no banco de dados.

## Dicionário de dados

- Op: tipo de operação da transação
- oid__id: identificador do rastreamento
- createdAt: data de criação do rastreamento
- updatedAt: data de atualização do rastreamento
- lastSyncTracker: data da última sincronização do rastreamento
- array_trackingEvents: contém informações sobre os eventos de rastreamento
- createdAt: data de criação do evento
- trackingCode: código de rastreamento
- status: status do evento
- description: descrição do evento
- trackerType: código da transportadora
- from: agência de partida do evento
- to: agência de destino do evento

## Diferencial

- O Amazon Redshift (Data Warehouse) possui uma interface similar ao PostgreSQL mas não implementa alguns comandos e funcionalidades. Um destes comandos é `INSERT ... ON CONFLICT`. Desenvolva a lógica de inserção com update (upsert) dos dados sem a utilização deste comando nativo do PostgreSQL.