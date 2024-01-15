# Trabalho de Stream Processing Pipelines

## Estrutura do projeto

- Docker e Docker Compose
- Spark Streaming
- Python
- Parquet

## Enunciado:

Construir um Stream Pipeline utilizando uma Ferramenta ou Plataforma. Realizar a ingestão dos dados e o output dele em outro formato (Por exemplo, se o DataSource está em JSON, o Output pode ser em Parquet).

- A escolha é livre de DataSource a ser utilizada.

- Sugestão: Apache Spark Streaming ou AWS Kinesis.

- Bônus 1: Acrescentar uma etapa de filtro e/ou Structured Stream ou Validação.

- Bônus 2: Acrescentar uma etapa de agregação nos dados.

- Bônus 3: Na etapa anterior, utilizar a função Window.

- Bônus 4: Construir o processo de Deploy desse Pipeline.

- Dica: Evidenciar o passo-a-passo de como o Pipeline foi construído ou o quê ele faz.

- Grupos de 3 a 6 integrantes.

- Dúvidas: mande um [e-mail](profrafael.matsuyama@fiap.com.br) ou mensagem pelo LinkedIn.

## Descrição do projeto

Os dados coletados tem como origem o site de compra de apartamentos https://www.chavesnamao.com.br/apartamentos-a-venda/sp-sao-paulo/, onde o dataset está limitado a apartamentos de 100 mil a 500 mil reais nas 5 zonas de SP (Sul, Leste, Oeste, Norte e Central).

A coleta foi realizada via webscraping com Python, e o processamento em Streaming com  Spark Streaming localmente utilizando Docker e Docker Compose.

## Evidencias de execução


### Bônus 1 - Filtro e validação

São desconsiderados dados de anuncios com dados faltantes como preço e área do imovel

```python
def remove_bad_data(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.filter(F.col("price") != "")
        .dropna(subset="price")
        .filter(~F.col("floorSize").contains("--"))
        .withColumn("floorSize", F.regexp_replace("floorSize", r"\D+", ""))
    )
```

### Bônus 2 - Agregação

Contagem do número de auncios de imoveis por região, e faixa de preço de 100 a 500 mil reais com intervalos de 100 mil reais.

```python
def group_by_price_range_and_region(dataframe: DataFrame) -> DataFrame:
    df_with_bins = dataframe.withColumn(
        "price_bin",
        F.when(F.col("price") < 100000, "<100K")
        .when((F.col("price") >= 100000) & (F.col("price") < 200000), "100K-200K")
        .when((F.col("price") >= 200000) & (F.col("price") < 300000), "200K-300K")
        .when((F.col("price") >= 300000) & (F.col("price") < 400000), "300K-400K")
        .when((F.col("price") >= 400000) & (F.col("price") < 500000), "400K-500K")
        .otherwise(">=500K"),
    )
    return df_with_bins
```

Resultado

![Group Data](/docs/group_data.png "Dados Agrupados")

### Bônus 3 - Agregação com a função Window

Calculo do preço por metro quadrado de cada imovel, calculo da média do metro quadrado agrupada por região e feito o rank de maneira descendente.

```python
def group_by_price_range_and_region(dataframe: DataFrame) -> DataFrame:
    df_with_bins = (
        dataframe.withColumn(
            "price_bin",
            F.when(F.col("price") < 100000, "<100K")
            .when((F.col("price") >= 100000) & (F.col("price") < 200000), "100K-200K")
            .when((F.col("price") >= 200000) & (F.col("price") < 300000), "200K-300K")
            .when((F.col("price") >= 300000) & (F.col("price") < 400000), "300K-400K")
            .when((F.col("price") >= 400000) & (F.col("price") < 500000), "400K-500K")
            .otherwise(">=500K"),
        )
        .groupBy("price_bin", "region")
        .count()
    )
    return df_with_bins
```

Resultado

![Window Data](/docs/window_data.png "Dados Janelados")


## Como executar o pipeline

O Scrap dos dados é opcional pois no repositorio já temos uma massa de dados de teste

(Opcional)Instalando as dependencias do Python

```bash
poetry install --no-root
```

(Opcional)Executar o pipeline de Scrap

```bash
python src/scrap.py
```

Executar o Spark localmente

```bash
docker compose up --build -d
```

Executando o codigo do Spark Streaming e gerar arquivos em Parquet

Acessar o terminal do container

```bash
docker exec -it spark-master-1 bash
```
Executar o script com o Spark Submit

```bash
cd /opt/spark/bin/

./spark-submit \
   --master local[*] \
   --deploy-mode client \
   --driver-memory 8g \
   --executor-memory 16g \
   --executor-cores 4  \
   --py-files /opt/spark/code/streaming.py \
   /opt/spark/code/streaming.py
```

Para parar os containeres

```bash
docker compose stop
# ou
docker compose down -v
```

### Demonstração da execução do Pipeline de Streaming

https://github.com/gian-lepear/trab-streaming/assets/11441766/b1a54bc2-d32b-4426-b5f6-92f43f5136a6

