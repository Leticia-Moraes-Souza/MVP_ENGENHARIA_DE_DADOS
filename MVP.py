# Databricks notebook source
# MAGIC %md
# MAGIC ## MVP Engenharia de dados
# MAGIC Projeto desenvolvido para a disciplina de Engenharia de dados. Para desenvolvimento do projeto, será usada base de dados com informações sobre tempo de tela entre crianças e adolescentes obtida no repositório kaggle: https://www.kaggle.com/datasets/ak0212/average-daily-screen-time-for-children
# MAGIC
# MAGIC Objetivos: com o sucesso da série "Adolescente" lançada recentemente, observou-se aumento de discussões sobre a exposição de crianças e adolescentes a conteúdo online. Assim, as analises desse projeto visão entender qual o tempo médio de exposição a telas entre diferentes faixas etárias, qual o gênero com maior tempo de exposição a tela, qual o tipo de acesso mais frequente entre os usuários.
# MAGIC
# MAGIC
# MAGIC Para alcançar os objetivos, serão usados os recursos da Azure, Databricks e Power Bi, para carregar, tratar e visualizar os dados. Também serão realizadas analises iniciais para avaliação da qualidade das informações. Por fim, será realizado estudo para alcançar os objetivos apresentados inicialmente.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Recurso de cloud: Azure
# MAGIC
# MAGIC   Para realizar o processo de armazenamento em nuvem, foi escolhida a ferramenta Azure, usada na versão de teste gratuito. A escolha foi feita para simular um ambiente de trabalho com uso de múltiplo recursos operacionais e para ampliar o conhecimento sobre novas ferramentas.
# MAGIC
# MAGIC   O ambiente foi configurado para ter o grupo de recursos contendo conta de armazenamento, Data Factory e conexão com serviço do Azure Databricks.
# MAGIC
# MAGIC ![ambiente_azure_tela_inicial.png](./ambiente_azure_tela_inicial.png "ambiente_azure_tela_inicial.png")
# MAGIC
# MAGIC   Na ferramenta da Azure, foi criado o container "mvp" e a pasta "raw" que serão usadas para armazenamento de arquivo em nuvem.
# MAGIC  ![container_azure.png](./container_azure.png "container_azure.png")
# MAGIC
# MAGIC   Para simular um ambiente de produção, foi escolhido baixar o arquivo disponibilizado no site https://www.kaggle.com/datasets/ak0212/average-daily-screen-time-for-children, e carregar no github para que fosse possível criar um conjunto de dados no Azure Data Factory que realiza a leitura de arquivos csv com fonte Http.
# MAGIC   ![github.png](./github.png "github.png")
# MAGIC   No ambiente do data factory, foram criados dois serviços vinculados, para leitura de arquivo com fonte http e para armazenamento em blob.
# MAGIC   ![serviços_vinculados.png](./serviços_vinculados.png "serviços_vinculados.png")
# MAGIC   Para construção do pipeline, foram criados dois conjuntos de dados. O conjunto "get_data" para ler o arquivo no github: ![get_data.png](./get_data.png "get_data.png")
# MAGIC
# MAGIC   E o conjunto "upload_data", para armazenamento em blob:
# MAGIC   ![load_data.png](./load_data.png "load_data.png")
# MAGIC
# MAGIC  Assim, foi possível estabelecer o pipeline que faria a carga do csv, usando os parâmetros do conjunto "get_data" como origem e os parâmetros do "upload_data" como coleta. Ao depurar o pipeline, o arquivo csv foi corretamente carregado na conta de armazenamento:
# MAGIC  ![pipeline.png](./pipeline.png "pipeline.png")
# MAGIC
# MAGIC  ![screen_time_container.png](./screen_time_container.png "screen_time_container.png")
# MAGIC   
# MAGIC   Uma vez que o arquivo foi armazenado em nuvem, foi criado notebook no Databricks para dar continuidade ao projeto.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Lendo arquivos da nuvem
# MAGIC Conectando Databricks com armazenamento Azure e observando se o dataframe criado corretamente.

# COMMAND ----------


# Configurar a chave de acesso da conta de armazenamento
spark.conf.set("fs.azure.account.key.mvpengdado.dfs.core.windows.net", "UiP8dafkrcGZd3/THhfKIVRv7WMEYn1NFulhXEh7pvZK+EAHO/uDMsy2xurLyHQLB17fF8s8M89M+ASt1x2Efg==")

# Definir o caminho do arquivo CSV
file_path = "abfss://mvp@mvpengdado.dfs.core.windows.net/raw/Leticia-Moraes-Souza/MVP_ENGENHARIA_DE_DADOS/refs/heads/main/screen_time.csv"

# Carregar o arquivo CSV no DataFrame
df = spark.read.option("header", "true").csv(file_path)

# Mostrar primeiras linhas do data frame
display(df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC O dataframe foi criado corretamente, como mostrado pela exibição das 5 primeiras linhas.

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Analises dos dados carregados

# COMMAND ----------

# MAGIC %md
# MAGIC Inicialmente, será analisado o formato dos dados de cada coluna e algumas analises descritivas básicas.

# COMMAND ----------

# Importando funções do pyspark
import pyspark.sql.functions as F

# COMMAND ----------

# Mostrando o schema dos dados
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Como visto, o conjunto de dados é composto por 6 colunas.
# MAGIC
# MAGIC A coluna Age, contém informações de idade, a coluna Gender especifica o gênero, a coluna Sreen Time Type contém os tipos de uso das telas, a coluna Day Type mostra se o uso foi durante o fim de semana ou durante a semana, a coluna Average screen time (hours) contém dados de tempo médio de uso de tela, e a coluna Sample Size contém dados sobre o tamanho da amostra observada.
# MAGIC
# MAGIC Inicialmente, as colunas serão renomeadas para que fiquem com cabeçalhos padronizados.

# COMMAND ----------

# Renomar colunas
df = df.withColumnRenamed("Screen Time Type", "Screen_Time_Type")
df = df.withColumnRenamed("Average Screen Time (hours)", "Average_Screen_Time_hours")
df = df.withColumnRenamed("Day Type", "Day_Type")
df = df.withColumnRenamed("Sample Size", "Sample_Size")

# Mostrar primeiras linhas do data frame
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Ao observar o schema de dados, também foi visto que as 6 colunas estão em formato string. Então, será ajustado para que as colunas com dados numéricos sejam corretamente armazenadas.

# COMMAND ----------

# Mudar tipo de dados das colunas numéricas
df = df.withColumn("Age", df["Age"].cast("int"));
df = df.withColumn("Sample_Size", df["Sample_Size"].cast("int"));
df = df.withColumn("Average_Screen_Time_hours", df["Average_Screen_Time_hours"].cast("float"))

# Conferir schema
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Agora que as colunas estão com tipos corretos, serão obervadas algumas analises descritivas básicas.

# COMMAND ----------

df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Pode-se ver que há 198 observações para esse conjunto de dados. 
# MAGIC A coluna Age tem valore de 5 a 15, com média de 10 anos.
# MAGIC A coluna Average_Screen_Time_hours tem valores que vão de aproximadamente 26 minutos a 8 horas.
# MAGIC A coluna Sample_Size tem valores que vão de 300 a 500, com média de 400 observações.
# MAGIC
# MAGIC Agora será analisado se as colunas contem valores nulos e posteriormente serão observados todos os valores contidos em cada coluna para analisar se é necessário algum tipo de tratamento para retirada de dados inválidos.
# MAGIC  

# COMMAND ----------

# Importar função count
from pyspark.sql.functions import count, when, isnan, col

# Mostrar valores nulos
df.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Não há valores nulos nas colunas, serão analisados se há valores fora do esperado para as informações de cada coluna.

# COMMAND ----------

# Mostrando valores da coluna Age
dropDisDF = df.dropDuplicates(["Age"]).select("Age")
dropDisDF.show(truncate=False)

# COMMAND ----------

# Mostrando valores da coluna Gender
dropDisDF = df.dropDuplicates(["Gender"]).select("Gender")
dropDisDF.show(truncate=False)

# COMMAND ----------

# Mostrando valores da coluna Screen_Time_Type
dropDisDF = df.dropDuplicates(["Screen_Time_Type"]).select("Screen_Time_Type")
dropDisDF.show(truncate=False)

# COMMAND ----------

# Mostrando valores da coluna Day_Type
dropDisDF = df.dropDuplicates(["Day_Type"]).select("Day_Type")
dropDisDF.show(truncate=False)

# COMMAND ----------

# Mostrando valores da coluna Average_Screen_Time_hours
dropDisDF = df.dropDuplicates(["Average_Screen_Time_hours"]).select("Average_Screen_Time_hours")
dropDisDF.show(truncate=False)

# COMMAND ----------

# Mostrando valores da coluna Sample_Size
dropDisDF = df.dropDuplicates(["Sample_Size"]).select("Sample_Size")
dropDisDF.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Aparentemente, não há nenhum valor que deva ser substituído. Para a visualização da coluna Average_Screen_Time_Hours foram mostrados apenas os primeiros 20 valores, mas considerando as informações de analise descritiva, onde o valor mínimo era 0.44 e o valor máximo 8.26, pode-se considerar que ela se encontra dentro de um range esperado.

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Extraindo informações dos dados
# MAGIC  
# MAGIC  Serão feitas as analises para atender os objetivos do projeto.
# MAGIC
# MAGIC  Sendo eles: 
# MAGIC  
# MAGIC -  entender qual o tempo médio de exposição a telas entre diferentes faixas etárias;
# MAGIC
# MAGIC -  qual o gênero com maior tempo de exposição a tela; 
# MAGIC  
# MAGIC -  qual o tipo de acesso mais frequente entre os usuários.

# COMMAND ----------

# MAGIC %md
# MAGIC Primeiramente, vamos analisar se a distribuição de amostras tem uma disparidade muito grande, o que poderia enviesar os resultados observados.

# COMMAND ----------

# importando função pyspark
from pyspark.sql.functions import desc

# Mostrando a quantidade de amostras
dfamostra = df.groupby('Age', 'Screen_Time_Type', 'Gender', 'Day_Type').agg({'Sample_Size': 'sum'})
dfamostra = dfamostra.orderBy(desc('Age'))
display(dfamostra)

# COMMAND ----------

# MAGIC %md
# MAGIC A quantidade de amostras parece ser bem distribuída entre as diferentes variáveis categóricas, portanto, não sendo considerado um possível viés por predomínio de observações de apenas um tipo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Objetivo 1: qual o tempo de exposição a telas entre diferentes faixas etárias

# COMMAND ----------

# Mostar a média de tempo por idade

df_medias = df.where(df["Screen_Time_Type"] == "Total").groupBy('Age').mean('Average_Screen_Time_hours')
df_medias = df_medias.orderBy(desc("avg(Average_Screen_Time_hours)"))
display(df_medias)

# COMMAND ----------

# MAGIC %md
# MAGIC Aqui, podemos ver que o tempo médio de tela é maior quanto maior for a idade do usuário, com a maior média sendo observada para adolescentes de 15 anos.
# MAGIC Uma hipótese para explicar esse fato poderia ser associada ao aumento das demandas escolares que exigem maior tempo para pesquisas online. Será feita uma  analise adicional para observar qual tempo médio para os diferentes tipos de uso por cada faixa etária.

# COMMAND ----------

# Mostar a média de tempo por idade e tipo de acesso

df_medias = df.where(df["Screen_Time_Type"] == "Educational").groupBy('Age','Screen_Time_Type').mean('Average_Screen_Time_hours')
df_medias = df_medias.orderBy(desc("avg(Average_Screen_Time_hours)"))
display(df_medias)

# COMMAND ----------

# MAGIC %md
# MAGIC De fato, o tempo de tela para uso educacional aumenta com o aumento da idade, porém os valores observados estão consideravelmente abaixo da média de tempo total. 
# MAGIC Então, também é possível concluir que o uso recreativo é o principal responsável pelo aumento de tempo online. Será analisado esse dado para ver se é possível comprovar essa segunda hipótese.

# COMMAND ----------

# Mostrar tempo de tela recreativo

df_medias = df.where(df["Screen_Time_Type"] == "Recreational").groupBy('Age','Screen_Time_Type').mean('Average_Screen_Time_hours')
df_medias = df_medias.orderBy(desc("avg(Average_Screen_Time_hours)"))
display(df_medias)

# COMMAND ----------

# MAGIC %md
# MAGIC Pode-se concluir que de fato, o uso recreacional é superior ao educacional.
# MAGIC E que o tempo online e a idade são diretamente proporcionais, ou seja, uma variação em uma medida provoca uma variação no mesmo sentido na outra.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Objetivo 2: qual o gênero com maior tempo de exposição a tela

# COMMAND ----------

# Mostar a média de tempo por gênero

df_medias = df.where(df["Screen_Time_Type"] == "Total").groupBy('Gender').mean('Average_Screen_Time_hours')
df_medias = df_medias.orderBy(desc("avg(Average_Screen_Time_hours)"))
display(df_medias)

# COMMAND ----------

# MAGIC %md
# MAGIC Podemos observar que o gênero masculino tem maior tempo médio de uso de telas. Vamos observar também qual o tipo de uso predominante para cada gênero.

# COMMAND ----------

# Mostrar tempo de tela por gênero e tipo de acesso

df_medias = df.where(df["Screen_Time_Type"] != "Total").groupBy('Gender','Screen_Time_Type').mean('Average_Screen_Time_hours')
df_medias = df_medias.orderBy(desc("avg(Average_Screen_Time_hours)"))
display(df_medias)

# COMMAND ----------

# MAGIC %md
# MAGIC Observamos que quando o uso de telas é para fins recreativos, os homens tendem a passar mais tempo online. Porém o as mulheres usam mais esses recursos quando tem o objetivo educacional.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Objetivo 3: Tipo de acesso mais frequente entre usuários

# COMMAND ----------

# MAGIC %md
# MAGIC Para essa analise, vamos considerar que como crianças pessoas que estão na faixa etária entre 5 e 10 anos e como adolescentes, pessoas que estão acima de 10 anos.

# COMMAND ----------

# Importar funções do pyspark
from pyspark.sql import functions as F

# Criando a coluna "Faixa_Etaria" baseada na coluna "Age"
df_usuarios = df.withColumn(
    "Faixa_Etaria", 
    F.when((df["Age"] >= 5) & (df["Age"] <= 10), "criança")
     .when((df["Age"] > 10) & (df["Age"] <= 15), "adolescente")
)

# Agrupar por genero, faixa etária, tipo de acesso e mostrar a média de tempo de tela
result = df_usuarios.where(df["Screen_Time_Type"] != "Total").groupBy("Gender", "Faixa_Etaria", "Screen_time_type") \
    .agg(F.avg("Average_Screen_Time_hours").alias("Average_Screen_Time_hours"))

# Mostrar o resultado
result = result.orderBy(F.desc("Average_Screen_Time_hours"))
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC Aqui podemos observar que, para o uso recreativo, todos os usuários adolescentes tem mais tempo de tela que os usuários crianças. Também pode-se notar que para o uso recreativo, tanto crianças quanto adolescentes homens ficam mais online que as mulheres.
# MAGIC Porém, para o uso educacional, as adolescentes mulheres usam mais as telas que os homens, quanto que na fase da infância é observado o contrário.

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Criando tabela a partir de dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC O dataframe usado para as analises até agora será salvo como tabela para que seja possível conectar ao power bi e mostrar as analises de forma visual em um dashboard.

# COMMAND ----------

# Salvar o DataFrame como uma tabela permanente
df.write.saveAsTable("sreen_time_tratado")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mostrar tabela criada
# MAGIC select * from sreen_time_tratado limit 5
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Visuzalização no Power Bi

# COMMAND ----------

# MAGIC %md
# MAGIC A tabela criada foi importada para o Power Bi desktop:
# MAGIC ![conectar_powerbi_databricks.png](./conectar_powerbi_databricks.png "conectar_powerbi_databricks.png")
# MAGIC Após ser importada, foram criados os seguintes visuais:![visuais_power_bi_.png](./visuais_power_bi_.png "visuais_power_bi_.png")
# MAGIC Cartões na parte superior indicando a média de tempo para uso receativo, educacional e total;
# MAGIC Gráfico de árvore para visualização de informações de tipo de acesso por usuário (quebra de média de tempo total de telas por tipo de acesso, gênero e faixa etária);
# MAGIC Gráfico de barras clusterizadas e colunas, com tempo de tela por idade e tipos de acesso;
# MAGIC Gráfico de pizza com tempo de tela por gênero;
# MAGIC Filtros para segmentação de dados.
# MAGIC
# MAGIC Para que fosse possível criar as visualizações, foi criada uma nova coluna na tabela importada usando a liguagem Dax, para agrupar as idades por faixa etária.
# MAGIC ![coluna_calculada.png](./coluna_calculada.png "coluna_calculada.png")
# MAGIC
# MAGIC E também foram criadas três medidas para mostrar as informações de tempo de tela por cada tipo de acesso: ![medida_dax.png](./medida_dax.png "medida_dax.png")

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Conclusão
# MAGIC O desenvolvimento do projeto MVP para a disciplina de Engenharia de dados permitiu o aprendizado prático dos conceitos de ETL e tratamento de dados. Além de possibilitar o conhecimento sobre o funcionamento de ferramentas como a Azure, Databricks e Power Bi, e como elas se interagem.
# MAGIC
# MAGIC Além disso, foi possível atingir os objetivos do projeto, com a observação da distribuição de tempo de tela entre crianças e jovens para diferentes fins.
# MAGIC Pode-se concluir que o tempo gasto online aumenta com a idade e o principal fator responsável por esse aumento é o uso recreativo. Além disso, quando são considerados fins recreativos, homens adolescentes passam mais tempo em frente a telas que mulheres na mesma faixa etária.
# MAGIC Com isso, é possível debater quais as melhores condutas para que crianças e adolescentes consigam usufruir das tecnologias existentes, mas garantindo o acesso a conteúdos de qualidade.