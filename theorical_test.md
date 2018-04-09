# Respostas para as perguntas teóricas

### Qual​ ​ o ​ ​ objetivo​ ​ do​ ​ comando​ ​ cache​ ​ em​ ​ Spark?
R: O Apache Spark opera com lazy evaluation. Quando realizamos uma sequência de operações como leitura e contagem de linhas, o Spark irá primeiro carregar o conteúdo do arquivo e depois realizar a contagem. Se executarmos os mesmos comandos novamente, ela fará as duas operações completas, lendo os arquivos para a memória. O comando `cache` força o Spark a manter o valor de um objeto em memória, caso haja memória disponível. Assim, se colocarmos um `variável.cache()` entre a leitura e contagem, ele guardará o conteúdo na memória e fará diretamente a contagem quando for requisitado.

fonte: https://stackoverflow.com/questions/28981359/why-do-we-need-to-call-cache-or-persist-on-a-rdd

### O​ ​ mesmo​ ​ código​ ​ implementado​ ​ em​ ​ Spark​ ​ é​ normalmente​ ​ mais​ ​ rápido​ que​ ​ a ​ ​ implementação​ ​ equivalente​ ​ em MapReduce.​ ​ Por​ ​ quê?
A principal diferença de performance se dá pelo Spark realizar todas as operações em memória, não gravando etapas preliminares no disco, como é o caso do MapReduce.

fonte: https://www.infoq.com/br/articles/mapreduce-vs-spark

### Qual​ ​ é ​ ​ a ​ ​ função​ ​ do​ ​ SparkContext​ ?
O SparkContext é um ambiente gerado para uma aplicação Spark. Nele, passamos configurações como o número de workers, memória que pode ser alocada etc. É uma maneira de dizer ao núcleo Spark a quantidade de recursos que deve ser alocado para aquela aplicação.

fonte: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-SparkContext.html

### Explique​ ​ com​ ​ suas​ ​ palavras​ ​ ​ o ​ ​ que​ ​ é ​ ​ Resilient​ ​ Distributed​ ​ Datasets​​ ​ (RDD).
R: RDD é uma maneira de representar um conjunto de dados distribuido na mémoria de multiplas máquinas em um cluster e pode ser manipulado por um conjunto de funções.

Fonte: https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf


### GroupByKey​ ​ é ​ ​ menos​ ​ eficiente​ ​ que​ ​ reduceByKey​ ​ em​ ​ grandes​ ​ dataset.​ ​ Por​ ​ quê?
As duas funções produzem resultados corretos, mas o reducebyKey, pois o Spark pode combinar o resultado utilizando uma chave comum em cada partição antes de executar um shuffle nos dados. Já no GroupByKey, todos os pares de chave são embaralhados, gerando um fluxo de dados desnecessários na rede.


fonte: https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html


### Explique​ ​ o ​ ​ que​ ​ o ​ ​ código​ ​ Scala​ ​ abaixo​ ​ faz:
```
val​ textFile​​ = sc​.​textFile("hdfs://..."​)

val​​ counts​​ = textFile​.flatMap​(line ​=> line.split("​ "))
            .map(word​​ => (word​,​1))
            .reduceByKey​(\_+\_)

counts​.saveAsTextFile​("hdfs://..."​)
```
R: O código está realizando a leitura de um arquivo, seguido de um mapeamento de cada palavra em um par (palavra, 1) e, em seguida, realizando a soma dos pares, obtendo o número de ocorrências de cada palavra do texto. Finalmente, o resultado é salvo em um arquivo de disco.


