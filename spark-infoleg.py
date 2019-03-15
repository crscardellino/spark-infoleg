# -*- coding: utf-8 -*-

import argparse

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.clustering import LDA
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("Infoleg LDA").getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser("$SPARK_HOME/bin/spark-submit --master $SPARK_CLUSTER ./spark-infoleg.py")
    parser.add_argument("input_file",
                        help="Dirección al archivo de texto del InfoLEG.",
                        metavar="INPUT_FILE")
    parser.add_argument("output_file",
                        help="Dirección al archivo de texto donde imprimir los resultados.",
                        metavar="OUTPUT_FILE")
    parser.add_argument("--topics",
                        default=10,
                        help="Cantidad de tópicos para LDA.",
                        type=int)
    parser.add_argument("--iterations",
                        default=10,
                        help="Cantidad de iteraciones máxima para LDA.",
                        type=int)
    parser.add_argument("--min-df",
                        default=5,
                        help="Mínima cantidad de documentos para considerar una palabra.",
                        type=int)

    args = parser.parse_args()

    text = spark.read.text(args.input_file).withColumnRenamed("value", "law_text")

    tokenizer = RegexTokenizer(inputCol="law_text", outputCol="words",
                               pattern=r"[^\p{L}]+")

    remover = StopWordsRemover(stopWords=StopWordsRemover.loadDefaultStopWords("spanish"),
                               inputCol="words", outputCol="tokens")

    tokenizedLaw = remover.transform(tokenizer.transform(text))

    counter = CountVectorizer(inputCol="tokens", outputCol="term_frequency",
                              minDF=args.min_df)

    counterModel = counter.fit(tokenizedLaw)

    vectorizedLaw = counterModel.transform(tokenizedLaw)

    idf = IDF(inputCol="term_frequency", outputCol="tf_idf")
    tfidfLaw = idf.fit(vectorizedLaw).transform(vectorizedLaw)

    lda = LDA(k=args.topics, maxIter=args.iterations, featuresCol="tf_idf")
    model = lda.fit(tfidfLaw)

    topics = model.describeTopics()
    vocabulary = counterModel.vocabulary

    def map_idx_to_term(indices):
        return [vocabulary[idx] for idx in indices]

    described_topics = topics.withColumn("terms", udf(map_idx_to_term)(topics["termIndices"]))
    
    with open(args.output_file, "w") as fh:
        for idx, row in enumerate(described_topics.select("terms").collect(), start=1):
            print("Tema %d: %s" % (idx, row.terms[1:-1]), file=fh)

