package com.yelp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, functions}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, EMLDAOptimizer, LDA, OnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import com.yelp.Models
/**
  * Created by goutamvenkat on 4/7/2017.w
  */
object Main {
    def main(args: Array[String]) {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        val sc = createContext;
        val session = SparkSession.builder().getOrCreate();
        import session.implicits._
        val (business_df, review_df) = loadData(session);
        val joined_df = review_df.join(business_df, business_df("business_id") === review_df("business_id"));
        val new_joined_df = joined_df.filter(functions.array_contains(joined_df("categories"), "Mexican") && $"text".contains("burrito"));

        val (corpus, vocabArray, actualNumTokens) = preprocess(sc, new_joined_df.select("text"), "data/stopwords.txt");
        val actualCorpusSize = corpus.count()
        val actualVocabSize = vocabArray.length

        val lda = new LDA();
        lda.setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0/actualCorpusSize))
            .setK(50)
        val startTime = System.nanoTime()
        val ldaModel = lda.run(corpus)
        val elapsed = (System.nanoTime() - startTime) / 1e9
        println(s"Finished training LDA model.  Summary:")
        println(s"\t Training time: $elapsed sec")

        // Print the topics, showing the top-weighted terms for each topic.
        val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
        val topics = topicIndices.map { case (terms, termWeights) =>
            terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
        }
        println(s"50 topics:")
        topics.zipWithIndex.foreach { case (topic, i) =>
            println(s"TOPIC $i")
            topic.foreach { case (term, weight) =>
                println(s"$term\t$weight")
            }
            println()
        }
        sc.stop()

    }
    def loadData(sqlContext: SparkSession): (DataFrame, DataFrame) = {
        val business_df = sqlContext.read.json("data/yelp_academic_dataset_business.json");
        val review_df = sqlContext.read.json("data/yelp_academic_dataset_review.json");
        business_df.createOrReplaceTempView("BUSINESSES");
        review_df.createOrReplaceTempView("REVIEWS");
        import sqlContext.implicits._;

        val new_business_df = business_df.filter($"categories".isNotNull && functions.array_contains(business_df("categories"), "Restaurants"));
//        business_df.where(functions.array_contains(business_df("categories"), "Italian")).take(5).foreach(println);
//        val new_business_df = business_df.where(business_df("categories").contains("Restaurants"));
        val new_review_df = review_df.filter($"business_id".isNotNull);
        (new_business_df, new_review_df)
    }
    def createContext(appName: String, masterUrl: String): SparkContext = {
        val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
        new SparkContext(conf)
    }

    def createContext(appName: String): SparkContext = createContext(appName, "local")
    def createContext: SparkContext = createContext("CSE 6242 Yelp", "local")
    /**
      * Load documents, tokenize them, create vocabulary, and prepare documents as term count vectors.
      * @return (corpus, vocabulary as array, total token count in corpus)
      */
    private def preprocess(
                              sc: SparkContext,
                              input_df: DataFrame,
                              stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long) = {

        val spark = SparkSession
            .builder
            .getOrCreate()
        import spark.implicits._

        // Get dataset of document texts
        // One document per line in each text file. If the input consists of many small files,
        // this can result in a large number of small partitions, which can degrade performance.
        // In this case, consider using coalesce() to create fewer, larger partitions.
//        val df = sc.textFile(paths.mkString(",")).toDF("docs")
        val df = input_df.toDF("docs")
        val customizedStopWords: Array[String] = if (stopwordFile.isEmpty) {
            Array.empty[String]
        } else {
            val stopWordText = sc.textFile(stopwordFile).collect()
            stopWordText.flatMap(_.stripMargin.split("\\s+"))
        }
        val tokenizer = new RegexTokenizer()
            .setInputCol("docs")
            .setOutputCol("rawTokens")
        val stopWordsRemover = new StopWordsRemover()
            .setInputCol("rawTokens")
            .setOutputCol("tokens")
        stopWordsRemover.setStopWords(stopWordsRemover.getStopWords ++ customizedStopWords)
        val countVectorizer = new CountVectorizer()
            .setInputCol("tokens")
            .setOutputCol("features")

        val pipeline = new Pipeline()
            .setStages(Array(tokenizer, stopWordsRemover, countVectorizer))

        val model = pipeline.fit(df)
        val documents = model.transform(df)
            .select("features")
            .rdd
            .map { case Row(features: MLVector) => Vectors.fromML(features) }
            .zipWithIndex()
            .map(_.swap)

        (documents,
            model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary,  // vocabulary
            documents.map(_._2.numActives).sum().toLong) // total token count
    }
}