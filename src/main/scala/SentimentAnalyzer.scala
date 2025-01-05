package edu.najah.bigdata

import java.util.Properties
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.ling.CoreAnnotations
import scala.jdk.CollectionConverters._
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

/**
  * Sentiment Analyzer based on the Stanford CoreNLP library
  * Reference: https://stanfordnlp.github.io/CoreNLP/index.html
  */
class SentimentAnalyzer extends Serializable {

  /**
    * Configure the properties of the pipeline and the needed annotators.
    * For full reference regarding the annotators see: https://stanfordnlp.github.io/CoreNLP/annotators.html
    * 
    * Annotators used in the pipeline are:
    * - tokenize: Splits the text into words.
    * - ssplit: Creates sentences from the tokens.
    * - parse: Provides full syntactic analysis.
    * - sentiment: Annotates sentences with the predicted sentiment.
    */
  private val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  @transient lazy private val pipeline = new StanfordCoreNLP(props)

  /**
    * Extracts the sentiment from the provided text.
    * If the text has multiple sentences then it returns the max sentiment.
    *
    * @param text The text to analyze
    * @return -1 for negative, 0 for neutral, and 1 for positive
    */
  def extractSentiment(text: String): Int = {
    val annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala.toSeq
    normalizeSentimentScore(sentences
      .map(sentence => RNNCoreAnnotations.getPredictedClass(sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .max
    )
  }

  private def normalizeSentimentScore(score: Int): Int = {
    score match {
      case 0 | 1 => -1 // negative
      case 2 => 0 // neutral
      case 3 | 4 => 1 // positive
      case _ => 0 // return neutral otherwise
    }
  }
}