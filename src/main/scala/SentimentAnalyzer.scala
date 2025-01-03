package edu.najah.bigdata

import java.util.Properties
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.ling.CoreAnnotations
import scala.jdk.CollectionConverters._
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

class SentimentAnalyzer extends Serializable {
  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  @transient lazy val pipeline = new StanfordCoreNLP(props)

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