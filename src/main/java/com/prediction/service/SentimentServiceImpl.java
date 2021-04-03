package com.prediction.service;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

@Service
public class SentimentServiceImpl implements SentimentService {

	private static Logger logger = LogManager.getLogger();
	private static Properties props;
	private static StanfordCoreNLP pipeline;
	    
	static {
		props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
		pipeline = new StanfordCoreNLP(props);
	}
	public int analyse(String tweet) {
        Annotation annotation = pipeline.process(tweet);
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            return RNNCoreAnnotations.getPredictedClass(tree);
        }
        return 0;
    }
}
