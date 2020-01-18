import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class Worker {
    private String processedUrl;
    private String unprocessedUrl;
    private AmazonSQS sqs;
    private StanfordCoreNLP NERPipeline;
    private StanfordCoreNLP sentimentPipeline;
    private List<String> entitiesToKeep;
    private Logger logger;

    public Worker(String processedUrl, String unprocessedUrl) {
        //setup logger
        logger = Logger.getLogger("MyLog");
        FileHandler fh;

        try {

            // This block configure the logger with handler and formatter
            fh = new FileHandler("/home/ec2-user/log.txt");//"/Users/yoav.keissar/Documents/log.txt");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

            // the following statement is used to log any messages
            logger.info("Logger Stated!");

        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Worker started at: " + new Date(System.currentTimeMillis()));

        this.processedUrl = processedUrl;
        this.unprocessedUrl = unprocessedUrl;
        sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .build();

        entitiesToKeep = new ArrayList<String>();
        entitiesToKeep.add("PERSON");
        entitiesToKeep.add("LOCATION");
        entitiesToKeep.add("ORGANIZATION");

        Properties props1 = new Properties();
        props1.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        NERPipeline = new StanfordCoreNLP(props1);

        Properties props2 = new Properties();
        props2.put("annotators", "tokenize, ssplit, parse, sentiment");
        sentimentPipeline = new StanfordCoreNLP(props2);

    }

    //----------------------------------AWS------------------------------------

    public Message readMessagesLookForFirstLine(String lookFor) {
        try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(unprocessedUrl);
            receiveMessageRequest.withWaitTimeSeconds(3);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            for (Message message : messages) {
                String firstLine = message.getBody().substring(0, message.getBody().indexOf("\n"));
                if (firstLine.equals(lookFor)) {
                    sqs.changeMessageVisibility(unprocessedUrl, message.getReceiptHandle(), 30);
                    return message;
                }
            }
        } catch (AmazonServiceException ase) {
            logger.info("Caught Exception: " + ase.getMessage());
            logger.info("Reponse Status Code: " + ase.getStatusCode());
            logger.info("Error Code: " + ase.getErrorCode());
            logger.info("Request ID: " + ase.getRequestId());
        }
        return null;

    }

    public void sendMessage(String message) {
        sqs.sendMessage(new SendMessageRequest(processedUrl, message));
        logger.info(String.format("Sending message '%s' to queue with url - %s.", message, processedUrl));
    }

    public void deleteMessage(Message message) {
        sqs.deleteMessage(new DeleteMessageRequest(unprocessedUrl, message.getReceiptHandle()));
    }

    //-----------------------------MessageProcess------------------------------

    public String processReview(String review) {

        String toReturn = review.substring(review.indexOf("{"));
        int rating = findRating(toReturn);
        int sentiment = findSentiment(findText(toReturn));

        //coloring review
        switch (sentiment) {
            case 0: {
                toReturn = "<font color=#930000>" + toReturn + "</font>";
                break;
            }
            case 1: {
                toReturn = "<font color=#FF0000>" + toReturn + "</font>";
                break;
            }
            case 2: {
                toReturn = "<font color=#000000>" + toReturn + "</font>";
                break;
            }
            case 3: {
                toReturn = "<font color=#0FFF00>" + toReturn + "</font>";
                break;
            }
            case 4: {
                toReturn = "<font color=#088300>" + toReturn + "</font>";
                break;
            }
        }

        int sarcastic = rating - sentiment;
        toReturn += getEntities(review);
        return sarcastic > 3 ? toReturn + " sarcastic\n" : toReturn + " not_sarcastic\n";
    }

    public int findRating(String review) {
        JSONParser parser = new JSONParser();
        JSONObject obj2 = null;
        try {
            obj2 = (JSONObject) parser.parse(review);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Long rating = (Long) obj2.get("rating");
        return rating.intValue();
    }

    public String findText(String review) {
        JSONParser parser = new JSONParser();
        JSONObject obj2 = null;
        try {
            obj2 = (JSONObject) parser.parse(review);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String text = (String) obj2.get("text");
        return text;
    }

    public int findSentiment(String review) {
        int mainSentiment = 0;
        if (review != null && review.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(review);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        return mainSentiment;
    }

    public String getEntities(String review) {
        StringBuilder entities = new StringBuilder().append(" [");

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);

        // run all Annotators on this text
        NERPipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(CoreAnnotations.NamedEntityTagAnnotation.class);
                if (entitiesToKeep.contains(ne))
                    entities.append(ne).append(":").append(word).append(",");
            }
        }

        String str = entities.toString();
        if (str.charAt(str.length() - 1) == ',')
            str = str.substring(0, str.length() - 1);
        return str + "]";
    }
}
