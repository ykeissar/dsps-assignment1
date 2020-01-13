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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;


public class Worker {
    private String queueUrl;
    private AmazonSQS sqs;
    private StanfordCoreNLP NERPipeline;
    private StanfordCoreNLP sentimentPipeline;
    private List<String> entitiesToKeep;

    public Worker(String queueUrl) {
        this.queueUrl = queueUrl;
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
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
            receiveMessageRequest.withWaitTimeSeconds(3);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            for (Message message : messages) {
                String firstLine = message.getBody().substring(0, message.getBody().indexOf("\n"));
                if (firstLine.equals(lookFor)) {
                    sqs.changeMessageVisibility(queueUrl, message.getReceiptHandle(), 30);
                    return message;
                } else
                    sqs.changeMessageVisibility(queueUrl, message.getReceiptHandle(), 0);

            }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }
        return null;

    }

    public void sendMessage(String message) {
        sqs.sendMessage(new SendMessageRequest(queueUrl, message));
        System.out.println(String.format("Sending message '%s' to queue with url - %s.", message, queueUrl));
    }

    public void deleteMessage(Message message) {
        sqs.deleteMessage(new DeleteMessageRequest(queueUrl, message.getReceiptHandle()));
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

    public String tempProcessReview(String review) {//TODO delete
        String toReturn = review.substring(review.indexOf("{"));
        int sentiment = new Random().nextInt(5);
        int rating = findRating(toReturn);
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
        return sarcastic > 3 ? toReturn + " sarcastic\n" : toReturn + " not_sarcastic\n";
    }
}
