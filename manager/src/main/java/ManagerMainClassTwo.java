public class ManagerMainClassTwo {
    public static void main(String[] args){
        Manager man = new Manager("https://sqs.us-west-2.amazonaws.com/592374997611/MyQueuefa811c98-c773-4ef4-a082-a024f6c55deb",Integer.valueOf(args[1]));
        man.sendMessage("Hello LocalApp - from Manager "+args[1] ,man.getLocalAppQueueUrl());
    }
}
