package org.upm.storm.tttProducer;

/**
 * Hello world!
 *
 */
public class App 
{


    public static void main( String[] args )
    {
      String mode = args[0];
      String consumerKey = args[1];
      String consumerSecret = args[2];
      String token = args[3];
      String secret  = args[4];
      String kafkaBrokerUrl = args[5];
      String fileName = args[6];
        try {
            KafkaProducer.run(
                    consumerKey,
                    consumerSecret,
                    token,
                    secret,
                    mode,
                    kafkaBrokerUrl,
                    fileName
            );
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}
