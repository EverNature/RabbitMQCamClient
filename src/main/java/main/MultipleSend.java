package main;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class MultipleSend {
    private static final Logger logger = LoggerFactory.getLogger(MultipleSend.class);

    final static String EXCHANGE_CAMERAS = "cameras";

    public static void main(String[] args) throws IOException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        Connection connection;
		try {
			connection = connectionFactory.newConnection();
			Channel channel = connection.createChannel();

	        channel.exchangeDeclare(EXCHANGE_CAMERAS, "fanout");

	        for (int i = 0; i < 100; i++) {
	            String message = "Hello world" + i;
	            channel.basicPublish(EXCHANGE_CAMERAS, "", null, message.getBytes());
	            logger.info(" [x] Sent '" + message + "'");
	        }

	        channel.close();
	        connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }
}
