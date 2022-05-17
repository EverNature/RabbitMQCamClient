package main;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MultipleSend {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultipleSend.class);
    final static String EXCHANGE_CAMERAS = "cameras";
    final static String PHOTOS_FOLDER = "photos";
    final static Integer MAX_WAIT = 9;
    boolean stop;
    
    Random random;
    List<Path> lista;
    
    public MultipleSend() {
    	random = new Random();
    	lista = getFileNames(PHOTOS_FOLDER);
    	stop = false;
    }
    
    public List<Path> getFileNames(String folder){
    	ForkJoinPool pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
    	List<Path> lista = pool.invoke(new DirectoryTreat(FileSystems.getDefault().getPath(folder).toString()));
    	pool.shutdown();
    	
    	try {
            pool.awaitTermination(2000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    	
    	return lista;
    }
    
    public void register() {
    	ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        Connection connection;
		try {
			connection = connectionFactory.newConnection();
			Channel channel = connection.createChannel();

	        channel.exchangeDeclare(EXCHANGE_CAMERAS, "fanout");
	        
	        while(!stop) {
	        	Thread.sleep(random.nextInt(MAX_WAIT)*1000);
	        	Path path = lista.get(random.nextInt(lista.size()));
	        	byte[] fileContent = Files.readAllBytes(path);
	            byte[] message = Base64.getEncoder().encode(fileContent);
	            
	            channel.basicPublish(EXCHANGE_CAMERAS, "", null, message);

	            LOGGER.info(" [x] Sent '" + path + "'");
	        }
	        
	        channel.close();
	        connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
    
	public synchronized void stop() {
		stop = true;
	}

    public static void main(String[] args) throws IOException {
    	Scanner scanner = new Scanner(System.in);
        MultipleSend ms = new MultipleSend();
        Thread waitThread = new Thread(()-> {
			scanner.nextLine();
			ms.stop();
		});
        waitThread.start();
        ms.register();
        scanner.close();
    }
}
