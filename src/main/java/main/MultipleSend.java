package main;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.imageio.ImageIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class MultipleSend {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultipleSend.class);
    
    final static String EXCHANGE_CAMERAS = "cameras";
    final static String DLX_EXCHANGE_NAME = "deadLetter";
	final static String DLX_QUEUE_NAME = "deadLetterQueue";
	
    final static String PHOTOS_FOLDER = "photos";
    final static String TO_SEND = "toSend";
    final static Integer MAX_WAIT = 9;
    boolean stop;
    boolean connected;
    boolean first;
    
    Random random;
    List<Path> lista;
    ExecutorService executor;
    
    Integer uuid;
    
    public MultipleSend() {
    	random = new Random();
    	lista = getFileNames(PHOTOS_FOLDER);
    	stop = false;
    	connected = false;
    	first = true;
    	executor = Executors.newCachedThreadPool();
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
		try {
			ConnectionFactory connectionFactory = new ConnectionFactory();
			InputStream input = new FileInputStream("conf.properties");
			Properties prop = new Properties();

            prop.load(input);
            connectionFactory.setHost(prop.getProperty("host"));
            connectionFactory.setUsername(prop.getProperty("username"));
            connectionFactory.setPassword(prop.getProperty("password"));
            uuid= Integer.parseInt(prop.getProperty("uuid"));
			input.close();
			
			Connection connection = null;
			Channel channel = null;
			String tag = null;
			
			while(!stop) {
				if(!connected)
				{
					try {
						connection = connectionFactory.newConnection();
						channel = connection.createChannel();

				        channel.exchangeDeclare(EXCHANGE_CAMERAS, "fanout", true, false, false, null);
			        	channel.exchangeDeclare(DLX_EXCHANGE_NAME, "fanout", true, false, false, null);
						
						channel.queueDeclare(DLX_QUEUE_NAME, true, false, true, null);
						channel.queueBind(DLX_QUEUE_NAME,DLX_EXCHANGE_NAME,"");
						
						boolean autoack = true;
			            
						ConsumerClient consumer = new ConsumerClient(channel, executor);
						tag = channel.basicConsume(DLX_QUEUE_NAME, autoack, consumer);
						connected = true;
					} catch (ConnectException e) {
						System.out.println("z");
					}
				}
				
				if(connected && first) {
					List<Path> list = getFileNames(TO_SEND);
					for(Path p:list)
					{
						byte[] fileContent = Files.readAllBytes(p);
						sendPhoto(fileContent, p, channel);
						p.toFile().delete();
					}
				}
				
	        	Thread.sleep(random.nextInt(MAX_WAIT)*1000);
	        	Path path = lista.get(random.nextInt(lista.size()));
	        	byte[] fileContent = Files.readAllBytes(path);
	        	
	        	try {
	        		if(connection.isOpen()) {
	        			sendPhoto(fileContent, path, channel);
		            }
				} catch (NullPointerException e) {
					InputStream is = new ByteArrayInputStream(fileContent);
	            	BufferedImage bi = ImageIO.read(is);
                	File file = new File(FileSystems.getDefault().getPath(TO_SEND, (new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new java.util.Date()) + ".jpg")).toString());
                    ImageIO.write(bi, "jpg", file);
                    is.close();
				}
	        }
	        if(channel != null && connection != null)
	        {
	        	channel.basicCancel(tag);
		        channel.close();
		        connection.close();
	        }
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
    }
    
	public synchronized void stop() {
		stop = true;
	}
	
	public void sendPhoto(byte[] fileContent, Path path, Channel channel) throws NoSuchAlgorithmException, IOException {
		byte[] message = Base64.getEncoder().encode(fileContent);
    	MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        messageDigest.update(message);
        
        String filename = path.getFileName().toString();
        
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties().builder();
        
		Map<String,Object> headerMap = new HashMap<String, Object>();
		headerMap.put("hash", new String(messageDigest.digest()));
		headerMap.put("camera_id", uuid.toString());
		headerMap.put("filename", filename.substring(0, filename.lastIndexOf(".")));
		builder.headers(headerMap);
		builder.deliveryMode(2);
        
        channel.basicPublish(EXCHANGE_CAMERAS, "", builder.build(), message);

        LOGGER.info(" [x] Sent '" + path + "'");
	}
	
	public class ConsumerClient extends DefaultConsumer {

    	ExecutorService executor;
    	Channel channel;

		public ConsumerClient(Channel channel, ExecutorService executor) {
			super(channel);
			this.executor = executor;
			this.channel = channel;
		}

		@Override
		public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
				throws IOException {
			String message = new String(body, StandardCharsets.UTF_8);

			try {
                executor.submit(new Runnable() {
                    public void run() {
                    	try {
							channel.basicPublish(EXCHANGE_CAMERAS, "", properties, message.getBytes());
						} catch (IOException e) {
							e.printStackTrace();
						}
                    }
                });
            } catch (Exception e) {
                LOGGER.error("", e);
            }	
		}
    }

    public boolean isStop() {
		return stop;
	}

	public static void main(String[] args) throws IOException {
    	Scanner scanner = new Scanner(System.in);
        MultipleSend ms = new MultipleSend();
        Thread waitThread = new Thread(()-> {
			scanner.nextLine();
			ms.stop();
		});
        waitThread.start();
        while(!ms.isStop()) {
        	ms.register();
        }
        scanner.close();
    }
}
