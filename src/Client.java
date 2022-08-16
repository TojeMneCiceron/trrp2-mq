import com.rabbitmq.client.*;
import org.apache.commons.lang3.SerializationUtils;
import ru.psu.movs.trrp.socketmq.Row;
import ru.psu.movs.trrp.socketmq.api.MQRequest;
import ru.psu.movs.trrp.socketmq.api.SocketRequest;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.TimeoutException;

public class Client implements Runnable {
    private final Channel mqChannel;
    private final AppConfig appConfig;
    private static RunArg runArg;

    public Client() throws IOException, TimeoutException {
        appConfig = AppConfig.load();
        AppConfig.MessageQueueServer messageQueueServer = appConfig.messageQueueServer;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(messageQueueServer.host);
        factory.setPort(messageQueueServer.port);
        factory.setUsername(messageQueueServer.username);
        factory.setPassword(messageQueueServer.password);
        mqChannel = factory.newConnection().createChannel();
    }

    private enum RunArg {
        Socket, MQ
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        System.out.println(args[0]);

        try {
            if (args[0].equals("s"))
                runArg = RunArg.Socket;
            else if (args[0].equals("mq"))
                runArg = RunArg.MQ;
            else
                throw new Exception("Wrong argument");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        new Client().run();
    }

    @Override
    public void run() {

        System.out.println("[Client] Running");

//        Database.CreateDB();
        Database.DropTables();
        Database.CreateTables();
//        Database.WriteData(rows);
        GetData();
    }

    private void GetData() {
        Socket socket;
        try {
            socket = new Socket(appConfig.cacheServer.host, appConfig.cacheServer.port);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        switch (runArg) {
            case Socket:
                System.out.println("Requested socket");
                try (ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {
                    oos.writeObject(new SocketRequest());

                    try (ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

                        int size = ois.read();

                        System.out.println("Reading " + size + " rows...");
                        for (int i = 0; i < size; i++) {
                            Object obj = ois.readObject();
                            Row row = (Row) obj;
                            System.out.println("\t>Received row: " + row.description);
                            Database.WriteRow(row);
                        }
                        System.out.println("All rows were read");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                }
                break;
            case MQ:
                System.out.println("Requested mq");

                try {
                    String queueName = mqChannel.queueDeclare("justsendmesmth", false, false, false, null).getQueue();
//                    System.out.println(queueName);

                    try (ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {
                        oos.writeObject(new MQRequest(queueName));
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                    }

                    Consumer consumer = new DefaultConsumer(mqChannel) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                            Row row = SerializationUtils.deserialize(body);
                            System.out.println("\t>Received row: " + row.description);
                            Database.WriteRow(row);
                        }
                    };
                    mqChannel.basicConsume(queueName, true, consumer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }
}