import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class Consumer implements Runnable {

  private static final String QUEUE_NAME = "test";
  private Gson gson = new Gson();
  private Connection connection;

  public Consumer(Connection connection) {
    this.connection = connection;
  }

  @Override
  public void run() {
    Channel channel;
    try {
      channel = connection.createChannel();
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);
      channel.basicQos(1);
      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        try {
          handleMessage(message);
        } catch (SQLException e) {
          e.printStackTrace();
        }
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        System.out.println(" [x] Received '" + message + "'");
      };
      channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void handleMessage(String message) throws SQLException {
    SwipeDetails swipeDetails = gson.fromJson(message, SwipeDetails.class);
    String swiper = swipeDetails.getSwiper();
    String swipee = swipeDetails.getSwipee();
    String leftOrRight = swipeDetails.getLeftOrRight();
    SwipeDataDao swipeDataDao = new SwipeDataDao();
    swipeDataDao.updateUserLikesDislikes(swiper, leftOrRight);
    if (leftOrRight.equals("right")) {
      swipeDataDao.insertUserSwipeRight(swiper, swipee);
    }
  }
}