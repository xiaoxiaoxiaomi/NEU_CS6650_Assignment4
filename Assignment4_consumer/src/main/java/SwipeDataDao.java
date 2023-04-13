import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.commons.dbcp2.*;

public class SwipeDataDao {

  private static BasicDataSource dataSource;

  public SwipeDataDao() {
    dataSource = DBCPDataSource.getDataSource();
  }

  public void updateUserLikesDislikes(String swiper, String leftOrRight) {
    Connection connection = null;
    PreparedStatement statement = null;
    String columnName = leftOrRight.equals("right") ? "numLikes" : "numDislikes";
    String sql =
        "INSERT INTO user_likes_dislikes (swiper, numLikes, numDislikes) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE "
            + columnName + " = " + columnName + " + 1";
    try {
      connection = dataSource.getConnection();
      statement = connection.prepareStatement(sql);
      statement.setString(1, swiper);
      statement.setInt(2, leftOrRight.equals("right") ? 1 : 0);
      statement.setInt(3, leftOrRight.equals("right") ? 0 : 1);
      statement.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException se) {
        se.printStackTrace();
      }
    }
  }

  public void insertUserSwipeRight(String swiper, String swipee) {
    Connection connection = null;
    PreparedStatement statement = null;
    String sql = "INSERT INTO user_swipe_right (swiper, swipee) VALUES (?, ?)";
    try {
      connection = dataSource.getConnection();
      statement = connection.prepareStatement(sql);
      statement = connection.prepareStatement(sql);
      statement.setString(1, swiper);
      statement.setString(2, swipee);
      statement.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (connection != null) {
          connection.close();
        }
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException se) {
        se.printStackTrace();
      }
    }
  }
}
