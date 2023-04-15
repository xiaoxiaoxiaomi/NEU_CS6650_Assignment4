package dao;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.utils.ImmutableMap;

public class SwipeDataDynamoDao {

  private DynamoDbClient dynamoDbClient;

  public SwipeDataDynamoDao() {
    String accessKeyId = "ASIA5G5IHQACTBFWTR4Y";
    String secretAccessKey = "uiScbNibsxBkm/kyBH3X78wXbc+9QwCvKbiWcR7U";
    String sessionToken = "FwoGZXIvYXdzEB8aDL+n+UFEdcIzv++Z8CLLAUs3Agaj2XsiUbMyDwofOf+pi8SWb4BVjyqBVLtP0MOjg/LMxu0sR16N8alsVWtSd7P0GzigD1rcjoPIOeAyr/4qglWU189aMnDRT1jFKmLc5MaxtYCMl9A4YhLtNd/RMwy/qpi0v6JQ5YbKWepMPMExuGheWwXSmKUSvNJt0/gp0T4X1UMwk+dignRBrQ3tVBP6XRXByyw/InspK+X8gf1OhvJ06eRc3AiYcjGytv/iJms3DSFIx+sQ9gnz/CUFPqqYsxF/wsnP5+DcKPGD56EGMi0RG70/U2J/Op5OAytSuhr7qUSuqRL4A20w2nD5KJk1VP7rxX1L0wSBWSmdgxw=";
    AwsSessionCredentials awsCredentials = AwsSessionCredentials.create(accessKeyId,
        secretAccessKey, sessionToken);
    StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
        awsCredentials);
    dynamoDbClient = DynamoDbClient.builder()
        .region(Region.US_WEST_2)
        .credentialsProvider(credentialsProvider)
        .build();
  }

    public void updateUserLikesDislikes(String swiper, String leftOrRight) {
    String tableName = "user_likes_dislikes";
    String columnName = leftOrRight.equals("right") ? "numLikes" : "numDislikes";
    UpdateItemRequest updateRequest = UpdateItemRequest.builder()
        .tableName(tableName)
        .key(java.util.Collections.singletonMap("swiper",
            AttributeValue.builder().s(swiper).build()))
        .updateExpression("ADD " + columnName + " :incr")
        .expressionAttributeValues(
            java.util.Collections.singletonMap(":incr", AttributeValue.builder().n("1").build()))
        .returnValues(ReturnValue.UPDATED_NEW)
        .build();
    try {
      dynamoDbClient.updateItem(updateRequest);
    } catch (DynamoDbException e) {
      System.err.println("Unable to update user likes/dislikes for swiper: " + swiper);
      System.err.println(e.getMessage());
    }
  }

  public void insertUserSwipeRight(String swiper, String swipee) {
    String tableName = "user_swipe_right";
    long timestamp = System.currentTimeMillis();
    PutItemRequest putRequest = PutItemRequest.builder()
        .tableName(tableName)
        .item(ImmutableMap.of(
            "swiper", AttributeValue.builder().s(swiper).build(),
            "swipee", AttributeValue.builder().s(swipee).build(),
            "timestamp", AttributeValue.builder().n(String.valueOf(timestamp)).build()))
        .build();
    try {
      dynamoDbClient.putItem(putRequest);
    } catch (DynamoDbException e) {
      System.err.println(
          "Unable to insert user swipe right event for swiper: " + swiper + " and swipee: "
              + swipee);
      System.err.println(e.getMessage());
    }
  }
}
