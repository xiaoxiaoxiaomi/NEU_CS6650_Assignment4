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
    String accessKeyId = "ASIA5G5IHQACV4AR3P6R";
    String secretAccessKey = "BxCcFtUsvX0cXsmbB2q0mMwQ2hN1+eUK206lloNV";
    String sessionToken = "FwoGZXIvYXdzEAcaDMHpGM+X5kmlsezxMSLLATMqac75RU1hrlm5bZ0zFKN6P31gVO3allKjCVdCnT/r7G/ZmNZHP/uGnWYrZPikqQo3qL0nZ/wypgjgd3lENykYyhyRoA/EOgCXrYR1cR7lDg1l7P+YaoH2gfEhfb2jPXCUQKlJ/j6R37t3DR0SmH/+gHoCaJaSqVa0yuqPVottB1U4Y2v27bPybrAG7A7UNtiIpe0CEqHZQExYVGgTuEEl82ciZSuGzaNdVuLASsyGaUtMqamsY0lnuZ85m3l3LVOkrcogX6x60rpxKOvo4aEGMi3dcnMIAG0a9AhtVYqd6TrIb2f/AIvoe4UX1QQSdTbVI5sa6uQjJ8LI/k7F2lI=";
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
