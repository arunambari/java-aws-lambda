package com.example.demo.poc.service;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.model.*;
import com.example.demo.poc.event.TimerEvent;
import com.example.demo.poc.model.LockItem;


//LockItem{key='sit-lock', leaseDuration=120000, startTime=12345669, active=true}
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.core.JsonProcessingException;

import static com.example.demo.poc.model.LockConstant.*;

public class LockService {
    String regionString= "us-east-2";
    String tableName = "locktable";
    Context context;
    String key;
    long leaseDuration;

    String ownerName;

    long startTime;


    public LockService() {
        this.regionString = "us-east-2";
        this.tableName = TABLE_NAME;
        this.context = context;
        this.key = LOCK_KEY;
        this.leaseDuration = LEASE_DURATION;
        this.startTime = System.currentTimeMillis();
        this.ownerName = generateOwnerNameFromLocalhost();

    }
    public LockService( TimerEvent timerEvent, Context context) {
        this.regionString = timerEvent.getLockTableRegion();
        this.tableName = timerEvent.getLockTable();
        this.context = context;
        this.key = timerEvent.getLockKey();
        this.leaseDuration = timerEvent.getLeaseDuration();
        this.startTime = System.currentTimeMillis();
        this.ownerName = generateOwnerNameFromLocalhost();
    }

    public String getOwnerName() {
        return ownerName;
    }

    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(regionString).build();
    public LockItem  addItem(String key,long leaseDuration, Long startTime, boolean active,String ownerName) throws JsonProcessingException {
        this.key = key;
        this.startTime = startTime;
        this.leaseDuration = leaseDuration;
        this.ownerName = ownerName;
        DynamoDBMapper mapper = new DynamoDBMapper(client);
        LockItem lockItem = mapper.load(LockItem.class,key);
        log("Inside addItem:: what is in DB currently is "+lockItem);

        lockItem = new LockItem(key,leaseDuration,startTime,true,ownerName,startTime);

        mapper.save(lockItem);

        lockItem = mapper.load(LockItem.class,key);
        log("Inside addItem after saving :: "+lockItem);
        return lockItem;
    }

    public LockItem releaseLock(){

        LockItem lockItem = getItem(key);

        lockItem.setActive(false); //release lock
        DynamoDBMapper mapper = new DynamoDBMapper(client);
        log("updating LockItem in setStatus method"+lockItem);

        DynamoDBSaveExpression saveExpression = getReleaseLockDynamicExpression();
        try {
            mapper.save(lockItem, saveExpression);
        }catch (ConditionalCheckFailedException e) {
           // e.printStackTrace();
            log("conditional check failed for owner "+ownerName+ "  error cause :"+e.getLocalizedMessage());
        }
        return lockItem;

    }

    private DynamoDBSaveExpression getReleaseLockDynamicExpression() {
        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression();
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();
        // expected.put("key", new ExpectedAttributeValue().withValue(new AttributeValue().withS(key)));
        expected.put("active", new ExpectedAttributeValue().withValue(new AttributeValue().withN("1")));
        expected.put("ownerName", new ExpectedAttributeValue(new AttributeValue().withS(ownerName)));
        saveExpression.setExpected(expected);
        saveExpression.setConditionalOperator(ConditionalOperator.AND);
        return saveExpression;
    }

    public void updateLockBasedOnScheduler() throws JsonProcessingException {
        log("Inside scheduleLockItem");
         putItem();
    }

    public boolean putItem() throws JsonProcessingException {
        LockItem lockItem = getItem(key);
        log("-----------Begin PutItem--------");
        log("Inside putItem:: what is in DB currently is "+lockItem);

        if(lockItem == null) {
            lockItem = new LockItem(key,leaseDuration,startTime,true,ownerName,System.currentTimeMillis());
        }
        long currentTime = System.currentTimeMillis();
        boolean ownerOfTheLock = lockItem.getOwnerName().equals(ownerName);
        long diffCurrentTimeWithLeaseDuration = System.currentTimeMillis() - lockItem.getLeaseDuration();
        if(lockItem.getStartTime() < diffCurrentTimeWithLeaseDuration) {
            log("Inside putItem:: Inside DB:: [isStartTime < diffCurrentTimeWithLeaseDuration is TRUE] and ownerOfTheLock is "+ownerOfTheLock);
            lockItem.setStartTime(currentTime);
            lockItem.setActive(true);
        } else {
            log("Inside putItem:: Inside DB:: [startTime >  diffCurrentTimeWithLeaseDuration is TRUE] and ownerOfTheLock is "+ownerOfTheLock);

        }
        lockItem.setLeaseRenewalTimeInMilliSeconds(currentTime);
        lockItem.setOwnerName(ownerName);
        lockItem.setLeaseDuration(leaseDuration);

        DynamoDBMapper mapper = new DynamoDBMapper(client);

        DynamoDBSaveExpression saveExpression = getPutItemDBSaveExpression(diffCurrentTimeWithLeaseDuration);
        try {
          mapper.save(lockItem, saveExpression);
          log("Inside putItem:: what is in DB currently AFTER SAVING is "+lockItem);

      }catch (ConditionalCheckFailedException e) {
          log("conditional check failed for owner "+ownerName+ "  error cause :"+e.getMessage());
          return false;
      }
        log("-----------End PutItem--------");
        return true;
    }

    private DynamoDBSaveExpression getPutItemDBSaveExpression(long diffCurrentTimeWithLeaseDuration) {
        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression();
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();


        expected.put("active", new ExpectedAttributeValue().withValue(new AttributeValue().withN("0")));
        expected.put("ownerName", new ExpectedAttributeValue(new AttributeValue().withS(ownerName)));
        expected.put("startTime", new ExpectedAttributeValue().withValue(new AttributeValue().withN(Long.toString(diffCurrentTimeWithLeaseDuration))).withComparisonOperator(ComparisonOperator.LT));

        saveExpression.setExpected(expected);
        saveExpression.setConditionalOperator(ConditionalOperator.OR);
        return saveExpression;
    }

    public LockItem getItem(String key) {
        DynamoDBMapper mapper = new DynamoDBMapper(client);

        LockItem lockItem = mapper.load(LockItem.class,key);

        return lockItem;
    }

    public void deleteItem(String key) {
        HashMap<String,AttributeValue> key_to_get =
                new HashMap<String,AttributeValue>();

        key_to_get.put("key", new AttributeValue(key));

        client.deleteItem(TABLE_NAME,key_to_get);

    }
    public  static final String generateOwnerNameFromLocalhost() {
        try {
            return Inet4Address.getLocalHost().getHostName() + UUID.randomUUID().toString();
        } catch (final UnknownHostException e) {
            return UUID.randomUUID().toString();
        }
    }
    public void scheduleLockUpdate() {
        long delayInMillis = leaseDuration/4;
        log("Scheduling release lock after "+delayInMillis);
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(()-> {
            try {
                updateLockBasedOnScheduler();
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        },  500L,delayInMillis, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) throws JsonProcessingException {
        LockService lockService = new LockService();
       //  lockService.deleteItem("sit-lock");
      //  lockService.addItem("sit-lock",120000,System.currentTimeMillis(),true,generateOwnerNameFromLocalhost());
        lockService.scheduleLockUpdate();
        System.out.println(lockService.getItem("sit-lock"));
    }
    private void log(String message) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        if(context!=null)
           context.getLogger().log(dtf.format(now) +"---"+message);
        else
            System.out.println(dtf.format(now) +"---"+message);
    }
}
