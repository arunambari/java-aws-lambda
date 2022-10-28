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
//    public LockItem  addItem(String key,long leaseDuration, Long startTime, boolean active) {
//
//        LockItem   lockItem = new LockItem(key,leaseDuration,startTime,true,ownerName,leaseRenewalTimeInMilliSeconds);
//
//        DynamoDBMapper mapper = new DynamoDBMapper(client);
//        mapper.save(lockItem);
//
//        lockItem = mapper.load(LockItem.class,key);
//        return lockItem;
//    }

    public LockItem releaseLock(){

        LockItem lockItem = getItem(key);

        lockItem.setActive(false); //release lock
        DynamoDBMapper mapper = new DynamoDBMapper(client);
        log("updating LockItem in setStatus method"+lockItem);

        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression();
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();
       // expected.put("key", new ExpectedAttributeValue().withValue(new AttributeValue().withS(key)));
        expected.put("active", new ExpectedAttributeValue().withValue(new AttributeValue().withN("1")));
        expected.put("ownerName", new ExpectedAttributeValue(new AttributeValue().withS(ownerName)));
        saveExpression.setExpected(expected);
        saveExpression.setConditionalOperator(ConditionalOperator.AND);
        try {
            mapper.save(lockItem, saveExpression);
        }catch (ConditionalCheckFailedException e) {
           // e.printStackTrace();
            log("conditional check failed for owner "+ownerName+ "  error cause :"+e.getLocalizedMessage());
        }
        return lockItem;

    }
    public LockItem updateLockBasedOnScheduler()
    {
        log("Inside scheduleLockItem");
        return putItem();
    }

    public LockItem putItem() {
        LockItem lockItem = getItem(key);
        if(lockItem == null) {
            lockItem = new LockItem(key,leaseDuration,startTime,true,ownerName,System.currentTimeMillis());
        }
;        boolean ownerOfTheLock = lockItem.getOwnerName().equals(ownerName);
        long diffCurrentTimeWithLeaseDuration = System.currentTimeMillis() - lockItem.getLeaseDuration();
        long leaseRenewalTime=lockItem.getLeaseRenewalTimeInMilliSeconds();


        log("currentOwner in DB ::"+lockItem.getOwnerName()+" new owner acquiring or updating lock::"+ownerName+ " diffCurrentTimeWithLeaseDuration  seconds ::"+diffCurrentTimeWithLeaseDuration/1000+"  ::: leaseRenewalTime" +

                leaseRenewalTime/1000+" diffleaseRenewl - diffCurrentTimeWithLeaseDuration"+(diffCurrentTimeWithLeaseDuration-leaseRenewalTime)/1000);
        if(!ownerOfTheLock) {
              if(  System.currentTimeMillis() > lockItem.getLeaseRenewalTimeInMilliSeconds()+leaseDuration) {

                  lockItem.setLeaseRenewalTimeInMilliSeconds(System.currentTimeMillis());
                  lockItem.setOwnerName(ownerName);
                  lockItem.setActive(true);
              }
        }  else {
            lockItem.setLeaseRenewalTimeInMilliSeconds(System.currentTimeMillis());
            lockItem.setActive(true);
        }


        DynamoDBMapper mapper = new DynamoDBMapper(client);
        log("updating LockItem in putItem method"+lockItem);

        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression();
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();


        expected.put("active", new ExpectedAttributeValue().withValue(new AttributeValue().withN("0")));
       expected.put("ownerName", new ExpectedAttributeValue(new AttributeValue().withS(ownerName)));
        expected.put("leaseRenewalTimeInMilliSeconds", new ExpectedAttributeValue().withValue(new AttributeValue().withN(Long.toString(diffCurrentTimeWithLeaseDuration))).withComparisonOperator(ComparisonOperator.LT));

        saveExpression.setExpected(expected);
        saveExpression.setConditionalOperator(ConditionalOperator.OR);
      try {
          mapper.save(lockItem, saveExpression);
      }catch (ConditionalCheckFailedException e) {
          log("conditional check failed for owner "+ownerName+ "  error cause :"+e.getMessage());
      }

        return lockItem;
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
    private static final String generateOwnerNameFromLocalhost() {
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
        executorService.scheduleAtFixedRate(()-> updateLockBasedOnScheduler(),  500L,delayInMillis, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) {
        LockService lockService = new LockService();
       //  lockService.deleteItem("sit-lock");
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
