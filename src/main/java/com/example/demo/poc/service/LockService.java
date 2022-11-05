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
import java.util.Objects;
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


    int counter=0;

    LockItem dbLockItem;


    private LockService() {
        this.regionString = "us-east-2";
        this.tableName = TABLE_NAME;
        this.context = context;
        this.key = LOCK_KEY;
        this.leaseDuration = LEASE_DURATION/2;
        this.ownerName = generateOwnerNameFromLocalhost();

    }
    public LockService( TimerEvent timerEvent, Context context) {
        this.regionString = timerEvent.getLockTableRegion();
        this.tableName = timerEvent.getLockTable();
        this.context = context;
        this.key = timerEvent.getLockKey();
        this.leaseDuration = timerEvent.getLeaseDuration();
        this.ownerName = generateOwnerNameFromLocalhost();


    }

    public String getOwnerName() {
        return ownerName;
    }
    public String getDBOwnerName() {

        return this.dbLockItem.getOwnerName();
    }

    private String getCurrentAndFutureOwner() {
        return  ownerName+" --owner--] [--dbowner :"+ dbLockItem.getOwnerName();
    }
    public boolean acquireLock() throws JsonProcessingException {
        LockItem lockItem = getItem(key);
        lockItem.setActive(1L);
        long currentTime = System.currentTimeMillis();
        long diffCurrentTimeWithLeaseDuration = currentTime - lockItem.getLeaseDuration();
        DynamoDBSaveExpression saveExpression = acquireLockDBSaveExpression(diffCurrentTimeWithLeaseDuration);
        DynamoDBMapper mapper = new DynamoDBMapper(client);

        try {
            mapper.save(lockItem, saveExpression);
            dbLockItem= new LockItem(lockItem);
            log("(ACQUIRE LOCK SUCCESSFUL) "+"["+getCurrentAndFutureOwner()+"]"+dbLockItem);
            return true;
        }catch (ConditionalCheckFailedException e) {
            // e.printStackTrace();
            log("(FAILED UPDATE DURING ACQUIRE LOCK) "+getCurrentAndFutureOwner()+" )");
            return false;
        }

    }
    private DynamoDBSaveExpression acquireLockDBSaveExpression(long diffCurrentTimeWithLeaseDuration) {
        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression();
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();


        return getDynamoDBSaveExpression(diffCurrentTimeWithLeaseDuration, saveExpression, expected);
    }

    private DynamoDBSaveExpression getDynamoDBSaveExpression(long diffCurrentTimeWithLeaseDuration, DynamoDBSaveExpression saveExpression, Map<String, ExpectedAttributeValue> expected) {
        expected.put("ownerName", new ExpectedAttributeValue().withValue(new AttributeValue().withS(ownerName)));
        expected.put("startTime", new ExpectedAttributeValue().withValue(new AttributeValue().withN(Long.toString(diffCurrentTimeWithLeaseDuration))).withComparisonOperator(ComparisonOperator.LT));
        expected.put("active", new ExpectedAttributeValue().withValue(new AttributeValue().withN(Long.toString(0L))));

        saveExpression.setExpected(expected);
        saveExpression.setConditionalOperator(ConditionalOperator.OR);
        return saveExpression;
    }

    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(regionString).build();
    public LockItem  addItem(String key,long leaseDuration, Long startTime, boolean active,String ownerName) throws JsonProcessingException {
        this.key = key;
        this.leaseDuration = leaseDuration;
        this.ownerName = ownerName;
        DynamoDBMapper mapper = new DynamoDBMapper(client);
        LockItem lockItem = mapper.load(LockItem.class,key);
        log("Inside addItem:: what is in DB currently is "+lockItem);

        lockItem = new LockItem(key,leaseDuration,startTime,1L,ownerName,startTime);

        mapper.save(lockItem);

        lockItem = mapper.load(LockItem.class,key);
        log("Inside addItem after saving :: "+lockItem);
        return lockItem;
    }

    public LockItem releaseLock(){

        LockItem lockItem = new LockItem(dbLockItem);


        lockItem.setOwnerName(ownerName);
        lockItem.setActive(0L); //release lock
        DynamoDBMapper mapper = new DynamoDBMapper(client);


        DynamoDBSaveExpression saveExpression = getReleaseLockDynamicExpression();
        try {
            mapper.save(lockItem, saveExpression);
            dbLockItem= new LockItem(lockItem);
            log("(RELEASE LOCK SUCCESSFUL) "+"["+getCurrentAndFutureOwner()+"]"+dbLockItem);
        }catch (ConditionalCheckFailedException e) {
           // e.printStackTrace();
            log("(FAILED UPDATE DURING RELEASE LOCK )conditional check failed for owner "+lockItem.getOwnerName()+ "  error cause :"+e.getCause()+"   DBitem is :"+getDBItem(key));
        }
        return lockItem;

    }

    private DynamoDBSaveExpression getReleaseLockDynamicExpression() {
        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression();
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();
        expected.put("key", new ExpectedAttributeValue().withValue(new AttributeValue().withS(key)));
      //  expected.put("active", new ExpectedAttributeValue().withValue(new AttributeValue().withN("1")));
        expected.put("ownerName", new ExpectedAttributeValue().withValue(new AttributeValue().withS(ownerName)));
        saveExpression.setExpected(expected);
        saveExpression.setConditionalOperator(ConditionalOperator.AND);
        return saveExpression;
    }

    public void updateLockBasedOnScheduler() throws JsonProcessingException {
        log("Inside scheduleLockItem "+counter++);
         putItem("(From Scheduler) ["+getCurrentAndFutureOwner()+"] ");
    }

    private boolean putItem(String calledFrom) throws JsonProcessingException {
        LockItem lockItem;
        if(Objects.isNull(dbLockItem)) {
            lockItem = getItem(key);
            log(calledFrom+" dbLockItem is NULL Inside putItem:: what is in DB currently is "+lockItem);
        }
        else {
            lockItem= new LockItem(dbLockItem);
        }

        log(calledFrom+" Inside putItem:: what is in DB currently is "+lockItem);


        boolean ownerOfTheLock = lockItem.getOwnerName().equals(ownerName);
        long currentTime = System.currentTimeMillis();
        long diffCurrentTimeWithLeaseDuration = currentTime - lockItem.getLeaseDuration();

        lockItem.setLeaseRenewalTimeInMilliSeconds(currentTime);
        lockItem.setOwnerName(ownerName);
        lockItem.setLeaseDuration(leaseDuration);

        DynamoDBMapper mapper = new DynamoDBMapper(client);

        DynamoDBSaveExpression saveExpression = getPutItemDBSaveExpression(diffCurrentTimeWithLeaseDuration);
        try {
          mapper.save(lockItem, saveExpression);
          dbLockItem = new LockItem(lockItem);
          log(calledFrom+" (SUCCESSFUL UPDATE)"+"["+getCurrentAndFutureOwner()+"]"+"Inside putItem:: what is in DB currently AFTER SAVING is "+lockItem);

      }catch (ConditionalCheckFailedException e) {
          log(calledFrom+" (FAILED UPDATE)conditional check failed for owner "+getCurrentAndFutureOwner()+"  error cause :"+e.getMessage()+"  lockItem :"+getItem(key));

          return false;
      }
        return true;
    }

    private DynamoDBSaveExpression getPutItemDBSaveExpression(long diffCurrentTimeWithLeaseDuration) {
        DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression();
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();


        return getDynamoDBSaveExpression(diffCurrentTimeWithLeaseDuration, saveExpression, expected);
    }

    public LockItem getItem(String key) {
        LockItem lockItem = getDBItem(key);
        long currentTime = System.currentTimeMillis();
        if(lockItem == null) {
            lockItem = new LockItem(key,leaseDuration,currentTime,1L,ownerName,System.currentTimeMillis());
        }  else this.dbLockItem = lockItem;
        return lockItem;
    }


    public LockItem getDBItem(String key) {
        DynamoDBMapper mapper = new DynamoDBMapper(client);

        LockItem lockItem = mapper.load(LockItem.class,key);
        log("[Inside  getDBItem() ] "+lockItem);
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
        log("Scheduling  lock update after "+delayInMillis);
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(()-> {
            try {
                updateLockBasedOnScheduler();
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        },  500L,delayInMillis, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) throws Exception {
        LockService lockService = new LockService();
        lockService.getItem(LOCK_KEY);
       //  lockService.deleteItem("sit-lock");
      //  lockService.addItem("sit-lock",120000,System.currentTimeMillis(),true,generateOwnerNameFromLocalhost());
        lockService.releaseLock();
      //  lockService.releaseLock();
      //  Thread.sleep(40000);
      //  lockService.acquireLock();
     //   Thread.sleep(80000);
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
