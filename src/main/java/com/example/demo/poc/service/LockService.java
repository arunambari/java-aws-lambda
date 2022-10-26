package com.example.demo.poc.service;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.example.demo.poc.event.TimerEvent;
import com.example.demo.poc.handler.JobHandler;
import com.example.demo.poc.model.LockConstant;
import com.example.demo.poc.model.LockItem;


//LockItem{key='sit-lock', leaseDuration=120000, startTime=12345669, active=true}
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
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
        mapper.save(lockItem);
        return lockItem;

    }

    public LockItem putItem() {
        LockItem lockItem = getItem(key);
        if(lockItem == null) {
            lockItem = new LockItem(key,leaseDuration,startTime,true,ownerName,System.currentTimeMillis());
        }
        boolean ownerOfTheLock = lockItem.getOwnerName().equals(ownerName);

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

        mapper.save(lockItem);

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
        long delayInSeconds = leaseDuration/4;
        log("Scheduling release lock after "+delayInSeconds);
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(()->putItem(),  delayInSeconds, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) {
        LockService lockService = new LockService();
       //  lockService.deleteItem("sit-lock");
        lockService.putItem();
        System.out.println(lockService.getItem("sit-lock"));
    }
    private void log(String message) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        context.getLogger().log(dtf.format(now) +"---"+message);
    }
}
