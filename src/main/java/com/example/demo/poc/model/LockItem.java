package com.example.demo.poc.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import static com.example.demo.poc.model.LockConstant.TABLE_NAME;

@DynamoDBTable(tableName= TABLE_NAME)

public class LockItem {

   private  String key;
   private  long leaseDuration;
   private  long startTime;
   private boolean active;

   private String ownerName;
   private long leaseRenewalTimeInMilliSeconds;


    public LockItem() {

    }

    public LockItem(String key, long leaseDuration, long startTime, boolean active, String ownerName, long leaseRenewalTimeInMilliSeconds) {
        this.key = key;
        this.leaseDuration = leaseDuration;
        this.startTime = startTime;
        this.active = active;
        this.ownerName = ownerName;
        this.leaseRenewalTimeInMilliSeconds = leaseRenewalTimeInMilliSeconds;
    }

    @DynamoDBHashKey
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @DynamoDBAttribute
    public String getOwnerName() {
        return ownerName;
    }

    public void setOwnerName(String ownerName) {
        this.ownerName = ownerName;
    }

    @DynamoDBAttribute
    public long getLeaseRenewalTimeInMilliSeconds() {
        return leaseRenewalTimeInMilliSeconds;
    }

    public void setLeaseRenewalTimeInMilliSeconds(long leaseRenewalTimeInMilliSeconds) {
        this.leaseRenewalTimeInMilliSeconds = leaseRenewalTimeInMilliSeconds;
    }

    @DynamoDBAttribute
    public long getLeaseDuration() {
        return leaseDuration;
    }

    public void setLeaseDuration(long leaseDuration) {
        this.leaseDuration = leaseDuration;
    }

    @DynamoDBAttribute
    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    @DynamoDBAttribute
    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public String toString() {
        return "LockItem{" +
                "key='" + key + '\'' +
                ", leaseDuration=" + leaseDuration +
                ", startTime=" + startTime +
                ", active=" + active +
                ", ownerName='" + ownerName + '\'' +
                ", leaseRenewalTimeInMilliSeconds=" + leaseRenewalTimeInMilliSeconds +
                '}';
    }
}
