package com.example.demo.poc.model;

public interface LockConstant {
    public  String TABLE_NAME="locktable";
    public  String LOCK_KEY = "sit-lock";

    public long  LEASE_DURATION=120000L;
}
