package com.example.demo.poc.model;

public interface LockConstant {
      String TABLE_NAME="locktable";
      String LOCK_KEY = "sit-lock";

     long  LEASE_DURATION=120000L;
     String DEFAULT_REGION="us-east-2";
    String HTTP_POST="POST";
    String HTTP_GET="GET";
}
