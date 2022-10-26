package com.example.demo.poc.event;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TimerEvent {
    private String remoteUrl;
    private String response;
    private String lockTableRegion="us-east-2";
    private String lockTable="locktable";
    private String lockKey;
    private long leaseDuration;



    public String initialContent() {
        return "(Beginning) "+toString();
    }
    public String withResponseContent() {
        return "(End) "+toString();
    }
}
