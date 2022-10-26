package com.example.demo.poc.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.example.demo.poc.event.TimerEvent;
import com.example.demo.poc.model.LockItem;
import com.example.demo.poc.service.LockService;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class JobHandler implements RequestHandler<TimerEvent, String> {
    Context context;

    @Override
    public String handleRequest(TimerEvent timerEvent, Context context) {
        try {
            this.context = context;
            LockService lockService = new LockService(timerEvent,context);
            LockItem lockItem=lockService.putItem();
            if(!lockItem.isActive()) {
                context.getLogger().log("Lock is held by another owner "+lockItem.getOwnerName()
                        + " CurrentOwner is "+lockService.getOwnerName());
                return lockItem.toString();
            }
            lockService.scheduleLockUpdate();
            String response= processEvent(timerEvent);
            log("Output is :"+response);
            log("Sleeping for 180 seconds....");
           // Thread.sleep(180000);
            lockService.releaseLock();
            return response;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    private String processEvent(TimerEvent timerEvent) throws IOException {


        try (CloseableHttpClient httpClient = HttpClients.createDefault())  {
            HttpGet request = new HttpGet(timerEvent.getRemoteUrl());
            request.addHeader("content-type","application/json");
            request.addHeader("accept","application/json");
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                if(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    HttpEntity entity =  response.getEntity();
                    timerEvent.setResponse(EntityUtils.toString(entity));

                }
            }
        }
        return timerEvent.toString();
    }

    private void log(String message) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        context.getLogger().log(dtf.format(now) +"---"+message);
    }
}
