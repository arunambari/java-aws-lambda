package com.example.demo.poc.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.example.demo.poc.event.TimerEvent;
import com.example.demo.poc.model.LockConstant;
import com.example.demo.poc.model.LockItem;
import com.example.demo.poc.service.LockService;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.example.demo.poc.model.LockConstant.HTTP_GET;
import static com.example.demo.poc.model.LockConstant.HTTP_POST;


public class JobHandler implements RequestHandler<TimerEvent, String> {
    Context context;

    @Override
    public String handleRequest(TimerEvent timerEvent, Context context) {
        try {
            this.context = context;
            LockService lockService = new LockService(timerEvent,context);

            if(!lockService.acquireLock()) {
                log("Lock is held by another DB owner "+lockService.getDBOwnerName()
                        + " Host trying acquire lock to become the new owner is "+lockService.getOwnerName());
                return timerEvent.toString();
            }
            lockService.scheduleLockUpdate();
            String response= processEvent(timerEvent);
            log("Output is :"+response);
            log("Sleeping for :"+ timerEvent.getTestSleepDuration()/1000+" seconds....");
            Thread.sleep(timerEvent.getTestSleepDuration());
            lockService.releaseLock();
            return response;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    private String processEvent(TimerEvent timerEvent) throws IOException {
        switch (timerEvent.getHttpMethod()) {

            case HTTP_POST:
                processPOSTRequest(timerEvent);
                break;
            default:
                processGETRequest(timerEvent);
                break;


        }
        return timerEvent.toString();
    }

    private static void processGETRequest(TimerEvent timerEvent) throws IOException {
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
    }

    private static void processPOSTRequest(TimerEvent timerEvent) throws IOException {
        try (CloseableHttpClient httpClient = HttpClients.createDefault())  {
            HttpPost request = new HttpPost(timerEvent.getRemoteUrl());
            request.addHeader("content-type","application/json");
            request.addHeader("accept","application/json");
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                if(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    HttpEntity entity =  response.getEntity();
                    timerEvent.setResponse(EntityUtils.toString(entity));

                }
            }
        }
    }

    private void log(String message) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        context.getLogger().log(dtf.format(now) +"---"+message);
    }
}
