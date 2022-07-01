package com.pepperdata;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json.simple.*;
import org.json.simple.parser.*;

import com.uber.m3.promremoteclient.Client;
import com.uber.m3.promremoteclient.Prometheus.TimeSeries;
import com.uber.m3.promremoteclient.Prometheus.WriteRequest;
import com.uber.m3.promremoteclient.Prometheus.Label;
import com.uber.m3.promremoteclient.Prometheus.Sample;

class Writer2 extends Thread {
    public Client client;
    public ArrayList<Label.Builder> labels;
    public Sample.Builder sample;
    
    public Writer2(Client client, ArrayList<Label.Builder> labels, Sample.Builder sample) {
        this.client = client;
        this.labels = labels;
        this.sample = sample;
    }

    public void writeToM3DB() {
        TimeSeries.Builder timeseries = TimeSeries.newBuilder();

        int ind = 0;
        while (this.labels.size() > ind) {
            timeseries.addLabels(this.labels.get(ind));
            ind++;
        }

        timeseries.addSamples(this.sample);

        WriteRequest.Builder writerRequest = WriteRequest.newBuilder();
        writerRequest.addTimeseries(timeseries);

        try {
            this.client.WriteProto(writerRequest.build());
        } catch (IOException e) {
            try {
                TimeUnit.SECONDS.sleep(4);
                System.out.println("Repeat" + " " + this.sample.getTimestamp());
                this.writeToM3DB();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    public void run() {
        this.writeToM3DB();
    }
}

public class BulkWrite2 {
    public static String dataDir = "static/data";
    static String writerUrl = "http://127.0.0.1:7201/api/v1/prom/remote/write";

    private static Label.Builder getLable(String name, String value) {
        Label.Builder lable = Label.newBuilder();
        lable.setName(name);
        lable.setValue(value);
        return lable;
    }

    private static Sample.Builder getSample(long dTime, double dValue) {
        Sample.Builder sample = Sample.newBuilder();
        sample.setTimestamp(dTime * 1000);
        sample.setValue(dValue);
        return sample;
    }

    private static void processHistoricalData(Client client, String filePath, String dataType) {
        JSONParser jsonParser = new JSONParser();

        try (FileReader reader = new FileReader(filePath)) {

            JSONArray allSeries = (JSONArray) jsonParser.parse(reader);
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
            
            for (int i = 0; i < allSeries.size(); i++) {
                JSONObject series = (JSONObject) allSeries.get(i);

                JSONObject tags = (JSONObject) series.get("tags");
                JSONObject dps = (JSONObject) series.get("dps");

                ArrayList<Label.Builder> labels = new ArrayList<>();
                labels.add(BulkWrite2.getLable("__name__", "bulkdata"));
                labels.add(BulkWrite2.getLable("type", dataType));

                for (Object key : tags.keySet()) {
                    String value = (String) tags.get(key);
                    String name = (String) key;
                    Label.Builder lable = BulkWrite2.getLable(name, value);
                    labels.add(lable);
                }

                for (Object key : dps.keySet()) {
                    long dTime = Long.valueOf((String) key);
                    double dValue = (double) dps.get(key);

                    Sample.Builder sample = BulkWrite2.getSample(dTime, dValue);

                    Writer2 task = new Writer2(client, labels, sample);
                    executor.execute(task);
                }

                System.out.println("Completed" + " " + (String) tags.get("host"));
            }

            while(!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                Thread.sleep(5000);
            }

            executor.shutdown();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return;
    }

    public static void main(String[] args) {
        Client client = new Client(writerUrl);

        File directoryPath = new File(BulkWrite2.dataDir);
        File filesList[] = directoryPath.listFiles();

        System.out.println("Total files:" + filesList.length);

        for (File file : filesList) {
            System.out.println("File path: " + file.getAbsolutePath());
            String fileName = file.getName();
            String dataType = fileName.toLowerCase().contains("load") ? "load" : "average";
            BulkWrite2.processHistoricalData(client, file.getAbsolutePath(), dataType);

            // file.delete();
        }

        // BulkWrite2.processHistoricalData("static/node.loadavgStat.fiveMinute-2022-06-26-00",
        // "load");
        // BulkWrite2.processHistoricalData("static/node.loadavgStat.fiveMinute-2022-06-26-01",
        // "load");
        // BulkWrite2.processHistoricalData("static/node.loadavgStat.fiveMinute-2022-06-26-02",
        // "load");
    }
}
