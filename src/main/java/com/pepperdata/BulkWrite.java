package com.pepperdata;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.json.simple.*;
import org.json.simple.parser.*;

import com.uber.m3.promremoteclient.Client;
import com.uber.m3.promremoteclient.Prometheus.TimeSeries;
import com.uber.m3.promremoteclient.Prometheus.WriteRequest;
import com.uber.m3.promremoteclient.Prometheus.Label;
import com.uber.m3.promremoteclient.Prometheus.Sample;

class Writer extends Thread {
    static String writerUrl = "http://localhost:7201/api/v1/prom/remote/write";
    public List<JSONObject> seriesList;
    public Client client;
    public String dataType;

    public Writer(List<JSONObject> seriesList, String dataType) {
        this.seriesList = seriesList;
        this.client = new Client(writerUrl);
        this.dataType = dataType;
    }

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

    public void writeToM3DB(ArrayList<Label.Builder> labels, Sample.Builder sample) {
        TimeSeries.Builder timeseries = TimeSeries.newBuilder();

        int ind = 0;
        while (labels.size() > ind) {
            timeseries.addLabels(labels.get(ind));
            ind++;
        }

        timeseries.addSamples(sample);

        WriteRequest.Builder writerRequest = WriteRequest.newBuilder();
        writerRequest.addTimeseries(timeseries);

        try {
            this.client.WriteProto(writerRequest.build());
        } catch (IOException e) {
            try {
                TimeUnit.SECONDS.sleep(5);
                System.out.println("Repeat" + " " + sample.getTimestamp());
                this.writeToM3DB(labels, sample);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    public void processSeries(JSONObject series) {
        JSONObject tags = (JSONObject) series.get("tags");
        JSONObject dps = (JSONObject) series.get("dps");

        ArrayList<Label.Builder> labels = new ArrayList<>();
        labels.add(Writer.getLable("__name__", "bulkdata"));
        labels.add(Writer.getLable("type", this.dataType));

        for (Object key : tags.keySet()) {
            String value = (String) tags.get(key);
            String name = (String) key;
            Label.Builder lable = Writer.getLable(name, value);
            labels.add(lable);
        }

        for (Object key : dps.keySet()) {
            long dTime = Long.valueOf((String) key);
            double dValue = (double) dps.get(key);

            Sample.Builder sample = Writer.getSample(dTime, dValue);

            this.writeToM3DB(labels, sample);
        }

        System.out.println("Completed" + " " + (String) tags.get("host"));
    }

    public void run() {
        for (JSONObject series : this.seriesList) {
            this.processSeries(series);
        }
    }
}

public class BulkWrite {
    public static String dataDir = "static/data";

    private static void processHistoricalData(String filePath, String dataType) {
        JSONParser jsonParser = new JSONParser();
        try (FileReader reader = new FileReader(filePath)) {
            JSONArray allSeries = (JSONArray) jsonParser.parse(reader);

            List<JSONObject> seriesList = new ArrayList<JSONObject>();

            for (int i = 0; i < allSeries.size(); i++) {
                JSONObject series = (JSONObject) allSeries.get(i);
                seriesList.add(series);
            }

            int cores = Runtime.getRuntime().availableProcessors();
            int chunkSize = seriesList.size() / cores;
            final AtomicInteger counter = new AtomicInteger();
            final Collection<List<JSONObject>> result = seriesList.stream()
                    .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize))
                    .values();

            Writer[] threads = new Writer[10];
            int i = 0;
            for (List<JSONObject> seriesL : result) {
                System.out.println(seriesL.size());
                threads[i] = new Writer(seriesL, dataType);
                threads[i].start();
                i++;
            }

            for (Writer thread : threads) {
                thread.join();
            }

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
        File directoryPath = new File(BulkWrite.dataDir);
        File filesList[] = directoryPath.listFiles();

        System.out.println("Total files:" + filesList.length);

        for (File file : filesList) {
            System.out.println("File path: " + file.getAbsolutePath());
            String fileName = file.getName();
            String dataType = fileName.toLowerCase().contains("load") ? "load" : "average";
            BulkWrite.processHistoricalData(file.getAbsolutePath(), dataType);
        }

        // BulkWrite.processHistoricalData("static/node.loadavgStat.fiveMinute-2022-06-26-00",
        // "load");
        // BulkWrite.processHistoricalData("static/node.loadavgStat.fiveMinute-2022-06-26-01",
        // "load");
        // BulkWrite.processHistoricalData("static/node.loadavgStat.fiveMinute-2022-06-26-02",
        // "load");
    }
}
