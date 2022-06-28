package com.pepperdata;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.json.simple.*;
import org.json.simple.parser.*;

import com.uber.m3.promremoteclient.Client;
import com.uber.m3.promremoteclient.Prometheus.TimeSeries;
import com.uber.m3.promremoteclient.Prometheus.WriteRequest;
import com.uber.m3.promremoteclient.Prometheus.Label;
import com.uber.m3.promremoteclient.Prometheus.Sample;

public class BulkWrite {
    static String write_url = "http://3.84.95.36:7201/api/v1/prom/remote/write";

    private static void processHistoricalData(Client client, String filename, String dataType) {
        JSONParser jsonParser = new JSONParser();
        try (FileReader reader = new FileReader(filename)) {
            JSONArray allSeries = (JSONArray) jsonParser.parse(reader);
            Iterator<JSONObject> iterator = allSeries.iterator();

            int ind = 0;
            while (iterator.hasNext()) {
                JSONObject series = iterator.next();
                String metric = (String) series.get("metric");
                JSONObject tags = (JSONObject) series.get("tags");
                JSONObject dps = (JSONObject) series.get("dps");

                ArrayList<Label.Builder> labels = new ArrayList<>();
                labels.add(BulkWrite.getLable("__name__", "bulkdata"));
                labels.add(BulkWrite.getLable("type", dataType));

                for (Object key : tags.keySet()) {
                    String value = (String) tags.get(key);
                    String name = (String) key;
                    Label.Builder lable = BulkWrite.getLable(name, value);
                    labels.add(lable);
                }

                for (Object key : dps.keySet()) {
                    long dTime = Long.valueOf((String) key);
                    double dValue = (double) dps.get(key);

                    Sample.Builder sample = BulkWrite.getSample(dTime, dValue);

                    System.out.println(dTime);
                    System.out.println(dValue);
                    BulkWrite.writeToM3DB(client, labels, sample);
                    return;
                }

                System.out.println(ind + " " + metric);
                ind++;
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
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

    static private void writeToM3DB(Client client, ArrayList<Label.Builder> labels, Sample.Builder sample)
            throws IOException {
        TimeSeries.Builder timeseries = TimeSeries.newBuilder();

        int ind = 0;
        while (labels.size() > ind) {
            timeseries.addLabels(labels.get(ind));
            ind++;
        }

        timeseries.addSamples(sample);

        WriteRequest.Builder writeRequest = WriteRequest.newBuilder();
        writeRequest.addTimeseries(timeseries);

        try {
            client.WriteProto(writeRequest.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Client client = new Client(write_url);

        // BulkWrite.processHistoricalData(client, "static/node_cpu.json", "cpu");
        // BulkWrite.processHistoricalData(client, "static/node_memory.json", "memory");
        BulkWrite.processHistoricalData(client, "static/node.loadavgStat.fiveMinute-2022-06-26-00", "load");
    }
}
