package com.pepperdata;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.json.simple.*;
import org.json.simple.parser.*;

import com.uber.m3.promremoteclient.Client;
import com.uber.m3.promremoteclient.Prometheus.TimeSeries;
import com.uber.m3.promremoteclient.Prometheus.WriteRequest;
import com.uber.m3.promremoteclient.Prometheus.Label;
import com.uber.m3.promremoteclient.Prometheus.Sample;

public class Write {
    static String write_url = "http://3.84.95.36:7201/api/v1/prom/remote/write";

    private static void processHistoricalData(Client client, String filename, String dataType) {
        JSONParser jsonParser = new JSONParser();
        try (FileReader reader = new FileReader(filename)) {
            JSONObject obj = (JSONObject) jsonParser.parse(reader);
            JSONObject data = (JSONObject) obj.get("data");
            JSONArray allSeries = (JSONArray) data.get("allSeries");

            Iterator<JSONObject> iterator = allSeries.iterator();
            int i = 0;
            while (iterator.hasNext()) {
                JSONObject series = iterator.next();
                String seriesId = (String) series.get("seriesId");
                seriesId = seriesId.replace("k8s_node=", "");
                // seriesId = "machine_" + i;
                JSONArray dataPoints = (JSONArray) series.get("dataPoints");

                Iterator<JSONArray> dataPointIterator = dataPoints.iterator();
                while (dataPointIterator.hasNext()) {
                    JSONArray dataPoint = dataPointIterator.next();
                    long dTime = (long) dataPoint.get(0);
                    double dValue = (double) dataPoint.get(1);

                    Write.writeToM3DB(client, seriesId, dataType, dTime, dValue);
                }
                i++;
            }
            // System.out.println(allSeries);
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

    static private void writeToM3DB(Client client, String series, String type, long dTime, double dValue)
            throws IOException {
        Sample.Builder sample = Sample.newBuilder();
        
        System.out.println(dTime);
        sample.setTimestamp(dTime * 1000);
        sample.setValue(dValue);

        TimeSeries.Builder timeseries = TimeSeries.newBuilder();
        timeseries.addLabels(Write.getLable("__name__", "pepperdata"));
        timeseries.addLabels(Write.getLable("node", series));
        timeseries.addLabels(Write.getLable("type", type));
        timeseries.addSamples(sample);

        WriteRequest.Builder writeRequest = WriteRequest.newBuilder();
        writeRequest.addTimeseries(timeseries);

        try {
            client.WriteProto(writeRequest.build());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Client client = new Client(write_url);

        // Write.processHistoricalData(client, "static/node_cpu.json", "cpu");
        Write.processHistoricalData(client, "static/node_memory.json", "memory");
    }
}
