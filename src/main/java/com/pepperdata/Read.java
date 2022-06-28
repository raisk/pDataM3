package com.pepperdata;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.json.simple.*;
import org.json.simple.parser.*;
import org.xerial.snappy.Snappy;

import com.uber.m3.promremoteclient.Client;
import com.uber.m3.promremoteclient.Prometheus.TimeSeries;
import com.uber.m3.promremoteclient.Prometheus.Query;
import com.uber.m3.promremoteclient.Prometheus.LabelMatcher;
import com.uber.m3.promremoteclient.Prometheus.ReadHints;

import okhttp3.ResponseBody;

import com.uber.m3.promremoteclient.Prometheus.ReadRequest;
import com.uber.m3.promremoteclient.Prometheus.Label;
import com.uber.m3.promremoteclient.Prometheus.Sample;

public class Read {
    static long ML_SECOND = 60000;
    static String read_url = "http://3.84.95.36:7201/api/v1/prom/remote/read";

    public static void main(String[] args) {
        Client client = new Client(read_url);

        Query.Builder query = Query.newBuilder();
        query.setStartTimestampMs(1654663860*1000);
        query.setEndTimestampMs(1654750080*1000);


        LabelMatcher.Builder labelMatcher = LabelMatcher.newBuilder();
        labelMatcher.setType(LabelMatcher.Type.EQ);
        labelMatcher.setName("type");
        labelMatcher.setValue("cpu");

        query.addMatchers(labelMatcher);

        LabelMatcher.Builder labelMatcher1 = LabelMatcher.newBuilder();
        labelMatcher1.setType(LabelMatcher.Type.EQ);
        labelMatcher1.setName("__name__");
        labelMatcher1.setValue("cpu_agg_1h");

        query.addMatchers(labelMatcher1);

        ReadHints.Builder readHints = ReadHints.newBuilder();
        readHints.setStepMs(5*ML_SECOND);
        readHints.setFunc("SUM");
        query.setHints(readHints);

        ReadRequest.Builder readRequest = ReadRequest.newBuilder();
        readRequest.addQueries(query);

        try {
            ResponseBody response = client.ReadProto(readRequest.build());
            System.out.println(response.bytes());
            byte[] uncompressed = Snappy.uncompress(response.bytes());
            String result = new String(uncompressed, "UTF-8");
            System.out.println(result);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
