package io.opencensus.apm;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

import io.opencensus.apm.EventListener;

import java.io.BufferedReader;
import java.io.InputStreamReader;;
import java.io.IOException;;

import io.opencensus.exporter.stats.stackdriver.StackdriverStatsConfiguration;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.samplers.Samplers;
import io.opencensus.trace.Tracing;

public class Inspector {
    public static void main(String ...args) {
        try {
            enableOpenCensusExporting();
        } catch (Exception e) {
            System.err.println("Error while enabling OpenCensus and its exporters: " + e.toString());
            return;
        }

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

        MongoClientOptions opts = MongoClientOptions.builder()
                                    .addCommandListener(new EventListener())
                                    .build();

        MongoClient client = new MongoClient(new ServerAddress("localhost"), opts);

        DB db = client.getDB("media-searches");
        DBCollection dc = db.getCollection("youtube_searches");

        while (true) {
            try {
                System.out.print("> ");
                System.out.flush();
                String line = stdin.readLine();
                String processed = line.toUpperCase();

                DBCursor dcc = dc.find(BasicDBObjectBuilder.start("key", line).get());
                while (dcc.hasNext()) {
                    DBObject cur = dcc.next();
                    System.out.println("< " + cur);
                }

            } catch (IOException e) {
                System.err.println("Exception "+ e);
            }
        }
    }

    private static void enableOpenCensusExporting() throws IOException {
        TraceConfig traceConfig = Tracing.getTraceConfig();
        // For demo purposes, lets always sample.
        traceConfig.updateActiveTraceParams(
                traceConfig.getActiveTraceParams().toBuilder().setSampler(Samplers.alwaysSample()).build());

        String gcpProjectId = "census-demos";

        // The trace exporter
        StackdriverTraceExporter.createAndRegister(
                StackdriverTraceConfiguration.builder()
                .setProjectId(gcpProjectId)
                .build());

        // The stats exporter
        StackdriverStatsExporter.createAndRegister(
                StackdriverStatsConfiguration.builder()
                .setProjectId(gcpProjectId)
                .build());
    }
}
