package io.opencensus.apm;

import com.mongodb.connection.ConnectionDescription;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonDocument;

import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Aggregation.Distribution;
import io.opencensus.stats.BucketBoundaries;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.MeasureMap;
import io.opencensus.stats.Measure.MeasureDouble;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.stats.View;
import io.opencensus.stats.View.Name;
import io.opencensus.stats.ViewManager;
import io.opencensus.tags.TagContext;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tagger;
import io.opencensus.tags.Tags;

import java.util.concurrent.TimeUnit;

public class EventListener implements CommandListener {
    private static final Tagger TAGGER = Tags.getTagger();
    private static final StatsRecorder STATSRECORDER = Stats.getStatsRecorder();

    private static final MeasureDouble mRoundtripLatencyMs = MeasureDouble.create("mongo/roundtrip_latency", "The latency of a call from a client to a server", "ms");
    private static final MeasureLong mErrors = MeasureLong.create("mongo/errors", "The number of errors encountered by a call from a client", "1");
    private static final MeasureLong mBytesRead = MeasureLong.create("mongo/bytes_read", "The number of bytes written out to the server from a client", "By");
    private static final MeasureLong mBytesWritten = MeasureLong.create("mongo/bytes_written", "The number of bytes read from the server by a client", "By");

    private static final TagKey keyDriver = TagKey.create("driver");
    private static final TagKey keyServerVersion = TagKey.create("server_version");
    private static final TagKey keyServerType = TagKey.create("server_type");
    private static final TagKey keyCommandName = TagKey.create("command");

    public EventListener() {
        this.registerAllViews();
    }

    @Override
    public void commandStarted(CommandStartedEvent evt) {
        BsonDocument doc = evt.getCommand();

        // TODO: Count the number of call invocations tagged by commandName
        System.out.println(String.format("Started::\nCommand: %s\nDatabaseName: %s\nDoc size: %d\nReqID: %d\n\n",
                    evt.getCommandName(),
                    evt.getDatabaseName(),
                    doc.size(),
                    evt.getRequestId()));
    }

    private TagContext taggedContext(CommandEvent evt) {
        // Attributes to track in the span:
        // 1. "driver":             "java"
        // 2. "conn_id":            evt.getConnectionDescription().getConnectionId()
        // 3. "server_version":     evt.getConnectionDescription().getServerVersion().toString()
        // 4. "server_type":        evt.getConnectionDescription().getServerType().toString()
        ConnectionDescription cdesc = evt.getConnectionDescription();
        String serverType = cdesc.getServerType().toString();
        String commandName = evt.getCommandName();

        String serverVersion = ""; // Expecting the form say 3.4.2 to be created from the version list [3, 4, 2] etc.
        List<Integer> iServerVersionList = cdesc.getServerVersion().getVersionList();
        if (iServerVersionList.size() > 0) {
            List<String> sb = new ArrayList(iServerVersionList.size() * 2);
            for (Integer i : iServerVersionList) {
                sb.add(String.format("%d", i));
            }
            serverVersion = String.join(".", sb);
        }

        Map<String, AttributeValue> attributes = new HashMap<String, AttributeValue>();
        attributes.put("conn_id", AttributeValue.stringAttributeValue(cdesc.getConnectionId().toString()));
        attributes.put("driver", AttributeValue.stringAttributeValue("java")); 
        attributes.put("server_type", AttributeValue.stringAttributeValue(serverType));
        attributes.put("server_version", AttributeValue.stringAttributeValue(serverVersion));

        // Metrics to track
        // * Metrics to track
        //  1. latency in ms
        //  2. bytes read
        //
        // * Tags per metric
        //  1. driver
        //  2. server_version
        //  3. server_type
        //  4. command_name
        
        TagContext tctx = TAGGER.emptyBuilder()
                                    .put(keyDriver, TagValue.create("java"))
                                    .put(keyServerVersion, TagValue.create(serverVersion))
                                    .put(keyServerType, TagValue.create(serverType))
                                    .put(keyCommandName, TagValue.create(commandName))
                                    .build();

        return tctx;
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent evt) {
        BsonDocument doc = evt.getResponse();

        String spanName = "mongodb/" + evt.getCommandName();
        
        Double latencyMs = toMillis(evt.getElapsedTime(TimeUnit.NANOSECONDS));

        TagContext tctx = taggedContext(evt);
        Scope ss = TAGGER.withTagContext(tctx);
        try {
            MeasureMap mp = STATSRECORDER.newMeasureMap();
            mp.put(mRoundtripLatencyMs, latencyMs);
            mp.record();
        } finally {
            ss.close();
        }
                        
        // For tracing:
        // 1. Generate the traceID, spanID
        // We are using the simplest pseudo-random generator as we just want the simplest traceID.
        // TraceId traceId = TraceId.generateRandom(Math.random());
        // SpanId spanId = SpanId.generateRandom(Math.random());

        // 2. In the trace options insert the name as well as start and endtime, then export
        // TraceOptions topts = TraceOptionsBuilder.builder().setIsSampled(
        //         TRACER.getSampler()
        //             .shouldSample(null, null, traceId, spanId, name, null)
        //         );
        // SpanContext sc = SpanContext.create(traceId, spanId,
        
        System.out.println(String.format("Succeeded::\nTimespent(ms): %.3fms\nCommandName: %s\nResp size: %d\nReqID: %d\n\n",
                    latencyMs,
                    spanName,
                    doc.size(),
                    evt.getRequestId()));
    }

    @Override
    public void commandFailed(CommandFailedEvent evt) {
        Throwable reason = evt.getThrowable();
        Double latencyMs = toMillis(evt.getElapsedTime(TimeUnit.NANOSECONDS));

        String spanName = "mongodb/" + evt.getCommandName();
        
        TagContext tctx = taggedContext(evt);
        Scope ss = TAGGER.withTagContext(tctx);
        try {
            MeasureMap mp = STATSRECORDER.newMeasureMap();
            mp.put(mRoundtripLatencyMs, latencyMs);
            mp.put(mErrors, 1);
            mp.record();
        } finally {
            ss.close();
        }
         
        System.out.println(String.format("Failed::\nTimespent(ms): %.3fms\nCommandName: %s\nReason: %s\n",
                    latencyMs,
                    spanName,
                    evt.getConnectionDescription(),
                    reason.getMessage()));
    }

    private Double toMillis(long ns) {
        return new Double(ns)/1e6;
    }

    private void registerAllViews() {
       Aggregation defaultBytesDistribution = Distribution.create(BucketBoundaries.create(
                Arrays.asList(
                    // [0, 1KB, 2KB, 4KB, 16KB, 64KB, 256KB,   1MB,     4MB,     16MB,     64MB,     256MB,     1GB,        2GB]
                    0.0, 1024.0, 2048.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0, 16777216.0, 67108864.0, 268435456.0, 1073741824.0, 2147483648.0)
                    ));

        Aggregation defaultMillisecondsDistribution =  Distribution.create(BucketBoundaries.create(Arrays.asList(
                        // [0ms, 0.001ms, 0.005ms, 0.01ms, 0.05ms, 0.1ms, 0.5ms, 1ms, 1.5ms, 2ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 200ms, 400ms, 600ms, 800ms, 1s, 1.5s, 2.5s, 5s, 10s, 20s, 40s, 100s, 200s, 500s, 1000s]
                        0.0, 0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.0015, 0.002, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.5, 2.5, 5.0, 10.0, 20.0, 40.0, 100.0, 200.0, 500.0, 1000.0)));
                
        Aggregation countAggregation = Aggregation.Count.create();
        List<TagKey> noKeys = new ArrayList<TagKey>();

        View[] views = new View[]{
            View.create(
                    Name.create("mongo/client/bytes_read"),
                    "The number of bytes read back from the server",
                    mBytesRead,
                    defaultBytesDistribution,
                    Collections.singletonList(keyCommandName)),

            View.create(
                    Name.create("mongo/client/bytes_written"),
                    "The number of bytes written to the server",
                    mBytesWritten,
                    defaultBytesDistribution,
                    Collections.singletonList(keyCommandName)),

            View.create(
                    Name.create("mongo/client/roundtrip_latency"),
                    "The distribution of milliseconds",
                    mRoundtripLatencyMs,
                    defaultMillisecondsDistribution,
                    Collections.singletonList(keyCommandName)),

            View.create(
                    Name.create("mongo/client/errors"),
                    "The number of errors discerned by the various tags",
                    mErrors,
                    countAggregation,
                    Collections.unmodifiableList(Arrays.asList(keyDriver, keyServerVersion, keyServerType,keyCommandName)))};

        ViewManager vmgr = Stats.getViewManager();

        for (View view : views) {
            vmgr.registerView(view);
        }
    }
}
