// Copyright (c) Keith D Gregory
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.kdgregory.aws.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import net.sf.kdgcommons.collections.CollectionUtil;
import net.sf.kdgcommons.lang.ThreadUtil;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.*;

import com.kdgregory.aws.utils.cloudwatch.CloudWatchLogsReader;
import com.kdgregory.aws.utils.cloudwatch.CloudWatchLogsUtil;
import com.kdgregory.aws.utils.cloudwatch.CloudWatchLogsWriter;
import com.kdgregory.aws.utils.cloudwatch.LogStreamIterable;


/**
 *  Combined test for all CloudWatch Logs functionality.
 */
public class CloudWatchLogsIntegrationTest
{
    // a single instance of the client is shared between all tests
    private static AWSLogs client;

    private Logger logger = LoggerFactory.getLogger(getClass());

//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    /**
     *  Asserts that all passed records are distinct. This is used for multi-thread
     *  and multi-stream tests.
     */
    private void assertDistinctMessages(List<OutputLogEvent> events)
    {
        Set<String> messages = new HashSet<String>();
        for (OutputLogEvent event : events)
        {
            String message = event.getMessage();
            if (messages.contains(message))
            {
                fail("duplicate message: " + message);
            }
            messages.add(message);
        }
    }

//----------------------------------------------------------------------------
//  Pre/post operations
//----------------------------------------------------------------------------

    @BeforeClass
    public static void beforeClass()
    {
        client = AWSLogsClientBuilder.defaultClient();
    }


    @AfterClass
    public static void afterClass()
    {
        client.shutdown();
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testCreationAndDeletion() throws Exception
    {
        MDC.put("testName", "testCreationAndDeletion");
        logger.info("starting");

        String logGroupName = "CloudWatchLogsIntegrationTest-testCreationAndDeletion-" + UUID.randomUUID();
        String logStreamName = "stream-1";

        // this will also exercise createLogGroup()
        assertNotNull("creation succeeded",                     CloudWatchLogsUtil.createLogStream(client, logGroupName, logStreamName, 30000));

        assertNotNull("post-create describeLogGroup()",         CloudWatchLogsUtil.describeLogGroup(client, logGroupName));
        assertNotNull("post-create describeLogStream()",        CloudWatchLogsUtil.describeLogStream(client, logGroupName, logStreamName));

        assertTrue("stream deletion succeeded",                 CloudWatchLogsUtil.deleteLogStream(client, logGroupName, logStreamName, 30000));

        assertNotNull("post-delete-stream describeLogGroup()",  CloudWatchLogsUtil.describeLogGroup(client, logGroupName));
        assertNull("post-delete-stream describeLogStream()",    CloudWatchLogsUtil.describeLogStream(client, logGroupName, logStreamName));

        assertTrue("group deletion succeeded",                  CloudWatchLogsUtil.deleteLogGroup(client, logGroupName, 30000));

        assertNull("post-delete-group describeLogGroup()",      CloudWatchLogsUtil.describeLogGroup(client, logGroupName));
        assertNull("post-delete-group describeLogStream()",     CloudWatchLogsUtil.describeLogStream(client, logGroupName, logStreamName));

        logger.info("finished");
    }


    @Test
    public void testLogStreamIterable() throws Exception
    {
        String logGroupName = "CloudWatchLogsIntegrationTest-testLogStreamIterable-" + UUID.randomUUID();
        String logStreamName =  UUID.randomUUID().toString();

        MDC.put("testName", "testLogStreamIterable");
        logger.info("starting, log group is {}", logGroupName);

        long firstTimestamp = System.currentTimeMillis() - 10000;
        CloudWatchLogsWriter writer = new CloudWatchLogsWriter(client, logGroupName, logStreamName);
        writer.add(firstTimestamp,        "one");
        writer.add(firstTimestamp + 1000, "two");
        writer.add(firstTimestamp + 2000, "three");
        writer.add(firstTimestamp + 3000, "four");
        writer.flush();

        waitForRecords(logGroupName, logStreamName);

        LogStreamIterable itx1 = new LogStreamIterable(client, logGroupName, logStreamName);
        List<String> msg1 = new ArrayList<>();
        for (OutputLogEvent event : itx1)
        {
            msg1.add(event.getMessage());
        }
        assertEquals("test 1: forward iteration from start of stream",
                     Arrays.asList("one", "two", "three", "four"),
                     msg1);

        LogStreamIterable itx2 = new LogStreamIterable(client, logGroupName, logStreamName, false);
        List<String> msg2 = new ArrayList<>();
        for (OutputLogEvent event : itx2)
        {
            msg2.add(event.getMessage());
        }
        assertEquals("test 2: reverse iteration from end of stream",
                     Arrays.asList("four", "three", "two", "one"),
                     msg2);

        LogStreamIterable itx3 = new LogStreamIterable(client, logGroupName, logStreamName, true, new Date(firstTimestamp + 2000));
        List<String> msg3 = new ArrayList<>();
        for (OutputLogEvent event : itx3)
        {
            msg3.add(event.getMessage());
        }
        assertEquals("test 3: forward iteration from timestamp",
                     Arrays.asList("three", "four"),
                     msg3);

        LogStreamIterable itx4 = new LogStreamIterable(client, logGroupName, logStreamName, false, new Date(firstTimestamp + 2000));
        List<String> msg4 = new ArrayList<>();
        for (OutputLogEvent event : itx4)
        {
            msg4.add(event.getMessage());
        }
        assertEquals("test 4: reverse iteration from timestamp",
                     Arrays.asList("two", "one"),
                     msg4);

        CloudWatchLogsUtil.deleteLogGroup(client, logGroupName, 10000);
    }


    @Test
    public void testWriterAndReader() throws Exception
    {
        String logGroupName = "CloudWatchLogsIntegrationTest-testWriterAndReader-" + UUID.randomUUID();
        String logStreamName = "stream-1";

        int numRecords = 20000;
        int lastIndex = numRecords - 1;

        MDC.put("testName", "testWriterAndReader");
        logger.info("starting, log group is {}", logGroupName);

        long now = System.currentTimeMillis();

        CloudWatchLogsWriter writer = new CloudWatchLogsWriter(client, logGroupName, logStreamName)
                                      .withBatchLogging(true);

        for (int ii = 0 ; ii < numRecords ; ii++)
        {
            writer.add(now - 100 * ii, String.format("message %06d", ii));
        }
        writer.flush();

        assertTrue("write processed in multiple batches", writer.getBatchCount() > 1);


        CloudWatchLogsReader reader = new CloudWatchLogsReader(client, logGroupName, logStreamName)
                                      .withRetrieveEntryLogging(true)
                                      .withRetrieveExitLogging(true);

        logger.info("retrieving events without time limit");
        List<OutputLogEvent> events1 = reader.retrieve(numRecords, 10000);

        assertEquals("retrieved all records",   numRecords,                     events1.size());

        assertEquals("first record timestamp",   now - 100 * lastIndex,         events1.get(0).getTimestamp().longValue());
        assertEquals("last record timestamp",    now,                           events1.get(lastIndex).getTimestamp().longValue());
        assertEquals("first record message",    "message 019999",               events1.get(0).getMessage());
        assertEquals("last record's message",   "message 000000",               events1.get(lastIndex).getMessage());

        // note: time range is inclusive start, exclusive end
        long timeStart = now - 100 * 50;
        long timeEnd   = now - 100 * 20;
        reader.withTimeRange(timeStart, timeEnd);

        logger.info("retrieving events with time limit ({} to {})", timeStart, timeEnd);
        List<OutputLogEvent> events2 = reader.retrieve();

        assertEquals("number of records in range",  30,                                         events2.size());
        assertEquals("first event in range",        events1.get(lastIndex - 50).getMessage(),   events2.get(0).getMessage());
        assertEquals("last event in rage",          events1.get(lastIndex - 21).getMessage(),   events2.get(29).getMessage());

        logger.info("finished");
    }


    @Test
    public void testWriterMultiThread() throws Exception
    {
        final String logGroupName = "CloudWatchLogsIntegrationTest-testWriterMultiThread" + UUID.randomUUID();
        final String logStreamName = "stream-1";

        final int numThreads = 10;
        final int recordsPerThread = 1000;
        final int flushInterval = 200;

        MDC.put("testName", "testWriterMultiThread");
        logger.info("starting, log group is {}", logGroupName);

        List<Thread> threads = new ArrayList<Thread>();
        for (int ii = 0 ; ii < numThreads ; ii++)
        {
            threads.add(new Thread(new Runnable()
            {
                @Override
                public void run()
                {

                    CloudWatchLogsWriter writer = new CloudWatchLogsWriter(client, logGroupName, logStreamName);
                    for (int jj = 0 ; jj < recordsPerThread ; jj++)
                    {
                        writer.add("message " + jj + " on thread " + Thread.currentThread().getId());
                        if ((jj % flushInterval) == 0)
                            writer.flush();
                    }
                    writer.flush();
                }
            }));
        }

        for (Thread thread : threads)
        {
            thread.start();
        }

        for (Thread thread : threads)
        {
            thread.join();
        }


        CloudWatchLogsReader reader = new CloudWatchLogsReader(client, logGroupName, logStreamName);
        List<OutputLogEvent> events = reader.retrieve(numThreads * recordsPerThread, 10000);

        assertEquals("retrieved all events", numThreads * recordsPerThread, events.size());

        assertDistinctMessages(events);

        logger.info("finished");
    }


    @Test
    public void testReaderMultiStream() throws Exception
    {
        String logGroupName = "CloudWatchLogsIntegrationTest-testReaderMultiStream-" + UUID.randomUUID();
        String logStreamName1 = "stream-1";
        String logStreamName2 = "stream-2";

        int numRecords = 1000;

        long now = System.currentTimeMillis();

        MDC.put("testName", "testReaderMultiStream");
        logger.info("starting, log group is {}", logGroupName);

        CloudWatchLogsWriter writer1 = new CloudWatchLogsWriter(client, logGroupName, logStreamName1);
        CloudWatchLogsWriter writer2 = new CloudWatchLogsWriter(client, logGroupName, logStreamName2);

        for (int ii = 0 ; ii < numRecords ; ii++)
        {
            writer1.add(now - 100 * ii, String.format(logGroupName + " / " + logStreamName1 + " message %04d", ii));
            writer2.add(now - 100 * ii, String.format(logGroupName + " / " + logStreamName2 + " message %04d", ii));
        }
        writer1.flush();
        writer2.flush();

        CloudWatchLogsReader reader = new CloudWatchLogsReader(client, logGroupName, logStreamName1, logStreamName2);
        List<OutputLogEvent> events = reader.retrieve(numRecords * 2, 10000);

        assertEquals("retrieved all records", numRecords * 2, events.size());

        assertEquals("first two records have same timestamp (sorted together even though originating separately)",
                     events.get(0).getTimestamp(),
                     events.get(1).getTimestamp());

        assertDistinctMessages(events);

        logger.info("finished");
    }


    @Test
    public void testReaderMultiGroup() throws Exception
    {

        String logGroupNameBase = "CloudWatchLogsIntegrationTest-testReaderMultiGroup-" + UUID.randomUUID();
        String logGroupName1 = logGroupNameBase + "-1";
        String logGroupName2 = logGroupNameBase + "-2";
        String logStreamName = "stream-1";

        int numRecords = 1000;

        long now = System.currentTimeMillis();

        MDC.put("testName", "testReaderMultiGroup");
        logger.info("starting, log group base is {}", logGroupNameBase);

        CloudWatchLogsWriter writer1 = new CloudWatchLogsWriter(client, logGroupName1, logStreamName);
        CloudWatchLogsWriter writer2 = new CloudWatchLogsWriter(client, logGroupName2, logStreamName);

        for (int ii = 0 ; ii < numRecords ; ii++)
        {
            writer1.add(now - 100 * ii, String.format(logGroupName1 + " / " + logStreamName + " message %04d", ii));
            writer2.add(now - 100 * ii, String.format(logGroupName2 + " / " + logStreamName + " message %04d", ii));
        }
        writer1.flush();
        writer2.flush();

        CloudWatchLogsReader reader = new CloudWatchLogsReader(
                                        client,
                                        new CloudWatchLogsReader.StreamIdentifier(logGroupName1, logStreamName),
                                        new CloudWatchLogsReader.StreamIdentifier(logGroupName2, logStreamName));
        List<OutputLogEvent> events = reader.retrieve(numRecords * 2, 10000);

        assertEquals("retrieved all records", numRecords * 2, events.size());

        assertEquals("first two records have same timestamp (sorted together even though originating separately)",
                     events.get(0).getTimestamp(),
                     events.get(1).getTimestamp());

        assertDistinctMessages(events);

        logger.info("finished");
    }

//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    /**
     *  A spin-loop that waits for records to become available in a stream,
     *  throwing if they're not there within 30 seconds.
     *
     *  Re-creates what the classes-under-test do, but as a separate implementation
     *  so that we know we're not just believing our own bullshit.
     */
    private void waitForRecords(String logGroupName, String logStreamName)
    {
        long timeout = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < timeout)
        {
            GetLogEventsRequest request = new GetLogEventsRequest(logGroupName, logStreamName);
            GetLogEventsResult result = client.getLogEvents(request);
            if (CollectionUtil.isNotEmpty(result.getEvents()))
                return;
            ThreadUtil.sleepQuietly(250);
        }
        throw new IllegalStateException("records did not appear in stream before timeout");
    }
}
