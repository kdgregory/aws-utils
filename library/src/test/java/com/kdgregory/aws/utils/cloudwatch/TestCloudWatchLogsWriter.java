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

package com.kdgregory.aws.utils.cloudwatch;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.Level;

import net.sf.kdgcommons.lang.ClassUtil;
import net.sf.kdgcommons.lang.StringUtil;
import static net.sf.kdgcommons.test.NumericAsserts.*;
import static net.sf.kdgcommons.test.StringAsserts.*;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

import com.kdgregory.aws.utils.cloudwatch.CloudWatchLogsWriter;
import com.kdgregory.aws.utils.testhelpers.Log4JCapturingAppender;
import com.kdgregory.aws.utils.testhelpers.mocks.MockAWSLogs;


public class TestCloudWatchLogsWriter
{
    private Log4JCapturingAppender logCapture;

    // created per-test
    private CloudWatchLogsWriter writer;


    private void assertMessages(List<InputLogEvent> actual, String... expected)
    {
        assertEquals("number of messages", expected.length, actual.size());

        Iterator<InputLogEvent> eventItx = actual.iterator();
        for (int ii = 0 ; ii < expected.length ; ii++)
        {
            assertEquals("message " + ii, expected[ii], eventItx.next().getMessage());
        }
    }


    private void assertUnsentMessageQueueSize(int expectedSize)
    throws Exception
    {
        assertEquals("unsent messages", expectedSize, ClassUtil.getFieldValue(writer, "unsentMessages", Queue.class).size());
    }

//----------------------------------------------------------------------------
//  Per-test boilerplate
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        logCapture = Log4JCapturingAppender.getInstance();
        logCapture.reset();
    }

//----------------------------------------------------------------------------
//  Tests
//----------------------------------------------------------------------------

    @Test
    public void testBasicOperation() throws Exception
    {
        long now = System.currentTimeMillis();

        MockAWSLogs mock = new MockAWSLogs("foo", "bar");
        AWSLogs client = mock.getInstance();

        writer = new CloudWatchLogsWriter(client, "foo", "bar");
        writer.add("appended first");
        writer.add(now - 1000, "appended second");
        writer.flush();

        mock.assertInvocationCount("describeLogStreams",    1);
        mock.assertInvocationCount("putLogEvents",          1);
        assertEquals("request included group name",         "foo",      mock.getLastPutRequest().getLogGroupName());
        assertEquals("request included stream name",        "bar",      mock.getLastPutRequest().getLogStreamName());
        assertFalse("request included sequence token",                  StringUtil.isEmpty(mock.getLastPutRequest().getSequenceToken()));

        assertEquals("stats: flush count",                  1,          writer.getFlushCount());
        assertEquals("stats: batch count",                  1,          writer.getBatchCount());
        assertEquals("stats: invalid sequence count",       0,          writer.getInvalidSequenceCount());
        assertEquals("stats: total mesage count",           2,          writer.getTotalMessagesSent());

        assertMessages(mock.getLastPutEvents(), "appended second", "appended first");

        assertUnsentMessageQueueSize(0);
        logCapture.assertLogSize(0);
    }


    @Test
    public void testAutoCreateGroupAndStream() throws Exception
    {
        long now = System.currentTimeMillis();

        MockAWSLogs mock = new MockAWSLogs("foo", "bar");
        AWSLogs client = mock.getInstance();

        writer = new CloudWatchLogsWriter(client, "argle", "bargle");
        writer.add("appended first");
        writer.add(now - 1000, "appended second");
        writer.flush();

        mock.assertInvocationCount("describeLogStreams",    2);
        mock.assertInvocationCount("createLogGroup",        1);
        mock.assertInvocationCount("createLogStream",       1);
        mock.assertInvocationCount("putLogEvents",          1);
        assertFalse("request included sequence token",      StringUtil.isEmpty(mock.getLastPutRequest().getSequenceToken()));

        assertMessages(mock.getLastPutEvents(), "appended second", "appended first");

        assertUnsentMessageQueueSize(0);

        logCapture.assertLogEntry(0, Level.DEBUG, "stream.*argle.*bargle.*does not exist.*");
        logCapture.assertLogEntry(1, Level.DEBUG, "creating .* log stream.*bargle.*");
        logCapture.assertLogEntry(2, Level.DEBUG, "creating .* log group.*argle.*");
    }


    @Test
    public void testBogusMessages() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar");
        AWSLogs client = mock.getInstance();
        writer = new CloudWatchLogsWriter(client, "argle", "bargle");

        try
        {
            writer.add(System.currentTimeMillis() - (86400000 * 14 + 1000), "bogus");
            fail("accepted message > 2 weeks old");
        }
        catch (IllegalArgumentException ex)
        {
            assertRegex("too-old message exception (was: " + ex.getMessage() + ")",
                        "message timestamp too far in past.*argle.*bargle.*: [0-9]+; limit: [0-9]+",
                        ex.getMessage());
        }

        try
        {
            writer.add(System.currentTimeMillis() + (3600000 * 2 + 1000), "bogus");
            fail("accepted message > 2 hours in future");
        }
        catch (IllegalArgumentException ex)
        {
            assertRegex("future message exception (was: " + ex.getMessage() + ")",
                        "message timestamp too far in future.*argle.*bargle.*: [0-9]+; limit: [0-9]+",
                        ex.getMessage());
        }

        try
        {
            // we'll use a two-byte UTF-8 character to make things interesting
            String bogus = StringUtil.repeat('\u00C0', 512 * 1024 - 13) + "X";
            writer.add(bogus);
            fail("accepted too-large message");
        }
        catch (IllegalArgumentException ex)
        {
            assertRegex("too-large message exception (was: " + ex.getMessage() + ")",
                        "message is too large.*argle.*bargle.*: 1048551 bytes; limit: 1048550",
                        ex.getMessage());
        }
    }


    @Test
    public void testCountBasedBatching() throws Exception
    {
        long now = System.currentTimeMillis();

        MockAWSLogs mock = new MockAWSLogs("foo", "bar");
        AWSLogs client = mock.getInstance();
        writer = new CloudWatchLogsWriter(client, "foo", "bar");

        for (int ii = 0 ; ii < 18000 ; ii++)
        {
            // note that these will be written in reverse order
            writer.add(now - ii, String.valueOf(ii));
        }
        writer.flush();

        mock.assertInvocationCount("describeLogStreams",    1);
        mock.assertInvocationCount("putLogEvents",          2);
        assertEquals("all messages written",                18000,      mock.allMessages.size());
        assertEquals("first message written",               "17999",    mock.allMessages.get(0).getMessage());
        assertEquals("last message written",                "0",        mock.allMessages.get(17999).getMessage());
        assertEquals("last batch size",                     8000,       mock.getLastPutEvents().size());
        assertEquals("last batch first message",            "7999",     mock.getLastPutEvents().get(0).getMessage());
        assertEquals("last batch last message",             "0",        mock.getLastPutEvents().get(7999).getMessage());

        assertEquals("stats: flush count",                  1,          writer.getFlushCount());
        assertEquals("stats: batch count",                  2,          writer.getBatchCount());
        assertEquals("stats: invalid sequence count",       0,          writer.getInvalidSequenceCount());
        assertEquals("stats: total mesage count",           18000,      writer.getTotalMessagesSent());

        assertUnsentMessageQueueSize(0);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testSizeBasedBatching() throws Exception
    {
        long now = System.currentTimeMillis();

        MockAWSLogs mock = new MockAWSLogs("foo", "bar");
        AWSLogs client = mock.getInstance();
        writer = new CloudWatchLogsWriter(client, "foo", "bar");

        // this format means that each record (including overhead) will be 1024 bytes
        String messageFormat = StringUtil.repeat('X', 993) + " %04d";

        for (int ii = 0 ; ii < 2000 ; ii++)
        {
            // note that these will be written in reverse order
            writer.add(now - ii, String.format(messageFormat, ii));
        }
        writer.flush();

        mock.assertInvocationCount("describeLogStreams",    1);
        mock.assertInvocationCount("putLogEvents",          2);
        assertEquals("all messages written",                2000,       mock.allMessages.size());
        assertRegex("first message written",                ".* 1999",  mock.allMessages.get(0).getMessage());
        assertRegex("last message written",                 ".* 0000",  mock.allMessages.get(1999).getMessage());
        assertEquals("last batch size",                     976,        mock.getLastPutEvents().size());
        assertRegex("last batch first message",             ".* 0975",  mock.getLastPutEvents().get(0).getMessage());
        assertRegex("last batch last message",              ".* 0000",  mock.getLastPutEvents().get(975).getMessage());

        assertEquals("stats: flush count",                  1,          writer.getFlushCount());
        assertEquals("stats: batch count",                  2,          writer.getBatchCount());
        assertEquals("stats: invalid sequence count",       0,          writer.getInvalidSequenceCount());
        assertEquals("stats: total mesage count",           2000,       writer.getTotalMessagesSent());

        assertUnsentMessageQueueSize(0);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testTimestampBasedBatching() throws Exception
    {
        long now = System.currentTimeMillis();

        MockAWSLogs mock = new MockAWSLogs("foo", "bar");
        AWSLogs client = mock.getInstance();
        writer = new CloudWatchLogsWriter(client, "foo", "bar");

        for (int ii = 0 ; ii < 1500 ; ii++)
        {
            // note that these will be written in reverse order
            writer.add(now - ii * 60000, String.valueOf(ii));
        }
        writer.flush();

        mock.assertInvocationCount("describeLogStreams",    1);
        mock.assertInvocationCount("putLogEvents",          2);
        assertEquals("all messages written",                1500,       mock.allMessages.size());
        assertEquals("first message written",               "1499",     mock.allMessages.get(0).getMessage());
        assertEquals("last message written",                "0",        mock.allMessages.get(1499).getMessage());
        assertEquals("last batch size",                     60,         mock.getLastPutEvents().size());
        assertEquals("last batch first message",            "59",       mock.getLastPutEvents().get(0).getMessage());
        assertEquals("last batch last message",             "0",        mock.getLastPutEvents().get(59).getMessage());

        assertEquals("stats: flush count",                  1,          writer.getFlushCount());
        assertEquals("stats: batch count",                  2,          writer.getBatchCount());
        assertEquals("stats: invalid sequence count",       0,          writer.getInvalidSequenceCount());
        assertEquals("stats: total mesage count",           1500,       writer.getTotalMessagesSent());

        assertUnsentMessageQueueSize(0);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testInvalidSequenceToken() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
        {
            @Override
            public PutLogEventsResult putLogEvents(PutLogEventsRequest request)
            {
                PutLogEventsResult result = super.putLogEvents(request);
                if (getInvocationCount("putLogEvents") < 3)
                    throw new InvalidSequenceTokenException("");
                else
                    return result;
            }
        };
        AWSLogs client = mock.getInstance();
        writer = new CloudWatchLogsWriter(client, "foo", "bar");

        writer.add("test");
        writer.flush();

        mock.assertInvocationCount("describeLogStreams",    3);
        mock.assertInvocationCount("putLogEvents",          3);
        assertEquals("message written (count)",             1,          mock.getLastPutEvents().size());
        assertEquals("message written (content)",           "test",     mock.getLastPutEvents().get(0).getMessage());

        assertEquals("stats: flush count",                  1,          writer.getFlushCount());
        assertEquals("stats: batch count",                  1,          writer.getBatchCount());
        assertEquals("stats: invalid sequence count",       2,          writer.getInvalidSequenceCount());
        assertEquals("stats: total mesage count",           1,          writer.getTotalMessagesSent());

        assertUnsentMessageQueueSize(0);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testDataAlreadyAccepted() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
        {
            @Override
            public PutLogEventsResult putLogEvents(PutLogEventsRequest request)
            {
                throw new DataAlreadyAcceptedException("");
            }
        };
        AWSLogs client = mock.getInstance();
        writer = new CloudWatchLogsWriter(client, "foo", "bar");

        writer.add("test");
        writer.flush();

        mock.assertInvocationCount("describeLogStreams",    1);
        mock.assertInvocationCount("putLogEvents",          1);
        assertEquals("message written (count)",             0,          mock.allMessages.size());

        assertEquals("stats: flush count",                  1,          writer.getFlushCount());
        assertEquals("stats: batch count",                  0,          writer.getBatchCount());
        assertEquals("stats: total mesage count",           0,          writer.getTotalMessagesSent());

        assertUnsentMessageQueueSize(0);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.WARN, "DataAlreadyAcceptedException.* 1 .*foo.*bar");
    }


    @Test
    public void testUncaughtExceptionInDescribe() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
        {
            @Override
            public DescribeLogStreamsResult describeLogStreams(DescribeLogStreamsRequest request)
            {
                throw new ServiceUnavailableException("");
            }
        };
        AWSLogs client = mock.getInstance();
        writer = new CloudWatchLogsWriter(client, "foo", "bar");

        writer.add("test");
        writer.flush();

        mock.assertInvocationCount("describeLogStreams",    1);
        mock.assertInvocationCount("putLogEvents",          0);
        assertEquals("message written (count)",             0,          mock.allMessages.size());

        assertEquals("stats: flush count",                  1,          writer.getFlushCount());
        assertEquals("stats: batch count",                  0,          writer.getBatchCount());
        assertEquals("stats: total mesage count",           0,          writer.getTotalMessagesSent());

        assertUnsentMessageQueueSize(1);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.WARN, "ServiceUnavailableException writing to.*foo.*bar");
    }


    @Test
    public void testUncaughtExceptionInPut() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
        {
            @Override
            public PutLogEventsResult putLogEvents(PutLogEventsRequest request)
            {
                throw new ServiceUnavailableException("");
            }
        };
        AWSLogs client = mock.getInstance();
        writer = new CloudWatchLogsWriter(client, "foo", "bar");

        writer.add("test");
        writer.flush();

        mock.assertInvocationCount("describeLogStreams",    1);
        mock.assertInvocationCount("putLogEvents",          1);
        assertEquals("message written (count)",             0,          mock.allMessages.size());

        assertEquals("stats: flush count",                  1,          writer.getFlushCount());
        assertEquals("stats: batch count",                  0,          writer.getBatchCount());
        assertEquals("stats: total mesage count",           0,          writer.getTotalMessagesSent());

        assertUnsentMessageQueueSize(1);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.WARN, "ServiceUnavailableException writing to.*foo.*bar");
    }


    @Test
    public void testRejectedMessages() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
        {
            @Override
            public PutLogEventsResult putLogEvents(PutLogEventsRequest request)
            {
                PutLogEventsResult result = super.putLogEvents(request);
                result.setRejectedLogEventsInfo(new RejectedLogEventsInfo()
                                                .withTooOldLogEventEndIndex(Integer.valueOf(2))
                                                .withExpiredLogEventEndIndex(Integer.valueOf(4))
                                                .withTooNewLogEventStartIndex(Integer.valueOf(request.getLogEvents().size() - 3)));
                return result;
            }
        };
        AWSLogs client = mock.getInstance();
        writer = new CloudWatchLogsWriter(client, "foo", "bar");

        for (int ii = 0 ; ii < 10 ; ii++)
            writer.add("test " + ii);
        writer.flush();

        mock.assertInvocationCount("describeLogStreams",    1);
        mock.assertInvocationCount("putLogEvents",          1);
        assertEquals("all messages passed to putLogEvents", 10,         mock.allMessages.size());

        assertEquals("stats: flush count",                  1,          writer.getFlushCount());
        assertEquals("stats: batch count",                  1,          writer.getBatchCount());
        assertEquals("stats: messages sent",                10,         writer.getTotalMessagesSent());
        assertEquals("stats: messages rejected",            5 + 3,      writer.getTotalMessagesRejected());

        assertUnsentMessageQueueSize(0);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.WARN, "rejected 8 messages.*foo.*bar.*: 5 expired/too old, 3 too new");
    }


    @Test
    public void testShutdown() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar");
        AWSLogs client = mock.getInstance();

        writer = new CloudWatchLogsWriter(client, "foo", "bar");
        writer.add("test");
        writer.shutdown();

        mock.assertInvocationCount("describeLogStreams",    1);
        mock.assertInvocationCount("putLogEvents",          1);
        assertEquals("last message",                        "test",     mock.getLastPutEvents().get(0).getMessage());

        assertEquals("stats: flush count",                  1,          writer.getFlushCount());
        assertEquals("stats: batch count",                  1,          writer.getBatchCount());
        assertEquals("stats: total mesage count",           1,          writer.getTotalMessagesSent());

        assertUnsentMessageQueueSize(0);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, "shutdown called.*foo.*bar.*");

        try
        {
            writer.add("test 2");
            fail("was able to add message after shutdown");
        }
        catch (IllegalStateException ex)
        {
            assertRegex("post-shutdown exception message",
                        "writer has shut down.*foo.*bar.*",
                        ex.getMessage());
        }
    }


    @Test
    public void testBackgroundThread() throws Exception
    {
        ScheduledExecutorService threadpool = new ScheduledThreadPoolExecutor(1);
        final long interval = 150;

        long now = System.currentTimeMillis();

        MockAWSLogs mock = new MockAWSLogs("foo", "bar");
        AWSLogs client = mock.getInstance();
        writer = new CloudWatchLogsWriter(client, "foo", "bar", threadpool, interval);

        writer.add("test");

        Thread.sleep(200);

        long lastFlush =  writer.getLastFlushTime();

        assertInRange("flush was invoked after delay",  now + 100, now + 300,               lastFlush);
        mock.assertInvocationCount("putLogEvents",      1);
        assertEquals("last message",                    "test",                             mock.getLastPutEvents().get(0).getMessage());

        Thread.sleep(200);

        assertInRange("flush was invoked again",        lastFlush + 100, lastFlush + 300,   writer.getLastFlushTime());
        mock.assertInvocationCount("putLogEvents",      1);

        long shutdownAt = System.currentTimeMillis();
        writer.shutdown();

        Thread.sleep(300);  // two chances to invoke

        assertInRange("flush invoked immediately after shutdown", shutdownAt, shutdownAt + 100, writer.getLastFlushTime());
    }


    @Test
    public void testBatchLogging() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar");
        AWSLogs client = mock.getInstance();

        writer = new CloudWatchLogsWriter(client, "foo", "bar")
                 .withBatchLogging(true);

        writer.add("batch 1 message 1");
        writer.flush();

        writer.add("batch 2 message 1");
        writer.add("batch 2 message 2");
        writer.flush();

        mock.assertInvocationCount("putLogEvents",          2);

        logCapture.assertLogSize(2);
        logCapture.assertLogEntry(0, Level.DEBUG, "sending.* 1 .*events.*foo.*bar");
        logCapture.assertLogEntry(1, Level.DEBUG, "sending.* 2 .*events.*foo.*bar");
    }
}
