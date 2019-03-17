// Copyright Keith D Gregory
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.kdgregory.aws.utils.kinesis;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.Level;

import static net.sf.kdgcommons.test.NumericAsserts.*;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;

import com.kdgregory.aws.utils.testhelpers.Log4JCapturingAppender;
import com.kdgregory.aws.utils.testhelpers.mocks.MockAmazonKinesis;


/**
 *  Mock-object tests of KinesisUtils.
 */
public class TestKinesisUtil
{
    private Log4JCapturingAppender logCapture;

//----------------------------------------------------------------------------
//  Sample data -- only populated with fields that we actually use
//----------------------------------------------------------------------------

    private static final Date NOW = new Date();

    public final static String STREAM_NAME = "TestKinesisUtils";

//----------------------------------------------------------------------------
//  Common setup
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        logCapture = Log4JCapturingAppender.getInstance();
        logCapture.reset();
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testDescribeShardsSingleRetrieve() throws Exception
    {
        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME);

        List<Shard> shards = KinesisUtil.describeShards(mock.getInstance(), STREAM_NAME, 1000);

        assertEquals("number of shards returned", 1, shards.size());

        mock.assertInvocationCount("describeStream", 1);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testDescribeShardsPaginatedRetrieve() throws Exception
    {
        MockAmazonKinesis mock = new MockAmazonKinesis()
                                 .withStream(STREAM_NAME, 2)
                                 .withDescribePageSize(1);

        List<Shard> shards = KinesisUtil.describeShards(mock.getInstance(), STREAM_NAME, 1000);

        assertEquals("number of shards returned",                   2, shards.size());
        assertTrue("shards returned in order (testing the mock)",   shards.get(0).getShardId().compareTo(shards.get(1).getShardId()) < 0);

        mock.assertInvocationCount("describeStream", 2);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testDescribeShardsStreamNotAvailable() throws Exception
    {
        MockAmazonKinesis mock = new MockAmazonKinesis();

        List<Shard> shards = KinesisUtil.describeShards(mock.getInstance(), STREAM_NAME, 1000);
        assertEquals("returned empty", null, shards);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testDescribeShardsThrottling() throws Exception
    {
        // expected sequence of calls
        //   returns data, no sleep
        //   throttled, sleep for 100 ms
        //   returns data, no sleep

        MockAmazonKinesis mock = new MockAmazonKinesis()
                                 .withStream(STREAM_NAME, 2)
                                 .withStatusChain(STREAM_NAME, StreamStatus.ACTIVE, LimitExceededException.class, StreamStatus.ACTIVE, IllegalStateException.class)
                                 .withDescribePageSize(1);

        long start = System.currentTimeMillis();
        List<Shard> shards = KinesisUtil.describeShards(mock.getInstance(), STREAM_NAME, 150);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("number of shards returned",       2, shards.size());
        mock.assertInvocationCount("describeStream",    3);
        assertInRange("execution time",                 50, 150, elapsed);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testDescribeShardsTimeout() throws Exception
    {
        // expected sequence of calls
        //   returns data, no sleep
        //   throttled, sleep for 100 ms
        //   throttled, sleep for 200 ms

        MockAmazonKinesis mock = new MockAmazonKinesis()
                                 .withStream(STREAM_NAME, 2)
                                 .withStatusChain(STREAM_NAME, StreamStatus.ACTIVE, LimitExceededException.class)
                                 .withDescribePageSize(1);

        long start = System.currentTimeMillis();
        List<Shard> shards = KinesisUtil.describeShards(mock.getInstance(), STREAM_NAME, 150);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("did not return anything",         null, shards);
        mock.assertInvocationCount("describeStream",    3);
        assertInRange("execution time",                 250, 350, elapsed);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.WARN, "describeStream timeout: " + STREAM_NAME);
    }


    @Test
    public void testWaitForStatusNormalOperation()
    {
        // expected sequence of calls
        //   CREATING, sleep for 100 ms
        //   CREATING, sleep for 100 ms
        //   ACTIVE, no sleep

        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
                                 .withStatusChain(STREAM_NAME, StreamStatus.CREATING, StreamStatus.CREATING, StreamStatus.ACTIVE);

        long start = System.currentTimeMillis();
        StreamStatus lastStatus = KinesisUtil.waitForStatus(mock.getInstance(), STREAM_NAME, StreamStatus.ACTIVE, 500);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("returned status",                 StreamStatus.ACTIVE, lastStatus);
        mock.assertInvocationCount("describeStream",    3);
        assertInRange("elapsed time",                   150, 250, elapsed);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testWaitForStatusTimeout()
    {
        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
                                 .withStatusChain(STREAM_NAME, StreamStatus.CREATING);

        long start = System.currentTimeMillis();
        StreamStatus lastStatus = KinesisUtil.waitForStatus(mock.getInstance(), STREAM_NAME, StreamStatus.ACTIVE, 250);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("returned status",                 StreamStatus.CREATING, lastStatus);
        mock.assertInvocationCount("describeStream",    3);
        assertApproximate("elapsed time",               300, elapsed, 25);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.WARN, "waitForStatus timeout: " + STREAM_NAME);
    }


    @Test
    public void testWaitForStatusWithThrottling()
    {
        // the expected sequence of calls:
        //    throttled, sleep for 100 ms
        //    throttled, sleep for 200 ms
        //    CREATING, sleep for 100 ms
        //    ACTIVE, no sleep

        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
                                 .withStatusChain(STREAM_NAME, LimitExceededException.class, LimitExceededException.class, StreamStatus.CREATING, StreamStatus.ACTIVE);

        long start = System.currentTimeMillis();
        StreamStatus lastStatus = KinesisUtil.waitForStatus(mock.getInstance(), STREAM_NAME, StreamStatus.ACTIVE, 500);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("returned status",                 StreamStatus.ACTIVE, lastStatus);
        mock.assertInvocationCount("describeStream",    4);
        assertApproximate("elapsed time",               400, elapsed, 25);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testCreateStreamHappyPath() throws Exception
    {
        // the expected sequence of calls:
        //   create stream
        //   describe, resource not found, sleep for 100 ms
        //   describe, creating, sleep for 100 ms
        //   describe, active, no sleep

        MockAmazonKinesis mock = new MockAmazonKinesis()
        {
            @Override
            public CreateStreamResult createStream(CreateStreamRequest request)
            {
                assertEquals("stream name passed to createStream", STREAM_NAME, request.getStreamName());
                assertEquals("shard count passed to createStream", 3,           request.getShardCount().intValue());
                return super.createStream(request);
            }
        }
        .withStatusChain(STREAM_NAME, ResourceNotFoundException.class, StreamStatus.CREATING, StreamStatus.ACTIVE);

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtil.createStream(mock.getInstance(), STREAM_NAME, 3, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("returned status",                 StreamStatus.ACTIVE, status);
        mock.assertInvocationCount("createStream",      1);
        mock.assertInvocationCount("describeStream",    3);
        assertApproximate("elapsed time",               200, elapsed, 25);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, "createStream: " + STREAM_NAME + ".* 3 shards");
    }


    @Test
    public void testCreateStreamThrottling() throws Exception
    {
        // expected sequence of calls
        //  - create, throttled, sleep 100 ms
        //  - create, throttled, sleep 200 ms
        //  - create, success, no sleep
        //  - describe, success, no sleep (not realistic, but not what we're testing)

        MockAmazonKinesis mock = new MockAmazonKinesis()
        {
            @Override
            public CreateStreamResult createStream(CreateStreamRequest request)
            {
                if (getInvocationCount("createStream") <= 2)
                    throw new LimitExceededException("");

                return super.createStream(request);
            }
        }
        .withStatusChain(STREAM_NAME, StreamStatus.ACTIVE);

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtil.createStream(mock.getInstance(), STREAM_NAME, 3, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("returned status",                 StreamStatus.ACTIVE, status);
        mock.assertInvocationCount("createStream",      3);
        mock.assertInvocationCount("describeStream",    1);
        assertApproximate("elapsed time",               300, elapsed, 25);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, "createStream: " + STREAM_NAME + ".* 3 shards");
    }


    @Test
    public void testCreateStreamAlreadyExists() throws Exception
    {
        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME);

        StreamStatus status = KinesisUtil.createStream( mock.getInstance(), STREAM_NAME, 3, 500L);

        assertEquals("returned status",                 StreamStatus.ACTIVE, status);
        mock.assertInvocationCount("createStream",      1);
        mock.assertInvocationCount("describeStream",    1);

        logCapture.assertLogSize(2);
        logCapture.assertLogEntry(0, Level.DEBUG, "createStream: " + STREAM_NAME + ".* 3 shards");
        logCapture.assertLogEntry(1, Level.WARN, "createStream .*existing stream: " + STREAM_NAME);
    }


    @Test
    public void testDeleteStreamHappyPath() throws Exception
    {
        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @Override
            public DeleteStreamResult deleteStream(DeleteStreamRequest request)
            {
                assertEquals("stream name passed in request", STREAM_NAME, request.getStreamName());
                return super.deleteStream(request);
            }
        };

        boolean status = KinesisUtil.deleteStream(mock.getInstance(), STREAM_NAME, 500L);

        assertTrue("returned status indicates success",     status);
        mock.assertInvocationCount("deleteStream",          1);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, "deleteStream: " + STREAM_NAME);
    }


    @Test
    public void testDeleteStreamTimeout() throws Exception
    {
        // expected calls:
        //  - throttled, sleep for 100 ms
        //  - throttled, sleep for 200 ms

        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @Override
            public DeleteStreamResult deleteStream(DeleteStreamRequest request)
            {
                throw new LimitExceededException("message is irrelevant");
            }
        };

        long start = System.currentTimeMillis();
        boolean status = KinesisUtil.deleteStream(mock.getInstance(), STREAM_NAME, 250L);
        long elapsed = System.currentTimeMillis() - start;

        assertFalse("returned status indicates failure",    status);
        mock.assertInvocationCount("deleteStream",          2);
        assertApproximate("elapsed time",                   300, elapsed, 25);

        logCapture.assertLogSize(2);
        logCapture.assertLogEntry(0, Level.DEBUG, "deleteStream: " + STREAM_NAME);
        logCapture.assertLogEntry(1, Level.WARN, "deleteStream timeout.*" + STREAM_NAME);
    }


    @Test
    public void testDeleteStreamDoesntExist() throws Exception
    {
        MockAmazonKinesis mock = new MockAmazonKinesis();

        boolean status = KinesisUtil.deleteStream(mock.getInstance(), STREAM_NAME, 250L);

        assertFalse("returned status indicates failure",    status);
        mock.assertInvocationCount("deleteStream",          1);

        logCapture.assertLogSize(2);
        logCapture.assertLogEntry(0, Level.DEBUG, "deleteStream: " + STREAM_NAME);
        logCapture.assertLogEntry(1, Level.WARN,  "deleteStream.*nonexistent.*: " + STREAM_NAME);
    }


    @Test
    public void testIncreaseRetentionPeriodHappyPath() throws Exception
    {
        // the expected sequence of calls:
        //  - wait for status (1 describe)
        //  - pre-change describe (determines whether period is increased or descreased)
        //  - increase retention
        //  - get status, updating, sleep for 100ms
        //  - get status, active

        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            // for this test only, we'll verify that we don't try to decrease
            @Override
            public DecreaseStreamRetentionPeriodResult decreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest request)
            {
                throw new UnsupportedOperationException("decreaseStreamRetentionPeriod should not be called by this test");
            }
        }
        .withStatusChain(STREAM_NAME, StreamStatus.ACTIVE, StreamStatus.ACTIVE, StreamStatus.UPDATING, StreamStatus.ACTIVE);

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtil.updateRetentionPeriod(mock.getInstance(), STREAM_NAME, 36, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("final stream status",                             StreamStatus.ACTIVE, status);
        assertEquals("current retention period",                        36, mock.getMockStream(STREAM_NAME).retentionPeriod);
        mock.assertInvocationCount("describeStream",                    4);
        mock.assertInvocationCount("increaseStreamRetentionPeriod",     1);
        mock.assertInvocationCount("decreaseStreamRetentionPeriod",     0);
        assertInRange("elapsed time",                                   90, 150, elapsed);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, "updateRetentionPeriod: " + STREAM_NAME + " to 36 hours");
    }


    @Test
    public void testIncreaseRetentionPeriodThrottling() throws Exception
    {
        // the expected sequence of calls:
        //  - wait for status
        //  - pre-change describe
        //  - try to increase retention, throttled, sleep 100ms
        //  - wait for status
        //  - pre-change describe
        //  - try to increase retention, throttled, sleep 200ms
        //  - wait for status
        //  - pre-change describe
        //  - try to increase retention, success
        //  - wait for status

        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @Override
            public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest request)
            {
                if (getInvocationCount("increaseStreamRetentionPeriod") < 3)
                    throw new LimitExceededException("message irrelevant");
                else
                    return super.increaseStreamRetentionPeriod(request);
            }
        };

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtil.updateRetentionPeriod( mock.getInstance(), STREAM_NAME, 36, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("final stream status",                             StreamStatus.ACTIVE, status);
        assertEquals("current retention period",                        36, mock.getMockStream(STREAM_NAME).retentionPeriod);
        mock.assertInvocationCount("describeStream",                    7);
        mock.assertInvocationCount("increaseStreamRetentionPeriod",     3);
        assertInRange("elapsed time",                                   290, 350, elapsed);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, "updateRetentionPeriod: " + STREAM_NAME + " to 36 hours");
    }


    @Test
    public void testIncreaseRetentionPeriodConcurrentUpdate() throws Exception
    {
        // we're going to try to increase the retention period but get blocked by a concurrent
        // call that increases it more than we planned, so we'll end up decreasing .. this is
        // an extremely convoluted example that's unlikely in real life, but verifies that the
        // logic is sound

        // the expected sequence of calls:
        //  - wait for status
        //  - pre-change describe
        //  - try to increase retention, throws ResourceInUseException, sleep for 100 ms
        //  - wait for status
        //  - pre-change describe
        //  - try to decrease retention, success
        //  - wait for status

        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @Override
            public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest request)
            {
                if (getInvocationCount("increaseStreamRetentionPeriod") == 1)
                {
                    knownStreams.get(request.getStreamName()).retentionPeriod = 48;
                    throw new ResourceInUseException("message irrelevant");
                }
                else
                {
                    // once we've seen ResourceInUse, we shouldn't try to increase again
                    throw new IllegalStateException("shouldn't call increase twice");
                }
            }
        };

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtil.updateRetentionPeriod(mock.getInstance(), STREAM_NAME, 36, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("final stream status",                             StreamStatus.ACTIVE, status);
        assertEquals("current retention period",                        36, mock.getMockStream(STREAM_NAME).retentionPeriod);
        mock.assertInvocationCount("describeStream",                    5);
        mock.assertInvocationCount("increaseStreamRetentionPeriod",     1);
        mock.assertInvocationCount("decreaseStreamRetentionPeriod",     1);
        assertInRange("elapsed time",                                   90, 150, elapsed);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, "updateRetentionPeriod: " + STREAM_NAME + " to 36 hours");
    }


    @Test
    public void testIncreaseRetentionPeriodStreamDisappears() throws Exception
    {
        // this is even more unlikely than the previous test: the stream disappears
        // between an "ACTIVE" status and the increase/decrease call ... in fact, I
        // don't think it would ever happen, but as it's a documented exception I
        // need to support it

        // the expected sequence of calls:
        //  - wait for status
        //  - pre-change describe
        //  - try to increase retention, throws ResourceNotFoundException, stop trying


        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @Override
            public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest request)
            {
                throw new ResourceNotFoundException("message irrelevant");
            }
        };

        AmazonKinesis client = mock.getInstance();

        StreamStatus status = KinesisUtil.updateRetentionPeriod(client, STREAM_NAME, 36, 1000L);

        assertEquals("final stream status",                             null, status);
        mock.assertInvocationCount("describeStream",                    2);
        mock.assertInvocationCount("increaseStreamRetentionPeriod",     1);
        mock.assertInvocationCount("decreaseStreamRetentionPeriod",     0);

        logCapture.assertLogSize(2);
        logCapture.assertLogEntry(0, Level.DEBUG, "updateRetentionPeriod: " + STREAM_NAME + " to 36 hours");
        logCapture.assertLogEntry(1, Level.WARN,  "updateRetentionPeriod.*nonexistent.*: " + STREAM_NAME );
    }


    @Test
    public void testDecreaseRetentionPeriodHappyPath() throws Exception
    {
        // sad path testing is handled by the various Increase tests; this just verifies
        // that we pick the correct call based on comparison of current and new period

        // the expected sequence of calls:
        //  - wait for status
        //  - pre-change describe (determines whether period is increased or descreased)
        //  - decrease retention
        //  - get status, updating, sleep for 100ms
        //  - get status, active

        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            // for this test only, we'll verify that we don't try to increase
            @Override
            public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest request)
            {
                throw new UnsupportedOperationException("increaseStreamRetentionPeriod should not be called by this test");
            }
        }
        .withStatusChain(STREAM_NAME, StreamStatus.ACTIVE, StreamStatus.ACTIVE, StreamStatus.UPDATING, StreamStatus.ACTIVE);

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtil.updateRetentionPeriod(mock.getInstance(), STREAM_NAME, 12, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("final stream status",                             StreamStatus.ACTIVE, status);
        assertEquals("current retention period",                        12, mock.getMockStream(STREAM_NAME).retentionPeriod);
        mock.assertInvocationCount("describeStream",                    4);
        mock.assertInvocationCount("increaseStreamRetentionPeriod",     0);
        mock.assertInvocationCount("decreaseStreamRetentionPeriod",     1);
        assertInRange("elapsed time",                                   90, 150, elapsed);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, "updateRetentionPeriod: " + STREAM_NAME + " to 12 hours");
    }


    @Test
    public void testReshardHappyPath() throws Exception
    {
        // expected sequence of calls
        // - update
        // - check status (updating), wait 100 ms
        // - check status (active)

        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @Override
            public UpdateShardCountResult updateShardCount(UpdateShardCountRequest request)
            {
                assertEquals("expected stream name", STREAM_NAME,                               request.getStreamName());
                assertEquals("expected shard count", 3,                                         request.getTargetShardCount().intValue());
                assertEquals("scaling type",         ScalingType.UNIFORM_SCALING.toString(),    request.getScalingType());

                return super.updateShardCount(request);
            }
        }
        .withStatusChain(STREAM_NAME, StreamStatus.UPDATING, StreamStatus.ACTIVE);

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtil.reshard(mock.getInstance(), STREAM_NAME, 3, 1000);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("returned status",                     StreamStatus.ACTIVE, status);
        mock.assertInvocationCount("updateShardCount",      1);
        mock.assertInvocationCount("describeStream",        3);
        assertInRange("elapsed time",                       90, 150, elapsed);

        // we don't look at the shard count from a describe, because that would just be testing the mock

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, "reshard: " + STREAM_NAME + ".* 3 shards");
    }


    @Test
    public void testReshardStatusTimout() throws Exception
    {
        // expected sequence of calls
        // - update
        // - check status (updating), wait 100 ms
        // - check status (updating), wait 100 ms
        // - check status (updating), wait 100 ms

        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
                                 .withStatusChain(STREAM_NAME, StreamStatus.UPDATING);

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtil.reshard(mock.getInstance(), STREAM_NAME, 2, 250);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("returned status",                     null, status);
        mock.assertInvocationCount("updateShardCount",      1);
        mock.assertInvocationCount("describeStream",        3);
        assertInRange("elapsed time",                       290, 350, elapsed);

        logCapture.assertLogSize(3);
        logCapture.assertLogEntry(0, Level.DEBUG, "reshard: " + STREAM_NAME + ".* 2 shards");
        logCapture.assertLogEntry(1, Level.WARN,  "waitForStatus timeout: " + STREAM_NAME);
        logCapture.assertLogEntry(2, Level.WARN,  "reshard timeout.* " + STREAM_NAME);
    }


    @Test(expected=LimitExceededException.class)
    public void testReshardUnrecoverableException() throws Exception
    {
        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @Override
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                fail("unexpected call to describeStream(): should have failed before now");
                return null;
            }

            @Override
            public UpdateShardCountResult updateShardCount(UpdateShardCountRequest request)
            {
                throw new LimitExceededException("message is irrelevant");
            }
        };

        KinesisUtil.reshard(mock.getInstance(), STREAM_NAME, 2, 1000);

        // we get the exception, so there's no logging of the error
        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, "reshard: " + STREAM_NAME + ".* 2 shards");
    }


    @Test
    public void testRetrieveShardIteratorLatest() throws Exception
    {
        // we assert the call because anything else would just be testing the mock
        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @SuppressWarnings("unused")
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("stream name",         STREAM_NAME,                                request.getStreamName());
                assertEquals("shard ID",            "shard-0001",                               request.getShardId());
                assertEquals("iterator type",       ShardIteratorType.LATEST.toString(),        request.getShardIteratorType());
                assertEquals("sequence number",     null,                                       request.getStartingSequenceNumber());
                assertEquals("starting timestamp",  null,                                       request.getTimestamp());
                return new GetShardIteratorResult().withShardIterator("blah blah blah");
            }
        };

        String result = KinesisUtil.retrieveShardIterator(mock.getInstance(), STREAM_NAME, "shard-0001", ShardIteratorType.LATEST, "123", NOW, 1000L);

        assertNotNull("call returned something", result);
        mock.assertInvocationCount("getShardIterator", 1);

        // this is called frequently enough that only trace-level logging would be appropriate
        logCapture.assertLogSize(0);
    }


    @Test
    public void testRetrieveShardIteratorTrimHorizon() throws Exception
    {
        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @SuppressWarnings("unused")
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("stream name",         STREAM_NAME,                                request.getStreamName());
                assertEquals("shard ID",            "shard-0001",                               request.getShardId());
                assertEquals("iterator type",       ShardIteratorType.TRIM_HORIZON.toString(),  request.getShardIteratorType());
                assertEquals("sequence number",     null,                                       request.getStartingSequenceNumber());
                assertEquals("starting timestamp",  null,                                       request.getTimestamp());
                return new GetShardIteratorResult().withShardIterator("blah blah blah");
            }
        };

        String result = KinesisUtil.retrieveShardIterator(mock.getInstance(), STREAM_NAME, "shard-0001", ShardIteratorType.TRIM_HORIZON, "123", NOW, 1000L);

        assertNotNull("call returned something", result);
        mock.assertInvocationCount("getShardIterator", 1);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testRetrieveShardIteratorAtSequenceNumber() throws Exception
    {
        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @SuppressWarnings("unused")
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("stream name",         STREAM_NAME,                                        request.getStreamName());
                assertEquals("shard ID",            "shard-0001",                                       request.getShardId());
                assertEquals("iterator type",       ShardIteratorType.AT_SEQUENCE_NUMBER.toString(),    request.getShardIteratorType());
                assertEquals("sequence number",     "123",                                              request.getStartingSequenceNumber());
                assertEquals("starting timestamp",  null,                                               request.getTimestamp());
                return new GetShardIteratorResult().withShardIterator("blah blah blah");
            }
        };

        String result = KinesisUtil.retrieveShardIterator(mock.getInstance(), STREAM_NAME, "shard-0001", ShardIteratorType.AT_SEQUENCE_NUMBER, "123", NOW, 1000L);

        assertNotNull("call returned something", result);
        mock.assertInvocationCount("getShardIterator", 1);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testRetrieveShardIteratorAfterSequenceNumber() throws Exception
    {
        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @SuppressWarnings("unused")
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("stream name",         STREAM_NAME,                                        request.getStreamName());
                assertEquals("shard ID",            "shard-0001",                                       request.getShardId());
                assertEquals("iterator type",       ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), request.getShardIteratorType());
                assertEquals("sequence number",     "123",                                              request.getStartingSequenceNumber());
                assertEquals("starting timestamp",  null,                                               request.getTimestamp());
                return new GetShardIteratorResult().withShardIterator("blah blah blah");
            }
        };

        String result = KinesisUtil.retrieveShardIterator(mock.getInstance(), STREAM_NAME, "shard-0001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "123", NOW, 1000L);

        assertNotNull("call returned something", result);
        mock.assertInvocationCount("getShardIterator", 1);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testRetrieveShardIteratorAtTimestamp() throws Exception
    {
        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @SuppressWarnings("unused")
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("stream name",         STREAM_NAME,                                request.getStreamName());
                assertEquals("shard ID",            "shard-0001",                               request.getShardId());
                assertEquals("iterator type",       ShardIteratorType.AT_TIMESTAMP.toString(),  request.getShardIteratorType());
                assertEquals("sequence number",     null,                                       request.getStartingSequenceNumber());
                assertEquals("starting timestamp",  NOW,                                        request.getTimestamp());
                return new GetShardIteratorResult().withShardIterator("blah blah blah");
            }
        };

        String result = KinesisUtil.retrieveShardIterator(mock.getInstance(), STREAM_NAME, "shard-0001", ShardIteratorType.AT_TIMESTAMP, "123", NOW, 1000L);

        assertNotNull("call returned something", result);
        mock.assertInvocationCount("getShardIterator", 1);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testRetrieveShardIteratorTimeout() throws Exception
    {
        // expected calls:
        // - attempt 1, sleep 100ms
        // - attempt 2, sleep 200ms

        MockAmazonKinesis mock = new MockAmazonKinesis(STREAM_NAME)
        {
            @SuppressWarnings("unused")
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                throw new ProvisionedThroughputExceededException("message irrelevant");
            }
        };

        long start = System.currentTimeMillis();
        String result = KinesisUtil.retrieveShardIterator(mock.getInstance(), STREAM_NAME, "shard-0001", ShardIteratorType.LATEST, "123", NOW, 250L);
        long elapsed = System.currentTimeMillis() - start;

        assertNull("call did not return anything",          result);
        mock.assertInvocationCount("getShardIterator",      2);
        assertInRange("elapsed time",                       290, 350, elapsed);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.WARN, "retrieveShardIterator timeout.* " + STREAM_NAME + " .* shard-0001");
    }


    @Test
    public void testShardListToMapConversion() throws Exception
    {
        Shard shard1 = new Shard().withShardId("shard-0001");
        Shard shard2 = new Shard().withShardId("shard-0002");
        Shard shard3 = new Shard().withShardId("shard-0003").withParentShardId("shard-0001");
        Shard shard4 = new Shard().withShardId("shard-0004").withParentShardId("shard-0001");
        List<Shard> allShards = Arrays.asList(shard1, shard2, shard3, shard4);

        Map<String,Shard> shardsById = KinesisUtil.toMapById(allShards);
        assertEquals("shard map size", 4, shardsById.size());
        for (Shard shard : allShards)
        {
            assertSame("shard map contains " + shard.getShardId(), shard, shardsById.get(shard.getShardId()));
        }

        Map<String,List<Shard>> shardsByParent = KinesisUtil.toMapByParentId(allShards);
        assertEquals("parent map size", 2, shardsByParent.size());
        assertEquals("ultimate parents",  Arrays.asList(shard1, shard2), shardsByParent.get(null));
        assertEquals("expected children", Arrays.asList(shard3, shard4), shardsByParent.get(shard1.getShardId()));

        logCapture.assertLogSize(0);
    }
}
