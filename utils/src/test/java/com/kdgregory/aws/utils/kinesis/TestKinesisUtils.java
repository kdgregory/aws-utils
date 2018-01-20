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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import static org.junit.Assert.*;

import net.sf.kdgcommons.collections.CollectionUtil;
import net.sf.kdgcommons.test.SelfMock;
import static net.sf.kdgcommons.test.NumericAsserts.*;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;


/**
 *  Mock-object tests of KinesisUtils.
 */
public class TestKinesisUtils
{
//----------------------------------------------------------------------------
//  Sample data -- only populated with fields that we actually use
//----------------------------------------------------------------------------

    private static final List<Shard> SHARDS_1 = Arrays.asList(
                                        new Shard().withShardId("0001"),
                                        new Shard().withShardId("0002"));
    private static final List<Shard> SHARDS_2 = Arrays.asList(
                                        new Shard().withShardId("0003"),
                                        new Shard().withShardId("0004"));

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testDescribeShardsSingleRetrieve() throws Exception
    {
        final List<Shard> expected = SHARDS_1;

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                assertEquals("request contains stream name", "example", request.getStreamName());
                return new DescribeStreamResult().withStreamDescription(
                        new StreamDescription().withShards(SHARDS_1).withHasMoreShards(Boolean.FALSE));
            }
        }.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 1000);
        assertEquals("returned expected list", expected, shards);
    }


    @Test
    public void testDescribeShardsMultiRetrieve() throws Exception
    {
        final List<Shard> expected = CollectionUtil.combine(new ArrayList<Shard>(), SHARDS_1, SHARDS_2);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                assertEquals("request contains stream name", "example", request.getStreamName());
                if ("0002".equals(request.getExclusiveStartShardId()))
                {
                    return new DescribeStreamResult().withStreamDescription(
                            new StreamDescription().withShards(SHARDS_2).withHasMoreShards(Boolean.FALSE));
                }
                else
                {
                    return new DescribeStreamResult().withStreamDescription(
                            new StreamDescription().withShards(SHARDS_1).withHasMoreShards(Boolean.TRUE));
                }
            }
        }.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 1000);
        assertEquals("returned expected list", expected, shards);
    }


    @Test
    public void testDescribeShardsStreamNotAvailable() throws Exception
    {
        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                throw new ResourceNotFoundException("whatever");
            }
        }.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 1000);
        assertEquals("returned empty", null, shards);
    }


    @Test
    public void testDescribeShardsRequestThrottling() throws Exception
    {
        final List<Shard> expected = CollectionUtil.combine(new ArrayList<Shard>(), SHARDS_1, SHARDS_2);
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                if (invocationCount.getAndIncrement() % 2 == 0)
                {
                    throw new LimitExceededException("");
                }

                if ("0002".equals(request.getExclusiveStartShardId()))
                {
                    return new DescribeStreamResult().withStreamDescription(
                            new StreamDescription().withShards(SHARDS_2).withHasMoreShards(Boolean.FALSE));
                }
                else
                {
                    return new DescribeStreamResult().withStreamDescription(
                            new StreamDescription().withShards(SHARDS_1).withHasMoreShards(Boolean.TRUE));
                }
            }
        }.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 1000);
        assertEquals("returned expected list", expected, shards);
        assertEquals("number of calls", 4, invocationCount.get());
    }


    @Test
    public void testDescribeShardsTimoutExceeded() throws Exception
    {
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                // we'll return one batch but then pretend to be throttled
                if (invocationCount.getAndIncrement() > 0)
                {
                    throw new LimitExceededException("");
                }

                return new DescribeStreamResult().withStreamDescription(
                        new StreamDescription().withShards(SHARDS_1).withHasMoreShards(Boolean.TRUE));
            }
        }.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 150);
        assertEquals("did not return anything", null, shards);
        assertEquals("number of calls", 3, invocationCount.get());
    }


    @Test
    public void testWaitForStatusNormalOperation()
    {
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                assertEquals("stream name", "example", request.getStreamName());
                StreamStatus status = (invocationCount.getAndIncrement() < 2)
                                    ? StreamStatus.CREATING
                                    : StreamStatus.ACTIVE;

                return new DescribeStreamResult().withStreamDescription(
                        new StreamDescription().withStreamStatus(status));
            }
        }.getInstance();

        long start = System.currentTimeMillis();
        StreamStatus lastStatus = KinesisUtils.waitForStatus(client, "example", StreamStatus.ACTIVE, 500);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("status",              StreamStatus.ACTIVE, lastStatus);
        assertEquals("invocation count",    3, invocationCount.get());
        assertApproximate("elapsed time",   200, elapsed, 10);
    }


    @Test
    public void testWaitForStatusTimeout()
    {
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                assertEquals("stream name", "example", request.getStreamName());
                invocationCount.getAndIncrement();
                return new DescribeStreamResult().withStreamDescription(
                        new StreamDescription().withStreamStatus(StreamStatus.CREATING));
            }
        }.getInstance();

        long start = System.currentTimeMillis();
        StreamStatus lastStatus = KinesisUtils.waitForStatus(client, "example", StreamStatus.ACTIVE, 250);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("status",              StreamStatus.CREATING, lastStatus);
        assertEquals("invocation count",    3, invocationCount.get());
        assertApproximate("elapsed time",   300, elapsed, 10);
    }


    @Test
    public void testWaitForStatusWithThrottling()
    {
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                int localCount = invocationCount.getAndIncrement();
                if (localCount < 2)
                {
                    throw new LimitExceededException("");
                }

                StreamStatus status = (localCount < 3)
                                    ? StreamStatus.CREATING
                                    : StreamStatus.ACTIVE;
                return new DescribeStreamResult().withStreamDescription(
                        new StreamDescription().withStreamStatus(status));
            }
        }.getInstance();

        // the expected sequence of calls:
        //  - limit exceeded, sleep for 100 ms
        //  - limit exceeded, sleep for 200 ms
        //  - return creating, sleep for 100 ms
        //  - return active, no sleep

        long start = System.currentTimeMillis();
        StreamStatus lastStatus = KinesisUtils.waitForStatus(client, "example", StreamStatus.ACTIVE, 500);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("status",              StreamStatus.ACTIVE, lastStatus);
        assertEquals("invocation count",    4, invocationCount.get());
        assertApproximate("elapsed time",   400, elapsed, 10);
    }


    @Test
    public void testCreateStreamHappyPath() throws Exception
    {
        final AtomicInteger describeInvocationCount = new AtomicInteger(0);
        final AtomicInteger createInvocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                assertEquals("stream name passed to describeStream", "example", request.getStreamName());

                int invocationCount = describeInvocationCount.getAndIncrement();
                if (invocationCount < 2)
                    throw new ResourceNotFoundException("");

                StreamStatus status = (invocationCount < 4)
                                    ? StreamStatus.CREATING
                                    : StreamStatus.ACTIVE;
                return new DescribeStreamResult().withStreamDescription(
                        new StreamDescription().withStreamStatus(status));
            }

            @SuppressWarnings("unused")
            public CreateStreamResult createStream(CreateStreamRequest request)
            {
                assertEquals("stream name passed to createStream", "example", request.getStreamName());
                assertEquals("shard count passed to createStream", 3,         request.getShardCount().intValue());
                createInvocationCount.incrementAndGet();
                return new CreateStreamResult();
            }
        }.getInstance();

        // the expected sequence of calls:
        //  - resource not found, sleep for 100 ms
        //  - resource not found, sleep for 100 ms
        //  - creating, sleep for 100 ms
        //  - creating, sleep for 100 ms
        //  - active, no sleep

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtils.createStream(client, "example", 3, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("stream status",                   StreamStatus.ACTIVE, status);
        assertEquals("invocations of createStream",     1, createInvocationCount.get());
        assertEquals("invocations of describeStream",   5, describeInvocationCount.get());
        assertApproximate("elapsed time",               400, elapsed, 10);
    }


    @Test
    public void testCreateStreamThrottling() throws Exception
    {
        final AtomicInteger describeInvocationCount = new AtomicInteger(0);
        final AtomicInteger createInvocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                describeInvocationCount.getAndIncrement();
                return new DescribeStreamResult().withStreamDescription(
                        new StreamDescription().withStreamStatus(StreamStatus.ACTIVE));
            }

            @SuppressWarnings("unused")
            public CreateStreamResult createStream(CreateStreamRequest request)
            {
                if (createInvocationCount.getAndIncrement() < 2)
                    throw new LimitExceededException("");
                else
                    return new CreateStreamResult();
            }
        }.getInstance();

        // expected calls:
        //  - throttled, sleep for 100 ms
        //  - throttled, sleep for 200 ms
        //  - success

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtils.createStream(client, "example", 3, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("stream status",                               StreamStatus.ACTIVE, status);
        assertApproximate("elapsed time",                           300, elapsed, 10);
        assertEquals("invocations of createStream",                 3, createInvocationCount.get());
        assertEquals("invocations of describeStream",               1, describeInvocationCount.get());
    }


    @Test
    public void testCreateStreamAlreadyExists() throws Exception
    {
        final AtomicInteger describeInvocationCount = new AtomicInteger(0);
        final AtomicInteger createInvocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                describeInvocationCount.getAndIncrement();
                return new DescribeStreamResult().withStreamDescription(
                        new StreamDescription().withStreamStatus(StreamStatus.ACTIVE));
            }

            @SuppressWarnings("unused")
            public CreateStreamResult createStream(CreateStreamRequest request)
            {
                createInvocationCount.getAndIncrement();
                throw new ResourceInUseException("");
            }
        }.getInstance();

        StreamStatus status = KinesisUtils.createStream(client, "example", 3, 500L);

        assertEquals("stream status",                               StreamStatus.ACTIVE, status);
        assertEquals("invocations of createStream",                 1, createInvocationCount.get());
        assertEquals("invocations of describeStream",               1, describeInvocationCount.get());
    }


    @Test
    public void testDeleteStreamHappyPath() throws Exception
    {
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DeleteStreamResult deleteStream(DeleteStreamRequest request)
            {
                invocationCount.getAndIncrement();
                assertEquals("stream name passed in request", "example", request.getStreamName());
                return new DeleteStreamResult();
            }
        }.getInstance();

        boolean status = KinesisUtils.deleteStream(client, "example", 500L);

        assertTrue("returned status indicates success",         status);
        assertEquals("invocations of deleteStream",             1, invocationCount.get());
    }


    @Test
    public void testDeleteStreamThrottling() throws Exception
    {
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DeleteStreamResult deleteStream(DeleteStreamRequest request)
            {
                invocationCount.getAndIncrement();
                throw new LimitExceededException("");
            }
        }.getInstance();

        // expected calls:
        //  - throttled, sleep for 100 ms
        //  - throttled, sleep for 200 ms

        long start = System.currentTimeMillis();
        boolean status = KinesisUtils.deleteStream(client, "example", 250L);
        long elapsed = System.currentTimeMillis() - start;

        assertFalse("returned status indicates failure",        status);
        assertApproximate("elapsed time",                       300, elapsed, 10);
        assertEquals("invocations of deleteStream",             2, invocationCount.get());
    }


    @Test
    public void testDeleteStreamDoesntExist() throws Exception
    {
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DeleteStreamResult deleteStream(DeleteStreamRequest request)
            {
                invocationCount.getAndIncrement();
                throw new ResourceNotFoundException("");
            }
        }.getInstance();

        boolean status = KinesisUtils.deleteStream(client, "example", 250L);

        assertFalse("returned status indicates failure",        status);
        assertEquals("invocations of deleteStream",             1, invocationCount.get());
    }
}