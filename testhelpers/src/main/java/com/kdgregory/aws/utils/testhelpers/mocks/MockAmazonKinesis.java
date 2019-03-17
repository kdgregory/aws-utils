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

package com.kdgregory.aws.utils.testhelpers.mocks;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.kdgcommons.collections.CollectionUtil;
import net.sf.kdgcommons.lang.StringUtil;

import static org.junit.Assert.*;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.kinesisfirehose.model.InvalidArgumentException;
import com.amazonaws.util.BinaryUtils;


/**
 *  Mock for testing Kinesis operation. Based on <code>SelfMock</code>, so subclasses
 *  can override individual Kinesis methods. However, most methods that tests might
 *  want to override have hooks that can be overridden instead; see method docs.
 */
public class MockAmazonKinesis
extends AbstractMock<AmazonKinesis>
{
    protected int describePageSize = Integer.MAX_VALUE;

    // this will be updated either during mock configuration or when creating/deleting a stream
    protected Map<String,MockStream> knownStreams = new ConcurrentHashMap<String,MockStream>();

    // status chains are separate from the streams because we can return status for a missing stream
    protected Map<String,StatusChain> statusChains = new ConcurrentHashMap<String,StatusChain>();


    /**
     *  Base constructor, typically used for overrides or when you're testing
     *  stream creation. If the latter, you need to set a status chain for the
     *  streams that you create.
     */
    public MockAmazonKinesis()
    {
        super(AmazonKinesis.class);
    }


    /**
     *  Convenience constructor that sets an expected stream name with one shard
     *  and active status.
     */
    public MockAmazonKinesis(String streamName)
    {
        this();
        withStream(streamName, 1);
    }

//----------------------------------------------------------------------------
//  Optional configuration
//----------------------------------------------------------------------------

    /**
     *  Adds a known stream to this mock; calls to get and put will be validated
     *  against it. The stream descriptions will return a status of ACTIVE unless
     *  you also call {@link #withStatusChain}.
     */
    public MockAmazonKinesis withStream(String streamName, int numShards)
    {
        knownStreams.put(streamName, new MockStream(streamName, numShards));
        return withStatusChain(streamName, StreamStatus.ACTIVE);
    }


    /**
     *  Defines a sequence of status values and/or exceptions to return from
     *  <code>describeStream()</code>. Specify exceptions as their class, status
     *  values as a <code>StreamStatus</code> enum.
     *  <p>
     *  The list of statuses is consumed by calls to <code>describeStream</code>,
     *  with the value in the list being repeated forever. If you need to test a
     *  more complex scenario, you can reset the chain at any point.
     */
    public MockAmazonKinesis withStatusChain(String streamName, Object... statuses)
    {
        statusChains.put(streamName, new StatusChain(statuses));
        return this;
    }


    /**
     *  Sets the page size for all "describe" calls.
     */
    public MockAmazonKinesis withDescribePageSize(int value)
    {
        describePageSize = value;
        return this;
    }

//----------------------------------------------------------------------------
//  Invocation accessors
//----------------------------------------------------------------------------

    public PutRecordsRequest getMostRecentPutRecordsRequest()
    {
        return getMostRecentInvocationArg("putRecords", 0, PutRecordsRequest.class);
    }


    public List<String> getMostRecentPutRecordsContent()
    {
        List<String> result = new ArrayList<String>();
        for (PutRecordsRequestEntry entry : getMostRecentPutRecordsRequest().getRecords())
        {
            byte[] bytes = BinaryUtils.copyBytesFrom(entry.getData());
            result.add(StringUtil.fromUTF8(bytes));
        }
        return result;
    }


    public Set<String> getMostRecentPutRecordsPartitionKeys()
    {
        Set<String> result = new TreeSet<String>();
        for (PutRecordsRequestEntry entry : getMostRecentPutRecordsRequest().getRecords())
        {
            result.add(entry.getPartitionKey());
        }
        return result;
    }


    public MockStream getMockStream(String streamName)
    {
        return knownStreams.get(streamName);
    }

//----------------------------------------------------------------------------
//  Mock implementations
//----------------------------------------------------------------------------

    public CreateStreamResult createStream(CreateStreamRequest request)
    {
        String streamName = request.getStreamName();

        // this exception determined via integration test (the documentation is misleading)
        if (knownStreams.containsKey(streamName))
            throw new ResourceInUseException("stream already exists: " + streamName);


        knownStreams.put(streamName, new MockStream(streamName, request.getShardCount().intValue()));
        return new CreateStreamResult();
    }


    public DeleteStreamResult deleteStream(DeleteStreamRequest request)
    {
        String streamName = request.getStreamName();
        if (! knownStreams.containsKey(streamName))
            throw new ResourceNotFoundException("stream does not exist: " + streamName);

        knownStreams.remove(streamName);
        return new DeleteStreamResult();
    }


    public DescribeStreamResult describeStream(DescribeStreamRequest request)
    {
        String streamName = request.getStreamName();
        MockStream mockStream = knownStreams.get(streamName);
        if (mockStream == null)
        {
            throw new ResourceNotFoundException("unknown stream: " + streamName);
        }

        return new DescribeStreamResult().withStreamDescription(
            createDescription(request, mockStream, statusChains.get(streamName)));
    }


    public DecreaseStreamRetentionPeriodResult decreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest request)
    {
        String streamName = request.getStreamName();
        MockStream stream = knownStreams.get(streamName);
        if(stream == null)
        {
            throw new ResourceNotFoundException("no such stream: " + streamName);
        }

        int newRetentionPeriod = request.getRetentionPeriodHours().intValue();
        if (newRetentionPeriod >= stream.retentionPeriod)
        {
            throw new InvalidArgumentException(
                "new retention period must be lower than old (was: " + stream.retentionPeriod
                + ", attempted to set: " + newRetentionPeriod + ")");
        }

        stream.retentionPeriod = newRetentionPeriod;
        return new DecreaseStreamRetentionPeriodResult();
    }


    public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest request)
    {
        String streamName = request.getStreamName();
        MockStream stream = knownStreams.get(streamName);
        if(stream == null)
        {
            throw new ResourceNotFoundException("no such stream: " + streamName);
        }

        int newRetentionPeriod = request.getRetentionPeriodHours().intValue();
        if (newRetentionPeriod <= stream.retentionPeriod)
        {
            throw new InvalidArgumentException(
                "new retention period must be higher than old (was: " + stream.retentionPeriod
                + ", attempted to set: " + newRetentionPeriod + ")");
        }

        stream.retentionPeriod = newRetentionPeriod;
        return new IncreaseStreamRetentionPeriodResult();
    }


    public PutRecordsResult putRecords(PutRecordsRequest request)
    {
        String streamName = request.getStreamName();
        MockStream mockStream = knownStreams.get(streamName);
        if (mockStream == null)
        {
            throw new ResourceNotFoundException("unknown stream: " + streamName);
        }

        List<PutRecordsResultEntry> results = new ArrayList<PutRecordsResultEntry>();
        int recordIndex = 0;
        int failedRecordCount = 0;
        for (PutRecordsRequestEntry requestEntry : request.getRecords())
        {
            assertNotNull("putRecords: partition key for record " + recordIndex, requestEntry.getPartitionKey());

            PutRecordsResultEntry resultEntry = processRecord(mockStream, requestEntry, recordIndex++);
            results.add(resultEntry);
            if (! StringUtil.isEmpty(resultEntry.getErrorCode()))
                failedRecordCount++;
        }

        return new PutRecordsResult()
               .withRecords(results)
               .withFailedRecordCount(failedRecordCount);
    }


    public UpdateShardCountResult updateShardCount(UpdateShardCountRequest request)
    {
        String streamName = request.getStreamName();
        MockStream stream = knownStreams.get(streamName);
        if (stream == null)
        {
            throw new ResourceNotFoundException("missing stream: " + streamName);
        }

        int currentCount = stream.activeShards.size();
        stream.reshard(request.getTargetShardCount().intValue());

        return new UpdateShardCountResult()
               .withStreamName(streamName)
               .withCurrentShardCount(currentCount)
               .withTargetShardCount(request.getTargetShardCount());
    }


    public void shutdown()
    {
        // nothing happening here, assertions just verify invocation count
    }

//----------------------------------------------------------------------------
//  Hooks that allow subclasses to change results
//----------------------------------------------------------------------------

    /**
     *  This hook is called for each record in a <code>putRecords()</code> call.
     *  By default it creates a reasonable response, including distributing
     *  records to available shards.
     */
    protected PutRecordsResultEntry processRecord(MockStream mockStream, PutRecordsRequestEntry record, int index)
    {
        MockShard shard = mockStream.getShardForRecord(record);
        String sequenceNumber = shard.addRecord(record);

        return new PutRecordsResultEntry()
               .withShardId(shard.shardId)
               .withSequenceNumber(sequenceNumber);
    }


    /**
     *  This hook allows subclasses to create their own stream description or throw
     *  a custom exception from within the describeStream call. It relies on {@link
     *  #applyStatus} (which may throw) and {@link #applyShards} to do its work.
     */
    protected StreamDescription createDescription(DescribeStreamRequest request, MockStream mockStream, StatusChain statusChain)
    {
        List<Shard> shards = mockStream.createShardsForDescribe(request, describePageSize);

        return new StreamDescription()
                   .withStreamName(mockStream.streamName)
                   .withRetentionPeriodHours(Integer.valueOf(mockStream.retentionPeriod))
                   .withStreamStatus(statusChain.next())
                   .withShards(shards)
                   .withHasMoreShards(! mockStream.isLastShard(CollectionUtil.last(shards)));
    }

//----------------------------------------------------------------------------
//  Helpers -- should be no need to override these
//----------------------------------------------------------------------------

    /**
     *  This class holds a chain of status responses for DescribeStream. These
     *  may either be actual status responses or exception classes; the former
     *  are returned as is, the latter are thrown via reflection. The last
     *  item in the stream will be repeated indefinitely.
     */
    private static class StatusChain
    {
        private LinkedList<Object> returns = new LinkedList<Object>();

        public StatusChain(Object... returns)
        {
            this.returns.addAll(Arrays.asList(returns));
        }


        /**
         *  Returns the next value or throws the next exception from this chain.
         */
        public StreamStatus next()
        {
            Object status = (returns.size() > 1)
                          ? returns.removeFirst()
                          : returns.getFirst();

            if (status instanceof StreamStatus)
                return (StreamStatus)status;

            if (! (status instanceof Class))
                throw new IllegalStateException("StatusChain improperly configured: " + status);

            RuntimeException throwMe = null;
            try
            {
                Class<? extends RuntimeException> klass = (Class<? extends RuntimeException>)status;
                Constructor<? extends RuntimeException> ctor = klass.getConstructor(String.class);
                throwMe = ctor.newInstance("generated exception");
            }
            catch (Exception ex)
            {
                throwMe = new RuntimeException("StatusChain unable to create exception", ex);
            }

            throw throwMe;
        }
    }


    /**
     *  This class holds information about a shard, including the records that
     *  were written to it. All methods are sycnronized but the internal structures
     *  are not: my expectation is that they will only be examined by the main test
     *  thread.
     */
    public static class MockShard
    {
        public final String shardId;

        protected boolean closed = false;
        protected List<PutRecordsRequestEntry> records = new ArrayList<PutRecordsRequestEntry>();

        public MockShard(String shardId)
        {
            this.shardId = shardId;
        }

        public synchronized void close()
        {
            closed = true;
        }

        public synchronized String addRecord(PutRecordsRequestEntry record)
        {
            String sequenceNumber = formatSequenceNumber(records.size());
            records.add(record);
            return sequenceNumber;
        }


        public synchronized Shard asAwsShard()
        {
            SequenceNumberRange seqnums = new SequenceNumberRange()
                                          .withStartingSequenceNumber(formatSequenceNumber(0));
            if (closed)
                seqnums.setEndingSequenceNumber(formatSequenceNumber(records.size()));

            return new Shard()
                   .withShardId(shardId)
                   .withSequenceNumberRange(seqnums);
        }


        public static String formatSequenceNumber(int ii)
        {
            return String.format("%06d", ii);
        }


        public static int parseSequenceNumber(String value)
        {
            return Integer.parseInt(value);
        }
    }


    /**
     *  This class holds information about a single mock stream, including its shards.
     */
    public static class MockStream
    {
        public final String streamName;

        // these parameters can be changed
        public int retentionPeriod = 24;

        public List<MockShard> allShards = new ArrayList<MockShard>();
        public List<MockShard> activeShards = new ArrayList<MockShard>();
        public Map<String,MockShard> shardsById = new TreeMap<String,MockShard>();


        public MockStream(String streamName, int numShards)
        {
            this.streamName = streamName;
            reshard(numShards);
        }

        public synchronized void reshard(int newShardCount)
        {
            // Kinesis is smarter about expanding and collapsing shards, but
            // for this pass I'm just going to replace all the active shards

            for (MockShard shard : activeShards)
            {
                shard.close();
            }

            activeShards.clear();

            for (int ii = 0 ; ii < newShardCount ; ii++)
            {
                MockShard shard = new MockShard(formatShardId(allShards.size()));
                activeShards.add(shard);
                allShards.add(shard);
                shardsById.put(shard.shardId, shard);
            }
        }


        public synchronized MockShard getShardForRecord(PutRecordsRequestEntry record)
        {
            int shardNum = record.getPartitionKey().hashCode() % activeShards.size();
            return activeShards.get(shardNum);
        }


        public synchronized List<Shard> createShardsForDescribe(DescribeStreamRequest request, int pageSize)
        {
            int firstShard = request.getExclusiveStartShardId() != null
                       ? parseShardId(request.getExclusiveStartShardId()) + 1
                       : 0;
            int lastShard  = Math.min(firstShard + pageSize, allShards.size());

            List<Shard> shards = new ArrayList<Shard>();
            for (int ii = firstShard ; ii < lastShard ; ii++)
            {
                shards.add(allShards.get(ii).asAwsShard());
            }

            return shards;
        }


        public synchronized boolean isLastShard(Shard shard)
        {
            String shardId = shard.getShardId();
            String lastShardId = CollectionUtil.last(allShards).shardId;
            return lastShardId.equals(shardId);
        }


        public String formatShardId(int shardNum)
        {
            return String.format("%s-%04d", streamName, shardNum);
        }


        public int parseShardId(String value)
        {
            return Integer.parseInt(StringUtil.extractRightOfLast(value, "-"));
        }
    }
}