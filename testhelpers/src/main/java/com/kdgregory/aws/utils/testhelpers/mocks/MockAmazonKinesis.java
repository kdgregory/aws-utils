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
import java.nio.ByteBuffer;
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
 *  <p>
 *  As a convenience for debugging, sequence numbers and shard iterators use the same
 *  format: STREAM_NAME:SHARD_NUMBER:OFFSET. You can use the {@link IdHelper} class
 *  to create or parse them.
 */
public class MockAmazonKinesis
extends AbstractMock<AmazonKinesis>
{
    protected int describePageSize = 1000;
    protected int retrievePageSize = 10000;

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
     *  Adds records to the named stream; the stream must already exist. May be called
     *  multiple times; subsequent calls append records to those already in the stream.
     *  If the stream has multiple shards the records are assigned to shards arbitrarily.
     */
    public MockAmazonKinesis withRecords(String streamName, String... records)
    {
        return withRecords(streamName, Arrays.asList(records));
    }


    /**
     *  Adds records to the named stream; the stream must already exist. May be called
     *  multiple times; subsequent calls append records to those already in the stream.
     *  If the stream has multiple shards the records are assigned to shards arbitrarily.
     */
    public MockAmazonKinesis withRecords(String streamName, List<String> records)
    {
        MockStream stream = getMockStream(streamName);
        for (String record : records)
        {
            String partitionKey = createPartitionKey(record);
            MockShard shard = stream.getShardForPartitionKey(partitionKey);
            shard.addRecord(partitionKey, record);
        }
        return this;
    }


    /**
     *  Adds records to the a specific shard of the named stream; the stream must already
     *  exist. The partition keys will be generated, and may not be "correct" for the shard.
     */
    public MockAmazonKinesis withRecords(String streamName, int shardNum, String... records)
    {
        return withRecords(streamName, shardNum, Arrays.asList(records));
    }


    /**
     *  Adds records to the a specific shard of the named stream; the stream must already
     *  exist. The partition keys will be generated, and may not be "correct" for the shard.
     */
    public MockAmazonKinesis withRecords(String streamName, int shardNum, List<String> records)
    {
        MockStream stream = getMockStream(streamName);
        MockShard shard = stream.getShardOrThrow(IdHelper.formatShardId(streamName, shardNum));
        for (String record : records)
        {
            shard.addRecord(createPartitionKey(record), record);
        }
        return this;
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


    /**
     *  Sets the page size for all "retrieve" calls.
     */
    public MockAmazonKinesis withRetrievePageSize(int value)
    {
        retrievePageSize = value;
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


    public MockStream getMockStreamOrThrow(String streamName)
    {
        MockStream stream = knownStreams.get(streamName);
        if(stream == null)
        {
            throw new ResourceNotFoundException("no such stream: " + streamName);
        }
        return stream;
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
        MockStream stream = getMockStreamOrThrow(request.getStreamName());

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

    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
    {
        MockStream stream = getMockStreamOrThrow(request.getStreamName());
        MockShard shard = stream.getShardOrThrow(request.getShardId());
        return new GetShardIteratorResult()
               .withShardIterator(
                   shard.getIterator(request.getShardIteratorType(),
                                     request.getStartingSequenceNumber()));
    }

    public GetRecordsResult getRecords(GetRecordsRequest request)
    {
        String shardItx = request.getShardIterator();
        String streamName = IdHelper.extractStreamNameFromSequenceNumber(shardItx);
        String shardId = IdHelper.extractShardIdFromSequenceNumber(shardItx);
        int offset = IdHelper.extractOffsetFromSequenceNumber(shardItx);

        MockStream stream = getMockStreamOrThrow(streamName);
        MockShard shard = stream.getShardOrThrow(shardId);

        List<Record> records = shard.getRecords(offset, retrievePageSize);
        String nextIterator = shard.getNextIterator(offset + records.size());

        // TODO -- add millis
        return new GetRecordsResult()
               .withRecords(records)
               .withNextShardIterator(nextIterator)
               .withMillisBehindLatest(Long.valueOf(0));
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
        MockShard shard = mockStream.getShardForPartitionKey(record.getPartitionKey());
        String sequenceNumber = shard.addRecord(record.getPartitionKey(), record.getData());

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
     *  Generates a partition key from the passed content.
     */
    private static String createPartitionKey(String content)
    {
        return String.valueOf(content.hashCode());
    }


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
                MockShard shard = new MockShard(IdHelper.formatShardId(streamName, allShards.size()));
                activeShards.add(shard);
                allShards.add(shard);
                shardsById.put(shard.shardId, shard);
            }
        }


        public synchronized MockShard getShardOrThrow(String shardId)
        {
            MockShard shard = shardsById.get(shardId);
            if (shard == null)
                throw new ResourceNotFoundException("no such shard: " + shardId);
            return shard;
        }


        public synchronized MockShard getShardForPartitionKey(String partitionKey)
        {
            int shardNum = partitionKey.hashCode() % activeShards.size();
            return activeShards.get(shardNum);
        }


        public synchronized List<Shard> createShardsForDescribe(DescribeStreamRequest request, int pageSize)
        {
            int firstShard = request.getExclusiveStartShardId() != null
                       ? IdHelper.extractShardNumberFromShardId(request.getExclusiveStartShardId()) + 1
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
    }


    /**
     *  This class holds information about a shard, including the records that were
     *  written to it. All methods are sycnhronized but the internal structures are
     *  not: my expectation is that they will only be examined by the main thread.
     */
    public static class MockShard
    {
        public final String shardId;

        protected boolean closed = false;
        protected ArrayList<Record> records = new ArrayList<Record>();

        public MockShard(String shardId)
        {
            this.shardId = shardId;
        }


        public synchronized void close()
        {
            closed = true;
        }


        public synchronized Shard asAwsShard()
        {
            SequenceNumberRange seqnums = new SequenceNumberRange()
                                          .withStartingSequenceNumber(IdHelper.formatSequenceNumber(shardId, 0));
            if (closed)
                seqnums.setEndingSequenceNumber(IdHelper.formatSequenceNumber(shardId, records.size()));

            return new Shard()
                   .withShardId(shardId)
                   .withSequenceNumberRange(seqnums);
        }


        public synchronized String addRecord(String partitionKey, String content)
        {
            byte[] bytes = StringUtil.toUTF8(content);
            return addRecord(partitionKey, ByteBuffer.wrap(bytes));
        }


        public synchronized String addRecord(String partitionKey, ByteBuffer content)
        {
            String sequenceNumber = IdHelper.formatSequenceNumber(shardId, records.size());
            records.add(new Record()
                        .withPartitionKey(partitionKey)
                        .withData(content)
                        .withSequenceNumber(sequenceNumber));
            return sequenceNumber;
        }


        public String getIterator(String iteratorType, String sequenceNumber)
        {
            switch (ShardIteratorType.fromValue(iteratorType))
            {
                case TRIM_HORIZON :
                    return IdHelper.formatSequenceNumber(shardId, 0);
                case LATEST :
                    return IdHelper.formatSequenceNumber(shardId, records.size());
                case AFTER_SEQUENCE_NUMBER :
                    int currentOffset = IdHelper.extractOffsetFromSequenceNumber(sequenceNumber);
                    return IdHelper.formatSequenceNumber(shardId, currentOffset + 1);
                default :
                    throw new IllegalArgumentException("unsupported shard iterator type: " + iteratorType);
            }
        }


        public List<Record> getRecords(int offset, int size)
        {
            List<Record> result = new ArrayList<Record>();
            int end = Math.min(offset + size, records.size());
            for (int ii = offset ; ii < end ; ii++)
            {
                result.add(records.get(ii));
            }
            return result;
        }


        // this is called as a result of GetRecords; it signals whether the shard is closed
        public String getNextIterator(int offset)
        {
            if (closed && (offset == records.size()))
                return null;

            return IdHelper.formatShardId(shardId, offset);
        }
    }


    /**
     *  A helper class for producing and parsing various types of IDs, in the
     *  format used by this mock. NOT usable to parse actual AWS IDs.
     */
    public static class IdHelper
    {
        public static String formatShardId(String streamName, int shardNum)
        {
            return String.format("%s:%d", streamName, shardNum);
        }

        public static String extractStreamNameFromShardId(String value)
        {
            String[] split = value.split(":");
            return split[0];
        }

        public static int extractShardNumberFromShardId(String value)
        {
            String[] split = value.split(":");
            return Integer.parseInt(split[1]);
        }

        public static String formatSequenceNumber(String streamName, int shardNum, int offset)
        {
            return String.format("%s:%d:%d", streamName, shardNum, offset);
        }

        public static String formatSequenceNumber(String shardId, int offset)
        {
            return String.format("%s:%d", shardId, offset);
        }

        public static String extractShardIdFromSequenceNumber(String value)
        {
            String streamName = extractStreamNameFromSequenceNumber(value);
            int shardNumber = extractShardNumberFromSequenceNumber(value);
            return formatShardId(streamName, shardNumber);
        }

        public static String extractStreamNameFromSequenceNumber(String value)
        {
            String[] split = value.split(":");
            return split[0];
        }

        public static int extractShardNumberFromSequenceNumber(String value)
        {
            String[] split = value.split(":");
            return Integer.parseInt(split[1]);
        }

        public static int extractOffsetFromSequenceNumber(String value)
        {
            String[] split = value.split(":");
            return Integer.parseInt(split[2]);
        }
    }
}