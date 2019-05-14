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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.*;
import org.apache.log4j.spi.*;

import net.sf.kdgcommons.collections.CollectionUtil;
import net.sf.kdgcommons.lang.StringUtil;
import net.sf.kdgcommons.test.SelfMock;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.util.BinaryUtils;

import com.kdgregory.aws.utils.testhelpers.mocks.MockAmazonKinesis;
import com.kdgregory.aws.utils.testhelpers.mocks.MockAmazonKinesis.IdHelper;


/**
 *  Mock-object tests of KinesisUtils.
 */
public class TestKinesisReader
{
    private Logger myLogger = Logger.getLogger(getClass());

    private LinkedBlockingQueue<LoggingEvent> loggingEvents = new LinkedBlockingQueue<LoggingEvent>();

//----------------------------------------------------------------------------
//  Some common test parameters
//----------------------------------------------------------------------------

    private final static String STREAM_NAME = "example";
    
    private final static String SHARDID_0 = IdHelper.formatShardId(STREAM_NAME, 0);
    private final static String SHARDID_1 = IdHelper.formatShardId(STREAM_NAME, 1);
    private final static String SHARDID_2 = IdHelper.formatShardId(STREAM_NAME, 2);
    private final static String SHARDID_3 = IdHelper.formatShardId(STREAM_NAME, 3);
    private final static String SHARDID_4 = IdHelper.formatShardId(STREAM_NAME, 4);

    private final static String OLDSTYLE_SHARDID_0 = formatShardId(0);
    private final static String OLDSTYLE_SHARDID_1 = formatShardId(1);
    private final static String OLDSTYLE_SHARDID_2 = formatShardId(2);
    private final static String OLDSTYLE_SHARDID_3 = formatShardId(3);
    private final static String OLDSTYLE_SHARDID_4 = formatShardId(4);

    private final static List<String> RECORDS_0 = Arrays.asList("foo", "bar", "baz");
    private final static List<String> RECORDS_1 = Arrays.asList("argle", "bargle");
    private final static List<String> RECORDS_2 = Arrays.asList("bingo", "zippy");
    private final static List<String> RECORDS_3 = Arrays.asList("crunchy", "bits");
    private final static List<String> RECORDS_4 = Arrays.asList("norwegian", "blue");
    
    private final static String LAST_SEQNUM_0 = IdHelper.formatSequenceNumber(SHARDID_0, RECORDS_0.size() - 1);
    private final static String LAST_SEQNUM_1 = IdHelper.formatSequenceNumber(SHARDID_1, RECORDS_1.size() - 1);


//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    private static String formatShardId(int id)
    {
        return String.format("shard-%03d", id);
    }


    private static String formatSequenceNumber(String shardId, int index)
    {
        return shardId + "," + String.format("%06d", index);
    }


    private static String extractShardIdFromSeqnum(String sequenceNumber)
    {
        return sequenceNumber.split(",")[0];
    }


    private static int extractOffsetFromSeqnum(String sequenceNumber)
    {
        return Integer.parseInt(sequenceNumber.split(",")[1]);
    }


    /**
     *  Identifies all log messages that contain the specified text, and asserts
     *  their level.
     */
    public void assertLogMessages(String message, String searchtext, int expectedCount, Level expectedLevel)
    {
        int actualCount = 0;
        for (LoggingEvent logEvent : loggingEvents)
        {
            if (logEvent.getRenderedMessage().contains(searchtext))
            {
                assertEquals(message + ": log level", expectedLevel, logEvent.getLevel());
                actualCount++;
            }
        }
        assertEquals(message + ": number of messages", expectedCount, actualCount);
    }


    /**
     *  Retrieves all records from a single iteration of the reader, and verifies
     *  that the reader's offsets are updated after each record is read.
     */
    private static List<String> retrieveRecords(KinesisReader reader)
    throws Exception
    {
        List<String> retrievedRecords = new ArrayList<String>();
        for (Record record : reader)
        {
            byte[] recordData = BinaryUtils.copyAllBytesFrom(record.getData());
            retrievedRecords.add(new String(recordData, "UTF-8"));
            boolean recordedSeqnum = false;
            // TODO - after all tests converted, use the helper to extract shard ID from record
            for (String seqnum : reader.getCurrentSequenceNumbers().values())
            {
                recordedSeqnum |= seqnum.equals(record.getSequenceNumber());
            }
            assertTrue("reader recorded record's sequence number", recordedSeqnum);
        }
        return retrievedRecords;
    }


    /**
     *  A mock class that supports just enough behavior to make the reader work.
     *  Construct with one or more lists of strings that represent the records
     *  in a shard.
     *  <p>
     *  Note: to simplify mock implementation, shard iterators and sequence
     *  numbers use the same format.
     */
    public static class KinesisMock extends SelfMock<AmazonKinesis>
    {
        private String expectedStreamName;
        private Map<String,Shard> shardsById = new HashMap<String,Shard>();
        private Map<String,List<Record>> recordsByShard = new HashMap<String,List<Record>>();

        public KinesisMock(String expectedStreamName, List<String>... contentToReturn)
        {
            super(AmazonKinesis.class);
            this.expectedStreamName = expectedStreamName;
            for (int ii = 0 ; ii < contentToReturn.length ; ii++)
            {
                String shardId = formatShardId(ii);
                List<Record> records = new ArrayList<Record>();
                for (int jj = 0 ; jj < contentToReturn[ii].size() ; jj++)
                {
                    byte[] data = StringUtil.toUTF8(contentToReturn[ii].get(jj));
                    records.add(new Record()
                                .withSequenceNumber(formatSequenceNumber(shardId, jj))
                                .withPartitionKey("ignored")
                                .withData(ByteBuffer.wrap(data)));
                }
                recordsByShard.put(shardId, records);
            }

            for (Shard shard : createShards(recordsByShard.keySet()))
            {
                shardsById.put(shard.getShardId(), shard);
            }
        }

        public DescribeStreamResult describeStream(DescribeStreamRequest request)
        {
            assertEquals("request contains stream name", expectedStreamName, request.getStreamName());

            return new DescribeStreamResult()
                       .withStreamDescription(new StreamDescription()
                           .withStreamName(expectedStreamName)
                           .withStreamStatus(StreamStatus.ACTIVE)
                           .withShards(shardsById.values())
                           .withHasMoreShards(Boolean.FALSE));
        }

        public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
        {
            assertShardIteratorRequest(request);

            String shardId = request.getShardId();

            int offset = 0;
            switch (ShardIteratorType.fromValue(request.getShardIteratorType()))
            {
                case TRIM_HORIZON :
                    // default value is OK
                    break;
                case LATEST :
                    offset = recordsByShard.get(shardId).size();
                    break;
                case AFTER_SEQUENCE_NUMBER :
                    offset = extractOffsetFromSeqnum(request.getStartingSequenceNumber()) + 1;
                    break;
                default :
                    throw new IllegalArgumentException("unexpected shard iterator type: " + request.getShardIteratorType());
            }

            return new GetShardIteratorResult().withShardIterator(formatSequenceNumber(shardId, offset));
        }

        public GetRecordsResult getRecords(GetRecordsRequest request)
        {
            String shardItx = request.getShardIterator();
            String shardId = extractShardIdFromSeqnum(shardItx);
            int offset = extractOffsetFromSeqnum(shardItx);
            List<Record> shardRecords = recordsByShard.get(shardId);
            List<Record> remainingRecords = shardRecords.subList(offset, shardRecords.size());
            List<Record> returnedRecords = limitReturnedRecords(remainingRecords);

            // we only return a null shard iterator if the shard has been closed
            String nextShardItx = formatSequenceNumber(shardId, offset + returnedRecords.size());
            if (nextShardItx.equals(shardsById.get(shardId).getSequenceNumberRange().getEndingSequenceNumber()))
                nextShardItx = null;

            return new GetRecordsResult()
                   .withRecords(returnedRecords)
                   .withNextShardIterator(nextShardItx)
                   .withMillisBehindLatest(Long.valueOf(0));
        }

        // hooks for subclasses

        protected List<Shard> createShards(Collection<String> shardIds)
        {
            List<Shard> shards = new ArrayList<Shard>();
            for (String shardId : shardIds)
            {
                shards.add(new Shard()
                           .withShardId(shardId)
                           .withSequenceNumberRange(new SequenceNumberRange()
                                                    .withStartingSequenceNumber(formatSequenceNumber(shardId, 0))));
            }
            return shards;
        }

        protected void assertShardIteratorRequest(GetShardIteratorRequest request)
        {
            // default does nothing
        }

        protected List<Record> limitReturnedRecords(List<Record> records)
        {
            return records;
        }
    }

//----------------------------------------------------------------------------
//  Setup and teardown
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        Logger readerLogger = Logger.getLogger(KinesisReader.class);
        readerLogger.setLevel(Level.DEBUG);
        readerLogger.addAppender(new AppenderSkeleton()
        {
            @Override
            public void close()
            {
                // no-op
            }

            @Override
            public boolean requiresLayout()
            {
                return false;
            }

            @Override
            protected void append(LoggingEvent event)
            {
                loggingEvents.add(event);
            }
        });
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testSingleShardNoOffsetsDefaultTrimHorizon() throws Exception
    {
        myLogger.info("testSingleShardNoOffsetsDefaultTrimHorizon");
        
        MockAmazonKinesis mock = new MockAmazonKinesis()
        {
            @Override
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("iterator type", ShardIteratorType.TRIM_HORIZON, ShardIteratorType.fromValue(request.getShardIteratorType()));
                return super.getShardIterator(request);
            }
        }
        .withStream(STREAM_NAME, 1)
        .withRecords(STREAM_NAME, RECORDS_0);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        assertEquals("retrieved records",       RECORDS_0,      retrieveRecords(reader));
        assertEquals("saved offsets size",      1,              reader.getCurrentSequenceNumbers().size());
        assertEquals("saved offset value",      LAST_SEQNUM_0,  reader.getCurrentSequenceNumbers().get(SHARDID_0));
    }


    @Test
    public void testSingleShardNoOffsetsDefaultLatest() throws Exception
    {
        myLogger.info("testSingleShardNoOffsetsDefaultLatest");
        
        MockAmazonKinesis mock = new MockAmazonKinesis()
        {
            @Override
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("iterator type", ShardIteratorType.LATEST, ShardIteratorType.fromValue(request.getShardIteratorType()));
                return super.getShardIterator(request);
            }
        }
        .withStream(STREAM_NAME, 1)
        .withRecords(STREAM_NAME, RECORDS_0);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME);

        assertEquals("retrieved records",   Collections.emptyList(),    retrieveRecords(reader));
        assertEquals("saved offsets size",  0,                          reader.getCurrentSequenceNumbers().size());
    }


    @Test
    public void testSingleShardWithOffsets() throws Exception
    {
        myLogger.info("testSingleShardWithOffsets");
        
        final String initialSequenceNumber = MockAmazonKinesis.IdHelper.formatSequenceNumber(STREAM_NAME, 0, 0);
        
        MockAmazonKinesis mock = new MockAmazonKinesis()
        {
            @Override
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("iterator type",   ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
                assertEquals("sequence number", initialSequenceNumber,                   request.getStartingSequenceNumber());
                return super.getShardIterator(request);
            }
        }
        .withStream(STREAM_NAME, 1)
        .withRecords(STREAM_NAME, RECORDS_0);
        
        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(MockAmazonKinesis.IdHelper.formatShardId(STREAM_NAME, 0), initialSequenceNumber);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).withInitialSequenceNumbers(offsets);

        assertEquals("retrieved records",       RECORDS_0.subList(1, RECORDS_0.size()),
                                                retrieveRecords(reader));
        assertEquals("saved offsets, size",     1,
                                                reader.getCurrentSequenceNumbers().size());
        assertEquals("saved offsets, value",    LAST_SEQNUM_0,
                                                reader.getCurrentSequenceNumbers().get(SHARDID_0));
    }


    @Test
    public void testSingleShardRepeatedRead() throws Exception
    {
        myLogger.info("testSingleShardRepeatedRead");
        
        MockAmazonKinesis mock = new MockAmazonKinesis()
            .withStream(STREAM_NAME, 1)
            .withRecords(STREAM_NAME, RECORDS_0)
            .withRetrievePageSize(2);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        List<String> firstRead = retrieveRecords(reader);
        assertEquals("first read, records",     RECORDS_0.subList(0, 2),
                                                firstRead);
        assertEquals("first read, offsets",     IdHelper.formatSequenceNumber(SHARDID_0, 1),
                                                reader.getCurrentSequenceNumbers().get(SHARDID_0));

        List<String> secondRead = retrieveRecords(reader);
        assertEquals("second read, records",    RECORDS_0.subList(2, RECORDS_0.size()),
                                                secondRead);
        assertEquals("second read, offsets",    LAST_SEQNUM_0,
                                                reader.getCurrentSequenceNumbers().get(SHARDID_0));
    }


    @Test
    public void testSingleShardAtEndOfStreamDoesNotRepeat() throws Exception
    {
        myLogger.info("testSingleShardAtEndOfStreamDoesNotRepeat");
        
        MockAmazonKinesis mock = new MockAmazonKinesis()
            .withStream(STREAM_NAME, 1)
            .withRecords(STREAM_NAME, RECORDS_0);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        List<String> firstRead = retrieveRecords(reader);
        assertEquals("first read, records",     RECORDS_0,
                                                firstRead);
        assertEquals("first read, offsets",     LAST_SEQNUM_0,
                                                reader.getCurrentSequenceNumbers().get(SHARDID_0));

        List<String> secondRead = retrieveRecords(reader);
        assertEquals("second read, records",    Collections.emptyList(),
                                                secondRead);
        assertEquals("second read, offsets",    LAST_SEQNUM_0,
                                                reader.getCurrentSequenceNumbers().get(SHARDID_0));
    }
    
    
    // TODO - add test that adds records after reader was created


    @Test
    public void testMultipleShardsNoOffsetsDefaultTrimHorizon() throws Exception
    {
        myLogger.info("testMultipleShardsNoOffsetsDefaultTrimHorizon");
        
        MockAmazonKinesis mock = new MockAmazonKinesis()
            .withStream(STREAM_NAME, 2)
            .withRecords(STREAM_NAME, 0, RECORDS_0)
            .withRecords(STREAM_NAME, 1, RECORDS_1);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        assertEquals("retrieved records",       CollectionUtil.combine(new ArrayList<String>(), RECORDS_0, RECORDS_1),
                                                retrieveRecords(reader));
        assertEquals("saved offsets, size",     2,
                                                reader.getCurrentSequenceNumbers().size());
        assertEquals("saved offsets, shard 0",  LAST_SEQNUM_0,
                                                reader.getCurrentSequenceNumbers().get(SHARDID_0));
        assertEquals("saved offsets, shard 1",  LAST_SEQNUM_1,
                                                reader.getCurrentSequenceNumbers().get(SHARDID_1));
    }


    @Test
    public void testMultipleShardsWithOffsets() throws Exception
    {
        myLogger.info("testMultipleShardsWithOffsets");

        final String sequenceNumber0 = MockAmazonKinesis.IdHelper.formatSequenceNumber(STREAM_NAME, 0, 0);
        final String sequenceNumber1 = MockAmazonKinesis.IdHelper.formatSequenceNumber(STREAM_NAME, 1, 0);
        
        MockAmazonKinesis mock = new MockAmazonKinesis()
        {
            @Override
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {                
                if (request.getShardId().equals(SHARDID_0))
                {
                    assertEquals("shard 0 iterator type",   ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
                    assertEquals("shard 0 sequence number", sequenceNumber0, request.getStartingSequenceNumber());
                }
                else if (request.getShardId().equals(SHARDID_1))
                {
                    assertEquals("shard 1 iterator type",   ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
                    assertEquals("shard 1 sequence number", sequenceNumber1, request.getStartingSequenceNumber());
                }
                else
                {
                    fail("unexpected shard: " + request.getShardId());
                }
                return super.getShardIterator(request);
            }
        }
        .withStream(STREAM_NAME, 2)
        .withRecords(STREAM_NAME, 0, RECORDS_0)
        .withRecords(STREAM_NAME, 1, RECORDS_1);

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARDID_0, sequenceNumber0);
        offsets.put(SHARDID_1, sequenceNumber1);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).withInitialSequenceNumbers(offsets);

        assertEquals("retrieved records",       CollectionUtil.combine(new ArrayList<String>(),
                                                                       RECORDS_0.subList(1, RECORDS_0.size()),
                                                                       RECORDS_1.subList(1, RECORDS_1.size())),
                                                retrieveRecords(reader));
        assertEquals("saved offsets, size",     2,
                                                reader.getCurrentSequenceNumbers().size());
        assertEquals("saved offsets, shard 0",  LAST_SEQNUM_0,
                                                reader.getCurrentSequenceNumbers().get(SHARDID_0));
        assertEquals("saved offsets, shard 1",  LAST_SEQNUM_1,
                                                reader.getCurrentSequenceNumbers().get(SHARDID_1));
    }


    @Test
    public void testMultipleShardsOnlyOneHasOffsets() throws Exception
    {
        myLogger.info("testMultipleShardsOnlyOneHasOffsets");

        final String sequenceNumber0 = MockAmazonKinesis.IdHelper.formatSequenceNumber(STREAM_NAME, 0, 0);
        
        MockAmazonKinesis mock = new MockAmazonKinesis()
        {
            @Override
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {                
                if (request.getShardId().equals(SHARDID_0))
                {
                    assertEquals("shard 0 iterator type",   ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
                    assertEquals("shard 0 sequence number", sequenceNumber0, request.getStartingSequenceNumber());
                }
                else if (request.getShardId().equals(SHARDID_1))
                {
                    assertEquals("shard 1 iterator type",   ShardIteratorType.TRIM_HORIZON, ShardIteratorType.fromValue(request.getShardIteratorType()));
                }
                else
                {
                    fail("unexpected shard: " + request.getShardId());
                }
                return super.getShardIterator(request);
            }
        }
        .withStream(STREAM_NAME, 2)
        .withRecords(STREAM_NAME, 0, RECORDS_0)
        .withRecords(STREAM_NAME, 1, RECORDS_1);

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARDID_0, sequenceNumber0);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).withInitialSequenceNumbers(offsets);

        assertEquals("retrieved records",
                     CollectionUtil.combine(new ArrayList<String>(), RECORDS_0.subList(1, RECORDS_0.size()),
                                                                     RECORDS_1),
                     retrieveRecords(reader));

        // at this point I believe that offsets will be updated
    }


    @Test
    public void testParentIsConsumedBeforeChild() throws Exception
    {
        myLogger.info("testParentIsConsumedBeforeChild");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1)
        {
            @Override
            protected List<Shard> createShards(Collection<String> shardIds)
            {
                Shard shard0 = new Shard()
                                 .withShardId(OLDSTYLE_SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_0, 0))
                                     .withEndingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_0, RECORDS_0.size())));

                Shard shard1 = new Shard()
                                 .withShardId(OLDSTYLE_SHARDID_1)
                                 .withParentShardId(OLDSTYLE_SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_1, 0)));

                return Arrays.asList(shard0, shard1);
            }
        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        assertEquals("records from first read", RECORDS_0, retrieveRecords(reader));
        assertNotNull("after first read, has offsets for parent",   reader.getCurrentSequenceNumbers().get(OLDSTYLE_SHARDID_0));
        assertNull("after first read, has no offsets for child",     reader.getCurrentSequenceNumbers().get(OLDSTYLE_SHARDID_1));

        assertEquals("records from second read", RECORDS_1, retrieveRecords(reader));
        assertNotNull("after second read, has offsets for child",   reader.getCurrentSequenceNumbers().get(OLDSTYLE_SHARDID_1));
    }


    @Test
    public void testUnbalancedTreeOffsetsOnShortSide() throws Exception
    {
        myLogger.info("testUnbalancedTreeOffsetsOnShortSide");

        final String sequenceNumber1 = formatSequenceNumber(OLDSTYLE_SHARDID_1, 0);

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1, RECORDS_2, RECORDS_3, RECORDS_4)
        {
            @Override
            protected List<Shard> createShards(Collection<String> shardIds)
            {
                Shard shard0 = new Shard()
                                 .withShardId(OLDSTYLE_SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_0, 0))
                                     .withEndingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_0, RECORDS_0.size())));

                Shard shard1 = new Shard()
                                 .withShardId(OLDSTYLE_SHARDID_1)
                                 .withParentShardId(OLDSTYLE_SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_1, 0)));

                Shard shard2 = new Shard()
                                 .withShardId(OLDSTYLE_SHARDID_2)
                                 .withParentShardId(OLDSTYLE_SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_2, 0))
                                     .withEndingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_2, RECORDS_2.size())));

                Shard shard3 = new Shard()
                                 .withShardId(OLDSTYLE_SHARDID_3)
                                 .withParentShardId(OLDSTYLE_SHARDID_2)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_3, 0)));

                Shard shard4 = new Shard()
                                 .withShardId(OLDSTYLE_SHARDID_4)
                                 .withParentShardId(OLDSTYLE_SHARDID_2)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_4, 0)));

                return Arrays.asList(shard0, shard1, shard2, shard3, shard4);
            }

            @Override
            protected void assertShardIteratorRequest(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(OLDSTYLE_SHARDID_0))
                {
                    fail("should not have queried shard 0");
                }
                else if (request.getShardId().equals(OLDSTYLE_SHARDID_1))
                {
                    assertEquals("shard 1 iterator type",   ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
                    assertEquals("shard 1 sequence number", sequenceNumber1,                         request.getStartingSequenceNumber());
                }
                else
                {
                    assertEquals(request.getShardId() + " iterator type", ShardIteratorType.TRIM_HORIZON, ShardIteratorType.fromValue(request.getShardIteratorType()));
                }
            }
        };

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(OLDSTYLE_SHARDID_1, sequenceNumber1);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).withInitialSequenceNumbers(offsets);

        assertEquals("records from first read",
                     CollectionUtil.combine(new ArrayList<String>(), RECORDS_1.subList(1, RECORDS_1.size()),
                                                                     RECORDS_2),
                     retrieveRecords(reader));

        assertEquals("records from second read",
                     CollectionUtil.combine(new ArrayList<String>(), RECORDS_3, RECORDS_4),
                     retrieveRecords(reader));

        assertEquals("records from third read",
                     Collections.emptyList(),
                     retrieveRecords(reader));
    }


    @Test
    public void testUnbalancedTreeOffsetsOnLongSide() throws Exception
    {
        myLogger.info("testUnbalancedTreeOffsetsOnLongSide");

        final String sequenceNumber4 = formatSequenceNumber(OLDSTYLE_SHARDID_4, 0);

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1, RECORDS_2, RECORDS_3, RECORDS_4)
        {
            @Override
            protected List<Shard> createShards(Collection<String> shardIds)
            {
                Shard shard0 = new Shard()
                                 .withShardId(OLDSTYLE_SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_0, 0))
                                     .withEndingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_0, RECORDS_0.size())));

                Shard shard1 = new Shard()
                                 .withShardId(OLDSTYLE_SHARDID_1)
                                 .withParentShardId(OLDSTYLE_SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_1, 0)));

                Shard shard2 = new Shard()
                                 .withShardId(OLDSTYLE_SHARDID_2)
                                 .withParentShardId(OLDSTYLE_SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_2, 0))
                                     .withEndingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_2, RECORDS_2.size())));

                Shard shard3 = new Shard()
                                 .withShardId(OLDSTYLE_SHARDID_3)
                                 .withParentShardId(OLDSTYLE_SHARDID_2)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_3, 0)));

                Shard shard4 = new Shard()
                                 .withShardId(OLDSTYLE_SHARDID_4)
                                 .withParentShardId(OLDSTYLE_SHARDID_2)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(OLDSTYLE_SHARDID_4, 0)));

                return Arrays.asList(shard0, shard1, shard2, shard3, shard4);
            }

            @Override
            protected void assertShardIteratorRequest(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(OLDSTYLE_SHARDID_0))
                {
                    fail("should not have queried shard 0");
                }
                else if (request.getShardId().equals(OLDSTYLE_SHARDID_1))
                {
                    assertEquals("shard 1 iterator type",   ShardIteratorType.TRIM_HORIZON, ShardIteratorType.fromValue(request.getShardIteratorType()));
                }
                else if (request.getShardId().equals(OLDSTYLE_SHARDID_2))
                {
                    fail("should not have queried shard 2");
                }
                else if (request.getShardId().equals(OLDSTYLE_SHARDID_3))
                {
                    assertEquals("shard 3 iterator type",   ShardIteratorType.TRIM_HORIZON, ShardIteratorType.fromValue(request.getShardIteratorType()));
                }
                else if (request.getShardId().equals(OLDSTYLE_SHARDID_4))
                {
                    assertEquals("shard 4 iterator type",   ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
                    assertEquals("shard 4 sequence number", sequenceNumber4,                         request.getStartingSequenceNumber());
                }
                else
                {
                    fail("unexpected iterator request: " + request.getShardId());
                }
            }
        };

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(OLDSTYLE_SHARDID_4, sequenceNumber4);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).withInitialSequenceNumbers(offsets);

        assertEquals("records from first read",
                     CollectionUtil.combine(new ArrayList<String>(), RECORDS_1, RECORDS_3, RECORDS_4.subList(1, RECORDS_4.size())),
                     retrieveRecords(reader));

        assertEquals("records from second read",
                     Collections.emptyList(),
                     retrieveRecords(reader));
    }


    @Test
    public void testSavedOffsetsDontCorrespondToCurrentShards() throws Exception
    {
        myLogger.info("testSavedOffsetsDontCorrespondToCurrentShards");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0);

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put("bogus", "123");

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME)
                               .withInitialSequenceNumbers(offsets)
                               .readFromTrimHorizon();

        assertEquals("retrieved records",       RECORDS_0,
                                                retrieveRecords(reader));
        assertEquals("saved offsets, size",     1,
                                                reader.getCurrentSequenceNumbers().size());
        assertEquals("saved offsets, shard 0",  formatSequenceNumber(OLDSTYLE_SHARDID_0, RECORDS_0.size() - 1),
                                                reader.getCurrentSequenceNumbers().get(OLDSTYLE_SHARDID_0));
    }


    @Test
    public void testRecordThrottling() throws Exception
    {
        myLogger.info("testThrottling");

        final AtomicInteger getRecordsInvocationCount = new AtomicInteger(0);

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1)
        {
            @Override
            public GetRecordsResult getRecords(GetRecordsRequest request)
            {
                if ((getRecordsInvocationCount.getAndIncrement() % 2) == 0)
                    throw new ProvisionedThroughputExceededException("testing");

                return super.getRecords(request);
            }
        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        List<String> firstRead = retrieveRecords(reader);
        assertEquals("first read, records returned",                        Collections.emptyList(), firstRead);
        assertEquals("after first read, number of calls to GetRecords",     1, getRecordsInvocationCount.get());

        List<String> secondRead = retrieveRecords(reader);
        assertEquals("second read, records returned",                       RECORDS_1, secondRead);
        assertEquals("after second read, number of calls to GetRecords",    2, getRecordsInvocationCount.get());

        assertLogMessages("throttling log messages", "provisioned throughput exceeded", 1, Level.WARN);
    }


    @Test
    public void testExpiredIterator() throws Exception
    {
        myLogger.info("testExpiredIterator");

        final AtomicInteger getRecordsInvocationCount = new AtomicInteger(0);

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1)
        {
            @Override
            public GetRecordsResult getRecords(GetRecordsRequest request)
            {
                if ((getRecordsInvocationCount.getAndIncrement() % 2) == 0)
                    throw new ExpiredIteratorException("testing");

                return super.getRecords(request);
            }
        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        List<String> firstRead = retrieveRecords(reader);
        assertEquals("first read, records returned",                        Collections.emptyList(), firstRead);
        assertEquals("after first read, number of calls to GetRecords",     1, getRecordsInvocationCount.get());

        List<String> secondRead = retrieveRecords(reader);
        assertEquals("second read, records returned",                       RECORDS_1, secondRead);
        assertEquals("after second read, number of calls to GetRecords",    2, getRecordsInvocationCount.get());

        assertLogMessages("expired iterator log messages", "iterator expired", 1, Level.WARN);
    }


    @Test
    public void testTimeoutRetrievingShardDescription() throws Exception
    {
        myLogger.info("testTimeoutRetrievingShardDescription");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1, RECORDS_2)
        {
            @Override
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                throw new LimitExceededException("testing");
            }
        };

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(OLDSTYLE_SHARDID_0, formatSequenceNumber(OLDSTYLE_SHARDID_0, 0));

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME)
                               .withInitialSequenceNumbers(offsets)
                               .readFromTrimHorizon()
                               .withTimeout(50);

        List<String> firstRead = retrieveRecords(reader);
        assertEquals("first read, records returned", Collections.emptyList(), firstRead);

        List<String> secondRead = retrieveRecords(reader);
        assertEquals("first read, records returned", Collections.emptyList(), secondRead);

        assertEquals("offsets unchanged", offsets, reader.getCurrentSequenceNumbers());

        assertLogMessages("unable to retrieve iterators log messages", "failed to retrieve shard description", 2, Level.WARN);
    }


    @Test
    public void testTimeoutRetrievingShardIterators() throws Exception
    {
        myLogger.info("testTimeoutRetrievingShardIterators");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1, RECORDS_2)
        {
            @Override
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                throw new ProvisionedThroughputExceededException("testing");
            }
        };

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(OLDSTYLE_SHARDID_0, formatSequenceNumber(OLDSTYLE_SHARDID_0, 0));

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME)
                               .withInitialSequenceNumbers(offsets)
                               .readFromTrimHorizon()
                               .withTimeout(50);

        List<String> firstRead = retrieveRecords(reader);
        assertEquals("first read, records returned", Collections.emptyList(), firstRead);

        List<String> secondRead = retrieveRecords(reader);
        assertEquals("first read, records returned", Collections.emptyList(), secondRead);

        assertEquals("offsets unchanged", offsets, reader.getCurrentSequenceNumbers());

        assertLogMessages("unable to retrieve iterators log messages", "failed to retrieve shard iterator", 2, Level.WARN);
    }


    @Test
    public void testMillisBehindLatest() throws Exception
    {
        myLogger.info("testMillisBehindLatest");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1)
        {
            LinkedList<Long> millisByRequest = new LinkedList<Long>(Arrays.asList(350L, 600L));

            @Override
            public GetRecordsResult getRecords(GetRecordsRequest request)
            {
                return super.getRecords(request)
                       .withMillisBehindLatest(millisByRequest.removeFirst());
            }

        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon().withTimeout(50);

        assertEquals("value before iteration", 0, reader.getMillisBehindLatest());

        retrieveRecords(reader);

        assertEquals("value after all records read", 600, reader.getMillisBehindLatest());

        reader.iterator();

        assertEquals("value after creating new iterator", 0, reader.getMillisBehindLatest());
    }
}
