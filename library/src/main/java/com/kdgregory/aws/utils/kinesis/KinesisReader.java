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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;


/**
 *  An instantiable class that represents a Kinesis stream as an iterable: each
 *  call to {@link #iterator} returns an iteration over a set of unread messages
 *  from the stream. It is intended for the use case where each consumer processes
 *  all messages in the stream, <em>not</em> for the case where there are multiple
 *  coordinated consumers reading the same stream.
 *  <p>
 *  Unlike a "normal" <code>Iterable</code>, the returned iterator does not read
 *  the entire stream. Instead, it calls <code>GetRecords</code> once for each
 *  shard in the stream, and then reports no more elements. This allows the caller
 *  to sleep between calls (to avoid throttling), as well as providing a good spot
 *  to retrieve and save sequence numbers.
 *
 *  <h1> Construction and Configuration </h1>
 *
 *  The sole constructor takes just two arguments: a Kinesis client object and the
 *  name of the stream to be read. This produces a reader that starts reading at
 *  the current end of stream. Specifically, it identifies all open shards in the
 *  stream creates a <code>LATEST</code> shard iterator for them.
 *  <p>
 *  This behavior can be altered via several configuration methods, each of which
 *  can be chained.
 *
 *  <h1> Sequence Numbers </h1>
 *
 *  To support applications that must start and stop, the reader retains the sequence
 *  number for the last record read from each shard. You can retrieve the current set
 *  of sequence numbers with {@link #getCurrentSequenceNumbers()}, and use them to
 *  configure a new reader via {@link #withInitialSequenceNumbers(Map)}.
 *  <p>
 *  When you configure a reader with initial sequence numbers, that changes the
 *  behavior of the reader:
 *  <ul>
 *  <li> If none of the <em>current</em> shards in the stream correspond to the
 *       saved sequence numbers, then the reader uses the default shard iterator
 *       type. This covers the case where saved sequence numbers are long out of
 *       date.
 *  <li> For any shard that has a saved sequence number, <em>provided that there
 *       are no descendent shards with saved sequence numbers,</em> the reader
 *       creates an <code>AFTER_SEQUENCE_NUMBER</code> iterator based on the
 *       saved sequence number.
 *  <li> If there are saved sequence numbers for a shard and its descendents, only
 *       the descendent sequence numbers are retained. This prevents re-reading a
 *       shard once we've started reading its children.
 *  <li> If there is a saved sequence number for a shard but not its sibling, then
 *       the sibling is given a <code>TRIM_HORIZON</code> iterator. This covers
 *       the case where a reader was interrupted before it could make a pass of all
 *       shards.
 *  </ul>
 *  For simplicity, the reader does not purge sequence numbers when finished with a
 *  shard. This avoids a situation where the application reads the last record from
 *  a shard but does not start reading records from its children. When initialized
 *  a sequence number from the end of a closed shard, it will automatically start
 *  reading from the child(ren). As a result, you can simply upsert sequence numbers
 *  to persist them; the reader will do the right thing.
 *  <p>
 *  The reader does, however, purge sequence numbers for expired shards (shards that
 *  have been closed and whose records are older than the retention period). This is
 *  only relevant if you frequently reshard the stream, and allows you to reduce the
 *  storage required for sequence numbers (you can freely delete any saved numbers
 *  that are not in the most recent call to {@link #getCurrentSequenceNumbers()}).
 *
 *  <h1> Error Handling and Timeouts </h1>
 *
 *  Non-recoverable errors, such as <code>ResourceNotFoundException</code> are
 *  thrown; the caller must be prepared to handle these or let them propagate.
 *  Recoverable errors, such as <code>ProvisionedThroughputExceededException</code>,
 *  are logged at WARN level but otherwise ignored: no records are returned for the
 *  affected shard.
 *  <p>
 *  There is no attempt to retry <code>GetRecords</code> request that are throttled;
 *  the assumption is that the caller will sleep and then iterate again. However,
 *  requests to retrieve shard iterators (and the stream description) are retried,
 *  with a timeout (and logged warning) if they fail to complete within a specified
 *  amount of time. The default timeout should be sufficient unless you have a large
 *  number of shards and multiple applications trying to read them; in that case you
 *  can call {@link #withTimeout} to increase the timeout as needed.
 *
 *  <h1> General Notes </h1>
 *
 *  Within a shard, records are returned in order. However, shards are read in
 *  arbitrary order, so there is not guarantee of total record ordering (Kinesis
 *  cannot make that guarantee anyway, due to distribution and resend of incoming
 *  records).
 *  <p>
 *  Due to the way that Kinesis stores records, any particular iteration may
 *  not return anything. This is particularly noticeable when you're at the
 *  end of the stream: the <code>GetRecords</code> call will always return an
 *  empty list of records.
 *  <p>
 *  To avoid throttling, the caller should sleep for an appropriate interval
 *  between creating iterators (where "appropriate" depends on the number of
 *  clients that are reading the stream simultaneously). As described above,
 *  throttling exceptions may be transparently handled via retries, but the
 *  application should not rely on this behavior.
 *  <p>
 *  Instances of this class and the iterators that they produce are <em>not</em>
 *  safe for concurrent use from multiple threads.
 *
 *  <h1> Example </h1>
 *
 *  This example assumes that the sequence numbers from the stream have been
 *  saved somewhere. After each iteration it will save the offsets and then
 *  sleep before iterating again.
 *
 *  <pre>
 *  AmazonKinesis client = AmazonKinesisClientBuilder.defaultClient();
 *  Map<String,String> offsets = // retrieve from somewhere or use empty map
 *  KinesisReader reader = new KinesisReader(client, "example").withInitialSequenceNumbers(offsets);
 *
 *  while (true) {
 *      for (Record record : reader) {
 *          byte[] data = BinaryUtils.copyAllBytesFrom(record.getData());
 *          // do something with data
 *      }
 *      offsets = reader.getCurrentSequenceNumbers();
 *      // save these offsets
 *      Thread.sleep(1000);
 *  }
 *  </pre>
 */
public class KinesisReader
implements Iterable<Record>
{
    private Log logger = LogFactory.getLog(getClass());

    private AmazonKinesis client;
    private String streamName;
    private ShardIteratorType defaultIteratorType;
    private long timeout;

    // these are populated by describeShards(), which is called when updating iterators
    private List<Shard> shards;
    private Map<String,Shard> shardsById;
    private Map<String,List<Shard>> shardsByParentId;

    // while the javadoc says that shards are iterated in an arbitrary order, I
    // want a known order for testing; thus the internal maps are TreeMaps
    private Map<String,String> shardIterators = new TreeMap<String,String>();

    // we'll copy callers offsets if so desired
    private Map<String,String> offsets = new TreeMap<String,String>();

    // this will contain the maximum value across all shards for current iteration
    private long millisBehindLatest;

//----------------------------------------------------------------------------
//  Constructor and Configuration Methods
//----------------------------------------------------------------------------

    /**
     *  Constructs an instance that starts reading the stream at its current end.
     *  Call one or more of the configuration methods to change this behavior.
     *
     *  @param  client              Used to access Kinesis. Client configuration
     *                              determines the region containing the stream.
     *  @param  streamName          The stream to read.
     */
    public KinesisReader(AmazonKinesis client, String streamName)
    {
        this.client = client;
        this.streamName = streamName;
        this.timeout = 2500;
        this.defaultIteratorType = ShardIteratorType.LATEST;
    }


    /**
     *  Configures the stream to start reading at the first record. Specifically,
     *  identifies all shards that do not have parents, and creates a
     *  <code>TRIM_HORIZON</code> iterator for them.
     *  <p>
     *  Note that this method has no effect if you call {@link #withInitialSequenceNumbers}
     *  with a non-empty map.
     */
    public KinesisReader readFromTrimHorizon()
    {
        this.defaultIteratorType = ShardIteratorType.TRIM_HORIZON;
        return this;
    }


    /**
     *  Sets the initial sequence numbers for the stream's shards from the passed
     *  map: keys are shard IDs, values are sequence numbers. If the map is empty
     *  then this method acts as a no-op and reader behavior depends on default
     *  iterator type.
     */
    public KinesisReader withInitialSequenceNumbers(Map<String,String> value)
    {
        this.offsets.putAll(value);
        return this;
    }


    /**
     *  Sets the timeout, in milliseconds, for Kinesis interaction. The default
     *  value of 2500 should be sufficient for most usages, but a large number
     *  of shards may require a higher value to avoid timing out while loading
     *  shard iterators.
     */
    public KinesisReader withTimeout(long millis)
    {
        this.timeout = millis;
        return this;
    }


//----------------------------------------------------------------------------
//  Public API
//----------------------------------------------------------------------------

    /**
     *  Returns an iterator that reads all current shards in the stream. Each iterator
     *  makes a single pass through the shards, at which point the calling application
     *  should sleep to avoid throttling errors.
     *  <p>
     *  Simultaneous use of multiple iterators from the same reader is undefined behavior.
     *  <p>
     *  Iterators are not safe for concurrent use from multiple threads.
     */
    @Override
    public Iterator<Record> iterator()
    {
        millisBehindLatest = 0;
        return new RecordIterator();
    }


    /**
     *  Returns a map of the most recent sequence numbers read for each shard. This can
     *  be saved and used to initialize a new reader. The returned map is a copy of the
     *  current reader offsets; the caller may do with it whatever they want.
     */
    public Map<String,String> getCurrentSequenceNumbers()
    {
        return new TreeMap<String,String>(offsets);
    }


    /**
     *  Returns an estimate of the reader's lag behind the end of the stream. This
     *  value is calculated as the maximum value for all shards in the current
     *  iteration. As such, it is only valid <em>at the end of the iteration</em>.
     */
    public long getMillisBehindLatest()
    {
        return millisBehindLatest;
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  Each instance of this iterator will make a single pass through the active shards,
     *  making one <code>GetRecords</code> call for each.
     */
    private class RecordIterator implements Iterator<Record>
    {
        private LinkedList<String> shardIds;
        private String currentShardId;
        private LinkedList<Record> currentRecords = new LinkedList<Record>();


        /**
         *  Determines whether there are more records to be read from this iteration
         *  of the stream.
         */
        @Override
        public boolean hasNext()
        {
            if (currentRecords.isEmpty())
            {
                findNextShardWithRecords();
            }
            return ! currentRecords.isEmpty();
        }


        /**
         *  Returns the next record from the stream. Note that records read from
         *  different shards may not be read in the order they were written.
         */
        @Override
        public Record next()
        {
            if (! hasNext())
                throw new NoSuchElementException("no more records for this iteration of " + streamName);

            Record record = currentRecords.removeFirst();
            offsets.put(currentShardId, record.getSequenceNumber());
            return record;
        }


        /**
         *  Unsupported operation: cannot remove a record from a Kinesis stream.
         */
        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Kinesis streams are immutable");
        }

        //---------------------------------------------------------------------

        private void findNextShardWithRecords()
        {
            // this happens when the reader first starts, or if iterators were
            // purged due to timeout
            if (shardIterators.isEmpty())
            {
                retrieveInitialIterators();
            }

            // this happens on the first call for each new iterator; if we were
            // unable to retrieve iterators the list will be empty and we'll be
            // done before we start
            if (shardIds == null)
            {
                shardIds = new LinkedList<String>(shardIterators.keySet());
            }

            while (currentRecords.isEmpty() && (! shardIds.isEmpty()))
            {
                currentShardId = shardIds.removeFirst();
                readCurrentShard();
            }
        }


        private void readCurrentShard()
        {
            String shardItx = shardIterators.get(currentShardId);
            if (shardItx == null)
            {
                // this will happen if the stream is resized under us; it can also
                // happen if we reload iterators halfway through an iteration due
                // to iterator expiration
                return;
            }

            if (logger.isDebugEnabled())
            {
                logger.debug("readCurrentShard: stream = " + streamName + ", shard = " + currentShardId);
            }

            try
            {
                GetRecordsResult response = client.getRecords(new GetRecordsRequest().withShardIterator(shardItx));
                currentRecords.addAll(response.getRecords());
                millisBehindLatest = Math.max(millisBehindLatest, response.getMillisBehindLatest().longValue());

                String nextShardIterator = response.getNextShardIterator();
                shardIterators.put(currentShardId, nextShardIterator);
                if (nextShardIterator == null)
                {
                    retrieveChildIterators(currentShardId);
                }
            }
            catch (ProvisionedThroughputExceededException ex)
            {
                logger.warn("provisioned throughput exceeded: stream = " + streamName + ", shard = " + currentShardId);
            }
            catch (ExpiredIteratorException ex)
            {
                logger.warn("iterator expired: stream = " + streamName + ", shard = " + currentShardId);
                retrieveInitialIterators();
            }
        }
    }


    /**
     *  Updates the iterator for the specified shard. If passed a null iterator,
     *  replaces the iterator with those of its children.
     */
    public void updateIterator(String shardId, String shardIterator)
    {
        if (shardIterator == null)
        {
            // this method will remove the existing iterator if able to retrieve children
            retrieveChildIterators(shardId);
        }
    }


    /**
     *  Reloads shard iterators based on current offsets / default iterator type. This
     *  is called when the reader is first used, as well as for expired iterators. If
     *  unable to retrieve shards, silently fails (will retrieve on next iterator).
     */
    private void retrieveInitialIterators()
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("retrieveInitialIterators: stream = " + streamName + ", saved sequence numbers = " + offsets);
        }

        shardIterators.clear();

        long timeoutAt = System.currentTimeMillis() + timeout;
        describeShards();
        if (shards == null)
            return;

        // this purges any expired shards from the offsets
        for (String shardId : new ArrayList<String>(offsets.keySet()))
        {
            if (! shardsById.containsKey(shardId))
            {
                offsets.remove(shardId);
            }
        }

        Map<String,ShardIteratorType> itxTypes = new HashMap<String,ShardIteratorType>();
        if (! constructMixedIteratorTree(shardsByParentId.get(null), itxTypes))
        {
            populateDefaultIterators(itxTypes);
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("retrieveInitialIterators: stream = " + streamName + ", iterator types = " + itxTypes);
        }

        retrieveIterators(itxTypes, timeoutAt);
    }


    private boolean constructMixedIteratorTree(List<Shard> children, Map<String,ShardIteratorType> itxTypes)
    {
        if (children == null)
            return false;

        Set<String> childrenWithOffsets = new HashSet<String>();
        for (Shard child : children)
        {
            String childShardId = child.getShardId();
            if (constructMixedIteratorTree(shardsByParentId.get(childShardId), itxTypes))
            {
                childrenWithOffsets.add(childShardId);
            }
            else if (offsets.containsKey(childShardId))
            {
                itxTypes.put(childShardId, ShardIteratorType.AFTER_SEQUENCE_NUMBER);
                childrenWithOffsets.add(childShardId);
            }
        }

        if (childrenWithOffsets.size() == 0)
            return false;

        if (childrenWithOffsets.size() == children.size())
            return true;

        for (Shard child : children)
        {
            String childShardId = child.getShardId();
            if (! childrenWithOffsets.contains(childShardId))
            {
                itxTypes.put(childShardId, ShardIteratorType.TRIM_HORIZON);
            }
        }
        return true;
    }


    private void populateDefaultIterators(Map<String,ShardIteratorType> itxTypes)
    {
        if (defaultIteratorType == ShardIteratorType.TRIM_HORIZON)
        {
            for (Shard shard : shardsByParentId.get(null))
            {
                itxTypes.put(shard.getShardId(), defaultIteratorType);
            }
        }
        else
        {
            for (Shard shard : shards)
            {
                if (shardsByParentId.get(shard.getShardId()) == null)
                {
                    itxTypes.put(shard.getShardId(), ShardIteratorType.LATEST);
                }
            }
        }
    }


    /**
     *  Called when we reach the end of a parent shard, to load iterators for its
     *  children. If unable to do so, will silently ignore
     */
    private void retrieveChildIterators(String parentShardId)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("retrieveChildIterators: stream = " + streamName + ", parentShardId = " + parentShardId);
        }

        long timeoutAt = System.currentTimeMillis() + timeout;
        describeShards();
        if (shards == null)
            return;

        List<Shard> children = shardsByParentId.get(parentShardId);
        if (children == null)
        {
            logger.warn("retrieveChildIterators: did not find any children; stream = " + streamName + ", parentShardId = " + parentShardId);
            return;
        }

        Map<String,ShardIteratorType> itxTypes = new TreeMap<String,ShardIteratorType>();
        for (Shard shard : children)
        {
            itxTypes.put(shard.getShardId(), ShardIteratorType.TRIM_HORIZON);
        }

        if (retrieveIterators(itxTypes, timeoutAt))
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("retrieveChildIterators: replacing shard with children; stream = " + streamName 
                             + ", parentShardId = " + parentShardId + ", child shard IDs = " + itxTypes.keySet());
            }
            shardIterators.remove(parentShardId);
        }
    }


    private void describeShards()
    {
        shards = KinesisUtil.describeShards(client, streamName, timeout);
        if (shards == null)
        {
            logger.warn("failed to retrieve shard description: stream = " + streamName);
            return;
        }

        shardsById = KinesisUtil.toMapById(shards);
        shardsByParentId = KinesisUtil.toMapByParentId(shards);
    }


    /**
     *  Retrieves a iterators specific set of shards (maybe not all shards in stream) based on
     *  the specified iterator types. Returns <code>true</code> if able to do this, <code>false</code>
     *  if the retrieve times-out.
     */
    private boolean retrieveIterators(Map<String,ShardIteratorType> iteratorTypes, long timeoutAt)
    {
        // we store in a local map so that we don't apply a partial update to the master map
        Map<String,String> result = new TreeMap<String,String>();

        for (Map.Entry<String,ShardIteratorType> entry : iteratorTypes.entrySet())
        {
            String shardId = entry.getKey();
            ShardIteratorType iteratorType = entry.getValue();
            String offset = offsets.get(shardId);

            long currentTimeout = timeoutAt - System.currentTimeMillis();
            String shardIterator = (currentTimeout > 0)
                                 ? KinesisUtil.retrieveShardIterator(client, streamName, shardId, iteratorType, offset, null, currentTimeout)
                                 : null;
            if (shardIterator == null)
            {
                logger.warn("failed to retrieve shard iterator: stream = " + streamName + ", shard = " + shardId);
                return false;
            }

            result.put(shardId, shardIterator);
        }

        shardIterators.putAll(result);
        return true;
    }
}
