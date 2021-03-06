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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;

import com.kdgregory.aws.utils.CommonUtils;


/**
 *  General utility functions for Kinesis. These methods perform retries with
 *  exponential backoff when throttled, and handle the case where AWS requires
 *  multiple calls to retrieve all information.
 *  <p>
 *  Functions will log warning-level messages on timeout, debug-level messages
 *  when creating or deleting resources.
 */
public class KinesisUtil
{
    private static Log logger = LogFactory.getLog(KinesisUtil.class);


    /**
     *  Extracts the status from a stream description and converts it from a string
     *  to a <code>StreamStatus</code> value. Returns null if the description is null
     *  or the status is unconvertable (which should never happen in the real world).
     */
    public static StreamStatus getStatus(StreamDescription description)
    {
        if (description == null) return null;

        try
        {
            return StreamStatus.valueOf(description.getStreamStatus());
        }
        catch (IllegalArgumentException ex)
        {
            // this should never happen with an AWS-constructed description but let's be sure
            logger.warn("getStatus: invalid status: " + description.getStreamStatus() + " for stream: " + description.getStreamName());
            return null;
        }
    }


    /**
     *  Executes a single <code>DescribeStream</code> request, returning the stream
     *  description from the response. Will return null if the stream does not exist,
     *  cannot be read within the specified timeout, or if the thread is interrupted.
     *
     *  @param  client          The AWS client used to make requests.
     *  @param  request         The request that's passed to the client.
     *  @param  timeout         The total number of milliseconds to attempt retries
     *                          due to throttling. The initial retry is 100 millis,
     *                          and this doubles for each retry.
     */
    public static StreamDescription describeStream(AmazonKinesis client, String streamName, long timeout)
    {
        return describeStream(client, new DescribeStreamRequest().withStreamName(streamName), timeout);
    }


    /**
     *  Executes a single <code>DescribeStream</code> request, returning the stream
     *  description from the response. Will return null if the stream does not exist,
     *  cannot be read within the specified timeout, or if the thread is interrupted.
     *  <p>
     *  This variant is primarily for internal use, to handle paginated shard lists.
     *  If you're interested in other parts of the description, such as stream status,
     *  it's easier to call the variant that takes a stream name.
     *
     *  @param  client          The AWS client used to make requests.
     *  @param  request         The request that's passed to the client.
     *  @param  timeout         The total number of milliseconds to attempt retries
     *                          due to throttling. The initial retry is 100 millis,
     *                          and this doubles for each retry.
     */
    public static StreamDescription describeStream(AmazonKinesis client, DescribeStreamRequest request, long timeout)
    {
        long currentSleep = 100;
        long timeoutAt = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < timeoutAt)
        {
            try
            {
                return client.describeStream(request).getStreamDescription();
            }
            catch (ResourceNotFoundException ex)
            {
                return null;
            }
            catch (LimitExceededException ex)
            {
                if (CommonUtils.sleepQuietly(currentSleep)) return null;
                currentSleep *= 2;
            }
        }

        logger.warn("describeStream timeout: " + request.getStreamName());
        return null;
    }


    /**
     *  A wrapper around <code>DescribeStream</code> that retrieves all shard
     *  descriptions and retries if throttled. Returns null if the stream does
     *  not exist, the calling thread was interrupted, or we could not retrieve
     *  all shards within the timeout (this method does not return partial
     *  results).
     *
     *  @param  client          The AWS client used to make requests.
     *  @param  streamName      Identifies the stream to describe.
     *  @param  timeout         The total number of milliseconds to attempt retries due
     *                          to throttling.
     */
    public static List<Shard> describeShards(AmazonKinesis client, String streamName, long timeout)
    {
        List<Shard> result = new ArrayList<Shard>();
        long timeoutAt = System.currentTimeMillis() + timeout;
        String lastShardId = null;
        do
        {
            long currentTimeout = timeoutAt - System.currentTimeMillis();
            DescribeStreamRequest request = new DescribeStreamRequest().withStreamName(streamName);
            if (lastShardId != null) request.setExclusiveStartShardId(lastShardId);

            StreamDescription description = describeStream(client, request, currentTimeout);
            if (description == null) return null;

            List<Shard> shards = description.getShards();
            result.addAll(shards);
            lastShardId = description.getHasMoreShards()
                        ? shards.get(shards.size() - 1).getShardId()
                        : null;
        } while (lastShardId != null);

        return result;
    }


    /**
     *  Repeatedly calls {@link #describeStream} (with a pause between calls) until
     *  the stream's status is the desired value, the calling thrad is interrupted,
     *  or the timeout expires. Returns the last retrieved status value (which may
     *  be null even if the actual status is not, due to timeout or interruption).
     *
     *  @param  client          The AWS client used to make requests.
     *  @param  streamName      Identifies the stream to describe.
     *  @param  desiredStatus   The desired stream status.
     *  @param  timeout         The total number of milliseconds to attempt retries due
     *                          to throttling.
     */
    public static StreamStatus waitForStatus(AmazonKinesis client, String streamName, StreamStatus desiredStatus, long timeout)
    {
        StreamStatus lastStatus = null;
        long timeoutAt = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < timeoutAt)
        {
            long remainingTimeout = timeoutAt - System.currentTimeMillis();
            StreamDescription description = describeStream(client, streamName, remainingTimeout);
            lastStatus = getStatus(description);
            if (lastStatus == desiredStatus)
            {
                return lastStatus;
            }

            // arbitrary sleep to minimize throttling
            CommonUtils.sleepQuietly(100);
        }

        logger.debug("waitForStatus timeout: " + streamName);
        return lastStatus;
    }


    /**
     *  Creates a stream and waits for it to become available.
     *
     *  @param  client      The AWS client used to make requests.
     *  @param  streamName  The name of the stream.
     *  @param  numShards   The number of shards to create.
     *  @param  timeout     The number of milliseconds to wait for the stream to become available (this
     *                      also controls the number of attempts that we'll make to create the stream
     *                      if throttled).
     *
     *  @return The final status retrieved. If this is <code>null</code> it means that thes stream
     *          was not created. Caller should only use the stream if it's <code>ACTIVE</code>/
     *
     *  @throws IllegalArgumentException if thrown by the underlying calls (ie, this is not caught).
     */
    public static StreamStatus createStream(AmazonKinesis client, String streamName, int numShards, long timeout)
    {
        logger.debug("createStream: " + streamName + ", with " + numShards + " shards");

        long currentSleep = 100;
        long abortAt = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < abortAt)
        {
            CreateStreamRequest request = new CreateStreamRequest()
                                              .withStreamName(streamName)
                                              .withShardCount(numShards);
            try
            {
                client.createStream(request);
                break;
            }
            catch (LimitExceededException ex)
            {
                if (CommonUtils.sleepQuietly(currentSleep)) return null;
                currentSleep *= 2;
            }
            catch (ResourceInUseException ex)
            {
                // the documentation isn't very clear on this exception; it's thrown when
                // the stream already exists
                logger.warn("createStream called with existing stream: " + streamName);
                break;
            }
        }

        return waitForStatus(client, streamName, StreamStatus.ACTIVE, (abortAt - System.currentTimeMillis()));
    }


    /**
     *  Deletes a stream, retrying if throttled. Does not wait for the stream to go
     *  away; call {@link #waitForStatus} to do that.
     *
     *  @param  client      The AWS client used to make requests.
     *  @param  streamName  The name of the stream.
     *  @param  timeout     The maximum amount of time (millis) that we'll spend trying to
     *                      delete the stream.
     *
     *  @return true if the <code>deleteStream</code> request completed successfully,
     *          <code>false</code> if it failed for any reason (timed out or the stream
     *          did not exist).
     */
    public static boolean deleteStream(AmazonKinesis client, String streamName, long timeout)
    {
        logger.debug("deleteStream: " + streamName);

        long timeoutAt = System.currentTimeMillis() + timeout;
        long currentSleep = 100;
        while (System.currentTimeMillis() < timeoutAt)
        {
            try
            {
                client.deleteStream(new DeleteStreamRequest().withStreamName(streamName));
                return true;
            }
            catch (LimitExceededException ex)
            {
                CommonUtils.sleepQuietly(currentSleep);
                currentSleep *= 2;
            }
            catch (ResourceNotFoundException ex)
            {
                logger.warn("deleteStream called for nonexistent stream: " + streamName);
                return false;
            }
        }

        logger.warn("deleteStream timeout: " + streamName);
        return false;
    }


    /**
     *  Updates a stream's retention period and waits for it to become available again. This
     *  function looks at the current retention period and chooses either the "increase" or
     *  "decrease" operation. Calling with the same retention period is a no-op (other than
     *  calls to retrieve the stream status).
     *
     *  @param  client      The AWS client used to make requests.
     *  @param  streamName  The name of the stream.
     *  @param  retention   The new retention period (hours)
     *  @param  timeout     The maximum amount of time (millis) that we'll spend trying to
     *                      update the stream or wait for it to become available.
     *
     *  @return The stream status. After successful update this will be ACTIVE. It will be
     *          <code>UPDATING</code> if the update operation completed but the stream was
     *          still updating when the timeout expired. If the update call timed out or
     *          the stream does not exist, will return <code>null</code>.
     *
     *  @throws IllegalArgumentException if thrown by underlying call. The AWS docs don't
     *          give specific reasons for this exception, but one cause is specifying a
     *          retention period outside the allowed 24..168 hours.
     */
    public static StreamStatus updateRetentionPeriod(AmazonKinesis client, String streamName, int retention, long timeout)
    {
        logger.debug("updateRetentionPeriod: " + streamName + " to " + retention + " hours");

        long timeoutAt = System.currentTimeMillis() + timeout;

        // this is certainly not the most efficient way to do things, but the code is simple
        // and nobody is going to be calling this method in a tight loop

        int currentSleep = 100;
        while (System.currentTimeMillis() < timeoutAt)
        {
            StreamStatus initialStatus = waitForStatus(client, streamName, StreamStatus.ACTIVE, timeout);
            if (initialStatus == null)
            {
                // probable timeout while waiting for stream to become active
                return null;
            }

            // there is a chance that status changes between the last describe and this one
            // but we'll simply let the update call fail rather than use an inner test

            DescribeStreamRequest describeRequest = new DescribeStreamRequest().withStreamName(streamName);
            StreamDescription description = describeStream(client, describeRequest, (timeoutAt - System.currentTimeMillis()));
            if (description == null)
            {
                // stream has magically disappeared between two calls or we've timed out
                return null;
            }
            if (getStatus(description) != StreamStatus.ACTIVE)
            {
                // race condition, someone else is changing stream so wait until they're done
                continue;
            }

            int currentRetentition = description.getRetentionPeriodHours().intValue();
            if (currentRetentition == retention)
            {
                // nothing to see here, move along
                return StreamStatus.ACTIVE;
            }

            try
            {
                if (currentRetentition > retention)
                {
                    DecreaseStreamRetentionPeriodRequest request = new DecreaseStreamRetentionPeriodRequest()
                                                                   .withStreamName(streamName)
                                                                   .withRetentionPeriodHours(retention);
                    client.decreaseStreamRetentionPeriod(request);
                    break;
                }
                else
                {
                    IncreaseStreamRetentionPeriodRequest request = new IncreaseStreamRetentionPeriodRequest()
                                                                   .withStreamName(streamName)
                                                                   .withRetentionPeriodHours(retention);
                    client.increaseStreamRetentionPeriod(request);
                    break;
                }
            }
            catch (LimitExceededException ex)
            {
                // fall out to the end of the loop
            }
            catch (ResourceInUseException ex)
            {
                // fall out to the end of the loop
            }
            catch (ResourceNotFoundException ex)
            {
                // stream doesn't exist so no point in trying to do anything more
                logger.warn("updateRetentionPeriod called for nonexistent stream: " + streamName);
                return null;
            }

            CommonUtils.sleepQuietly(currentSleep);
            currentSleep *= 2;
        }

        return waitForStatus(client, streamName, StreamStatus.ACTIVE, (timeoutAt - System.currentTimeMillis()));
    }


    /**
     *  Changes the number of shards in a stream, waits for it to become active again,
     *  and verifies that it has the expected number of open shards.
     *  <p>
     *  The AWS docs note that intermediate shards may be created as part of a reshard
     *  operation. In my experience, the stream may be marked as ACTIVE before the final
     *  shards have been created, thus the need to verify the number of open shards (this
     *  is, in my opinion, rather hacky, but seems to work).
     *  <p>
     *  Unlike other functions in this class, there is no attempt to retry as a result
     *  of throttling: AWS imposes daily limits on stream reshards, so any attempt to
     *  retry the operation is likely to result in a timeout. Caller should be prepared
     *  for exceptions, particularly <code>LimitExceededException</code>.
     *  <p>
     *  Requires minimum AWS SDK version 1.11.55.
     *
     *  @param  client          The AWS client used to make requests.
     *  @param  streamName      The name of the stream.
     *  @param  numberOfShards  The desired number of shards. Per AWS documentation, the
     *                          reshard operation is most efficient if the new number of
     *                          is twice or half the current number of shards.
     *  @param  timeout         Milliseconds that this function will wait for the stream
     *                          to become active and the shard count to stabilize.
     *
     *  @return StreamStatus.ACTIVE to indicate that the reshard completed and the number
     *          of shards were verified, <code>null</code> to indicate that the timout
     *          expired before this happened (this return type is used for consistency
     *          with other functions; the actual information returned is boolean in nature).
     */
    public static StreamStatus reshard(
        AmazonKinesis client, String streamName, int numberOfShards, long timeout)
    {
        logger.debug("reshard: " + streamName + " to " + numberOfShards + " shards");

        long timeoutAt = System.currentTimeMillis() + timeout;

        client.updateShardCount(new UpdateShardCountRequest()
                                .withStreamName(streamName)
                                .withTargetShardCount(numberOfShards)
                                .withScalingType(ScalingType.UNIFORM_SCALING));

        while (System.currentTimeMillis() < timeoutAt)
        {
            StreamStatus lastStatus = KinesisUtil.waitForStatus(client, streamName, StreamStatus.ACTIVE, timeoutAt - System.currentTimeMillis());
            if (lastStatus == null)
                break;

            if (lastStatus == StreamStatus.UPDATING)
                continue;

            List<Shard> shards = KinesisUtil.describeShards(client, streamName, timeoutAt - System.currentTimeMillis());
            if (shards == null)
                break;

            int openShards = 0;
            for (Shard shard : shards)
            {
                if (shard.getSequenceNumberRange().getEndingSequenceNumber() == null)
                {
                    openShards++;
                }
            }

            if (openShards == numberOfShards)
                return StreamStatus.ACTIVE;
        }

        logger.warn("reshard timeout waiting for stream to stabilize: " + streamName);
        return null;
    }


    /**
     *  Retrieves a single shard iterator, retrying until a caller-specified timeout.
     *  This function provides arguments for multiple iterator types; pass null for
     *  those arguments that don't apply to your call.
     *
     *  @param  client          The AWS client used to make requests.
     *  @param  streamName      The name of the stream.
     *  @param  shardId         The shard identifier.
     *  @param  iteratorType    The type of iterator to retrieve.
     *  @param  sequenceNumber  The relevant sequence number, for iterator types
     *                          that need one. Ignored for iterator types that don't.
     *  @param  timestamp       A starting timestamp, for the AT_TIMESTAMP iterator
     *                          type only. Ignored for all other iterator types.
     *  @param  timeout         The number of milliseconds to attempt retries due
     *                          to throttling.
     *
     *  @return The shard iterator, null if unable to do so within timeout.
     *
     *  @throws All non-throttling exceptions from underlying call.
     *
     */
    public static String retrieveShardIterator(
        AmazonKinesis client, String streamName, String shardId,
        ShardIteratorType iteratorType, String sequenceNumber, Date timestamp,
        long timeout)
    {
        GetShardIteratorRequest request = new GetShardIteratorRequest()
                      .withStreamName(streamName)
                      .withShardId(shardId)
                      .withShardIteratorType(iteratorType);

        switch (iteratorType)
        {
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                request.setStartingSequenceNumber(sequenceNumber);
                break;
            case AT_TIMESTAMP:
                request.setTimestamp(timestamp);
                break;
            default:
                // nothing special
        }

        long timeoutAt = System.currentTimeMillis() + timeout;
        long currentSleepTime = 100;
        while (System.currentTimeMillis() < timeoutAt)
        {
            try
            {
                GetShardIteratorResult response = client.getShardIterator(request);
                return response.getShardIterator();
            }
            catch (ProvisionedThroughputExceededException ex)
            {
                CommonUtils.sleepQuietly(currentSleepTime);
                currentSleepTime *= 2;
            }
        }

        logger.warn("retrieveShardIterator timeout: " + streamName + " shard " + shardId);
        return null;
    }


    /**
     *  A utility function that transforms a list of shards into a map of
     *  shards keyed by the shard ID. This is used internally and may be
     *  generally useful.
     */
    public static Map<String,Shard> toMapById(Collection<Shard> shards)
    {
        Map<String,Shard> result = new HashMap<String,Shard>();
        for (Shard shard : shards)
        {
            result.put(shard.getShardId(), shard);
        }
        return result;
    }


    /**
     *  A utility function that transforms a list of shards into a map of
     *  shards keyed by their parent ID. This is used internally and may
     *  be generally useful.
     */
    public static Map<String,List<Shard>> toMapByParentId(Collection<Shard> shards)
    {
        Map<String,List<Shard>> result = new HashMap<String,List<Shard>>();
        for (Shard shard : shards)
        {
            List<Shard> children = result.get(shard.getParentShardId());
            if (children == null)
            {
                children = new ArrayList<Shard>();
                result.put(shard.getParentShardId(), children);
            }
            children.add(shard);
        }
        return result;
    }
}
