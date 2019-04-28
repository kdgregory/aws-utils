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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

/**
 *  Writes messages to CloudWatch Logs in batches, either under application control
 *  or using a background thread. Messages may be added concurrently by any thread.
 *  Any thread may also call {@link #flush}; invocations are synchronized.
 *  <p>
 *  The writer maintains a set of monitoring variables, such as the number of calls
 *  to {@link #flush}, which can be examined by the application. It writes debug
 *  logging messages for significant events (such as creating a log stream), and
 *  warning messages for unexpected exceptions or rejected messages.
 */
public class CloudWatchLogsWriter
{
    private Log logger = LogFactory.getLog(getClass());

    private AWSLogs client;
    private String logGroupName;
    private String logStreamName;
    private ScheduledExecutorService executor;
    private long interval;

    private boolean logBatches;

    private LinkedBlockingDeque<QueuedMessage> unsentMessages = new LinkedBlockingDeque<QueuedMessage>();
    private volatile boolean isShutdown = false;

    private AtomicInteger flushCount = new AtomicInteger(0);
    private AtomicInteger batchCount = new AtomicInteger(0);
    private AtomicInteger invalidSequenceCount = new AtomicInteger(0);
    private AtomicLong totalMessagesSent = new AtomicLong();
    private AtomicLong totalMessagesRejected = new AtomicLong();
    private AtomicLong lastFlushTimestamp = new AtomicLong();


    /**
     *  Creates an instance that is called synchronously, under program control.
     *
     *  @param  client          The service client. AWS best practice is to share a single
     *                          client instance between all consumers.
     *  @param  logGroupName    The destination log group. This group will be created by
     *                          {@link #flush} if it does not exist (or has been deleted
     *                          since the last call).
     *  @param  logStreamName   The destination log stream. This stream will be created by
     *                          {@link #flush} if it does not exist (or has been deleted
     *                          since the last call).
     */
    public CloudWatchLogsWriter(AWSLogs client, String logGroupName, String logStreamName)
    {
        this.client = client;
        this.logGroupName = logGroupName;
        this.logStreamName = logStreamName;
    }


    /**
     *  Creates an instance that uses a background task to periodically flush the queue.
     *
     *  @param  client          The service client. AWS best practice is to share a single
     *                          client instance between all consumers.
     *  @param  logGroupName    The destination log group. This group will be created by
     *                          {@link #flush} if it does not exist (or has been deleted
     *                          since the last call).
     *  @param  logStreamName   The destination log stream. This stream will be created by
     *                          {@link #flush} if it does not exist (or has been deleted
     *                          since the last call).
     *  @param  executor        Provides a threadpool for running the background task.
     *  @param  interval        Number of milliseconds between (scheduled invocations. The
     *                          first invocation will be <code>interval</code> milliseconds
     *                          from the time of construction.
     */
    public CloudWatchLogsWriter(AWSLogs client, String logGroupName, String logStreamName, ScheduledExecutorService executor, long interval)
    {
        this.client = client;
        this.logGroupName = logGroupName;
        this.logStreamName = logStreamName;
        this.executor = executor;
        this.interval = interval;

        executor.schedule(new BackgroundTask(), interval, TimeUnit.MILLISECONDS);
    }


//----------------------------------------------------------------------------
//  Post-construction API
//----------------------------------------------------------------------------

    /**
     *  Enables debug-level logging when a batch is written.
     */
    public CloudWatchLogsWriter withBatchLogging(boolean value)
    {
        logBatches = value;
        return this;
    }

//----------------------------------------------------------------------------
//  Public API
//----------------------------------------------------------------------------

    /**
     *  Adds a new message to the queue, with current timestamp.
     *
     *  @throws IllegalArgumentException if the message is too large to fit in
     *          a batch (even by itself).
     *  @throws IllegalStateException if called after {@link #shutdown}.
     */
    public void add(String message)
    {
        add(System.currentTimeMillis(), message);
    }


    /**
     *  Adds a message to the queue, with specified timestamp.
     *
     *  @throws IllegalArgumentException if the message it too large to fit in a
     *          batch (even by itself) or the timestamp does not within the allowed
     *          range. Note that a message may accepted here but rejected during
     *          {@link #flush}, either because of a delay between add and flush or
     *          because the log group has a shorter retention period.
     *  @throws IllegalStateException if called after {@link #shutdown}.
     */
    public void add(long timestamp, String message)
    {
        if (isShutdown)
        {
            throw new IllegalStateException(
                        "writer has shut down"
                        + " (stream: " + logGroupName + "/" + logStreamName + ")");
        }

        long earliestAllowedTimestamp = System.currentTimeMillis() - 14 * 86400000;
        if (timestamp < earliestAllowedTimestamp)
        {
            throw new IllegalArgumentException(
                        "message timestamp too far in past"
                        + " (stream: " + logGroupName + "/" + logStreamName + ")"
                        + ": " + timestamp
                        + "; limit: " + earliestAllowedTimestamp);
        }

        long latestAllowedTimestamp = System.currentTimeMillis() + 2 * 3600000;
        if (timestamp > latestAllowedTimestamp)
        {
            throw new IllegalArgumentException(
                        "message timestamp too far in future"
                        + " (stream: " + logGroupName + "/" + logStreamName + ")"
                        + ": " + timestamp
                        + "; limit: " + latestAllowedTimestamp);
        }

        QueuedMessage qmessage = new QueuedMessage(timestamp, message);

        long sizeLimit = 1024 * 1024 - 26;
        if (qmessage.size > sizeLimit)
        {
            throw new IllegalArgumentException(
                        "message is too large"
                        + " (stream: " + logGroupName + "/" + logStreamName + ")"
                        + ": " + qmessage.size + " bytes"
                        + "; limit: " + sizeLimit);
        }

        unsentMessages.add(qmessage);
    }


    /**
     *  Flush outstanding messages. Will build and send batches until the queue
     *  is empty or an uncrecoverable error occurs.
     */
    public synchronized void flush()
    {
        flushCount.incrementAndGet();
        lastFlushTimestamp.set(System.currentTimeMillis());

        LinkedList<QueuedMessage> messages = extractAndSortMessages();
        if (messages.isEmpty())
            return;

        try
        {
            String uploadSequenceToken = retrieveUploadSequenceToken();
            while (messages.size() > 0)
            {
                List<QueuedMessage> batch = extractBatch(messages);
                try
                {
                    uploadSequenceToken = attemptToSend(batch, uploadSequenceToken);
                    batch.clear();
                }
                catch (DataAlreadyAcceptedException ex)
                {
                    // in normal operation this shouldn't happen, so it's worth a warning
                    logger.warn("DataAlreadyAcceptedException: discarded " + batch.size()
                                + " to stream: " + logGroupName + "/" + logStreamName);
                    batch.clear();
                }
                catch (InvalidSequenceTokenException ex)
                {
                    invalidSequenceCount.incrementAndGet();
                    uploadSequenceToken = retrieveUploadSequenceToken();
                }
                finally
                {
                    // the batch will be empty unless there was an exception
                    messages.addAll(0, batch);
                }
            }
        }
        catch (Exception ex)
        {
            // this catches unexpected exceptions, which are allowed to break out of the writer loop
            // it's worth logging, but I don't think a long stack trace will help anyone
            logger.warn(ex.getClass().getSimpleName() + " writing to stream: "
                        + logGroupName + "/" + logStreamName);
        }
        finally
        {
            unsentMessages.addAll(messages);
        }
    }


    /**
     *  Shuts down the writer: no more messages will be accepted, any current
     *  messages are flushed immediately, and not further tasks will be
     *  submitted to the executor (if any).
     */
    public void shutdown()
    {
        logger.debug("shutdown called (writing to: " + logGroupName + "/" + logStreamName + ")");
        isShutdown = true;
        flush();
    }

//----------------------------------------------------------------------------
//  Monitoring
//----------------------------------------------------------------------------

    /**
     *  Returns the number of times that {@link #flush} was called.
     */
    public int getFlushCount()
    {
        return flushCount.get();
    }


    /**
     *  Returns the number of batches that were successfully written.
     */
    public int getBatchCount()
    {
        return batchCount.get();
    }


    /**
     *  Returns the number of times a batch was retried due to an invalid
     *  sequence token. This exeption indicates concurrent writes to the
     *  same log stream. It should be close to 0; if not, increase the
     *  amount of time between batchs (if running on a background thread)
     *  or write to different log streams.
     */
    public int getInvalidSequenceCount()
    {
        return invalidSequenceCount.get();
    }


    /**
     *  Returns the total number of messages that were sent to the stream over the
     *  lifetime of the writer.
     */
    public long getTotalMessagesSent()
    {
        return totalMessagesSent.get();
    }


    /**
     *  Returns the number of messages that were sent to the stream but rejected for
     *  being outside the allowed time range.
     */
    public long getTotalMessagesRejected()
    {
        return totalMessagesRejected.get();
    }


    /**
     *  Returns the timestamp when {@link #flush} was last called, whether by
     *  application or background thread. Will be 0 if never called.
     */
    public long getLastFlushTime()
    {
        return lastFlushTimestamp.get();
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  Retrieves the current upload sequence token for the stream. If the
     *  stream does not exist, will attempt to re-create it.
     */
    private String retrieveUploadSequenceToken()
    {
        LogStream stream = CloudWatchLogsUtil.describeLogStream(client, logGroupName, logStreamName);
        if (stream == null)
        {
            logger.debug("stream " + logGroupName + "/" + logStreamName + " does not exist, creating");
            stream = CloudWatchLogsUtil.createLogStream(client, logGroupName, logStreamName, 30000);
            if (stream == null)
            {
                logger.warn("stream " + logGroupName + "/" + logStreamName + " not ready after creation");
                return null;
            }
        }

        return stream.getUploadSequenceToken();
    }


    /**
     *  Pulls all messages out of the queue, sorts them, and converts them
     *  to <code>InputLogEvent</code>s.
     */
    private LinkedList<QueuedMessage> extractAndSortMessages()
    {
        LinkedList<QueuedMessage> messages = new LinkedList<QueuedMessage>();
        for (Iterator<QueuedMessage> unsentItx = unsentMessages.iterator() ; unsentItx.hasNext() ; )
        {
            messages.add(unsentItx.next());
            unsentItx.remove();
        }

        Collections.sort(messages, new MessageComparator());
        return messages;
    }


    /**
     *  Given a list of sorted messages, builds a batch from the oldest messages
     *  (removing them from the list).
     */
    private List<QueuedMessage> extractBatch(LinkedList<QueuedMessage> sortedMessages)
    {
        LinkedList<QueuedMessage> batch = new LinkedList<QueuedMessage>();
        if (sortedMessages.isEmpty())
            return batch;

        long lastAllowedTimestamp = sortedMessages.getFirst().timestamp + 86399999;
        int batchMessages = 0;
        int batchBytes = 0;

        for (Iterator<QueuedMessage> srcItx = sortedMessages.iterator() ; srcItx.hasNext() ; )
        {
            QueuedMessage message = srcItx.next();
            batchMessages++;
            batchBytes += message.size + 26;

            if ((batchMessages > 10000) || (batchBytes > 1048576) || (message.timestamp > lastAllowedTimestamp))
                return batch;

            batch.add(message);
            srcItx.remove();
        }

        return batch;
    }


    /**
     *  Attempts to send a batch of messages to the service. If successful, returns
     *  the sequence token for the next batch. Allows any exceptions to propagate.
     */
    private String attemptToSend(List<QueuedMessage> batch, String uploadSequenceToken)
    {
        if (logBatches && logger.isDebugEnabled())
        {
            logger.debug("sending batch of " + batch.size() + " events to " + logGroupName + " / " + logStreamName);
        }

        List<InputLogEvent> events = new ArrayList<InputLogEvent>(batch.size());
        for (QueuedMessage message : batch)
        {
            events.add(new InputLogEvent()
                      .withTimestamp(message.timestamp)
                      .withMessage(message.message));
        }

        PutLogEventsRequest request = new PutLogEventsRequest(logGroupName, logStreamName, events)
                                      .withSequenceToken(uploadSequenceToken);
        PutLogEventsResult response = client.putLogEvents(request);

        int messagesRejected = 0;
        RejectedLogEventsInfo rejectInfo = response.getRejectedLogEventsInfo();
        if (rejectInfo != null)
        {
            int expired = rejectInfo.getExpiredLogEventEndIndex() != null
                        ? rejectInfo.getExpiredLogEventEndIndex().intValue() + 1
                        : 0;
            int tooOld  = rejectInfo.getTooOldLogEventEndIndex() != null
                        ? rejectInfo.getTooOldLogEventEndIndex().intValue() + 1
                        : 0;
            int tooNew  = rejectInfo.getTooNewLogEventStartIndex() != null
                        ? batch.size() - rejectInfo.getTooNewLogEventStartIndex().intValue()
                        : 0;
            messagesRejected = Math.max(expired, tooOld) + tooNew;
            logger.warn("rejected " + messagesRejected
                        + " messages on stream " + logGroupName + " / " + logStreamName + ": "
                        + Math.max(expired, tooOld) + " expired/too old, " + tooNew + " too new");
        }
        batchCount.incrementAndGet();
        totalMessagesSent.addAndGet(batch.size());
        totalMessagesRejected.addAndGet(messagesRejected);

        return response.getNextSequenceToken();
    }


    /**
     *  Holder for a message. We retain both the source string and the UTF-8
     *  representaton of that string for debugging. All fields are public
     *  because we only use it internally (if you want to create a subclass
     *  and muck with it, feel free).
     */
    protected static class QueuedMessage
    {
        public long timestamp;
        public String message;
        public int size;

        public QueuedMessage(long timestamp, String message)
        {
            this.timestamp = timestamp;
            this.message = message;
            try
            {
                this.size = message.getBytes("UTF-8").length;
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException("this JVM does not support UTF-8 (should never happen)");
            }
        }
    }


    /**
     *  Used to sort messages: CloudWatch Logs will only accept messages that
     *  are ordered by timestamp.
     */
    protected static class MessageComparator
    implements Comparator<QueuedMessage>
    {
        @Override
        public int compare(QueuedMessage m1, QueuedMessage m2)
        {
            return (m1.timestamp < m2.timestamp) ? -1
                 : (m1.timestamp > m2.timestamp) ? 1
                 : 0;
        }
    }


    /**
     *  Periodically calls flush() from a background thread.
     */
    private class BackgroundTask
    implements Runnable
    {
        @Override
        public void run()
        {
            flush();
            if (! isShutdown)
            {
                executor.schedule(this, interval, TimeUnit.MILLISECONDS);
            }
        }
    }

}
