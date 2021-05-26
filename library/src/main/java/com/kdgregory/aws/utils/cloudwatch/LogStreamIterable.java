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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

import com.kdgregory.aws.utils.CommonUtils;


/**
 *  Iterates records from a log stream, handling pagination and throttling. Can iterate
 *  forward or backward, from the start/end of the stream or a provided date. Repeatedly
 *  calls <code>GetLogEvents</code> until it indicates that the end of the stream has
 *  been reached.
 *  <p>
 *  Note: events may be added to the stream after a forward iterator has reached the
 *  "end". Calling <code>hasNext()</code> will return <code>true</code> in this case,
 *  even if it previously returned <code>false</code>.
 *  <p>
 *  Multiple iterators may be created by one instance of this class. These iterators
 *  operate independently.
 *  <p>
 *  This class is thread-safe. Iterators produced from it are not.
 */
public class LogStreamIterable
implements Iterable<OutputLogEvent>
{
    private final static int DEFAULT_RETRIES = 5;
    private final static long DEFAULT_RETRY_DELAY = 100;

    private Log logger = LogFactory.getLog(getClass());

    private AWSLogs client;
    private String groupName;
    private String streamName;
    private Date startingAt;
    private boolean isForward = true;
    private int maxRetries = DEFAULT_RETRIES;
    private long retryDelay = DEFAULT_RETRY_DELAY;


    /**
     *  Constructs an instance with default behavior: forward iteration starting at the
     *  head of the stream.
     *
     *  @param  client      The AWS client used to perform retrieves.
     *  @param  groupName   The name of a log group.
     *  @param  streamName  The name of a log stream within that group.
     */
    public LogStreamIterable(AWSLogs client, String groupName, String streamName)
    {
        this.client = client;
        this.groupName = groupName;
        this.streamName = streamName;
    }

//----------------------------------------------------------------------------
//  Configuration
//----------------------------------------------------------------------------

    public enum IterationDirection { FORWARD, BACKWARD }

    /**
     *  Sets the direction of iteration.
     */
    public LogStreamIterable withIterationDirection(IterationDirection direction)
    {
        switch (direction)
        {
            case FORWARD:
                this.isForward = true;
                break;
            case BACKWARD:
                this.isForward = false;
                break;
            default:
                throw new IllegalArgumentException("invalid direction: " + direction);
        }
        return this;
    }


    /**
     *  Sets the initial timestamp for iteration. Forward iteration will return events
     *  on or after that timestamp, backward iteration will return events before the
     *  timestamp (excluding any events at the timestamp).
     */
    public LogStreamIterable withStartingAt(Date value)
    {
        this.startingAt = value;
        return this;
    }


    /**
     *  Sets the retry configuration: the number of times that a request will be retried
     *  when throttled, and the base delay between retries.
     */
    public LogStreamIterable withRetryConfiguration(int numRetries, long baseDelay)
    {
        this.maxRetries = numRetries;
        this.retryDelay = baseDelay;
        return this;
    }

//----------------------------------------------------------------------------
//  Public Methods
//----------------------------------------------------------------------------

    @Override
    public Iterator<OutputLogEvent> iterator()
    {
        return new LogStreamIterator();
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  Implements an iterator over the records in a log stream.
     *  <p>
     *  Implementation notes (aka, things that aren't in the GetLogEvents docs):
     *  <ul>
     *  <li> <code>startFromHead</code> must be set, otherwise you just get the
     *       last block of messages.
     *  <li> Returned events are ordered by increasing timestmap, even if you're
     *       iterating backwards.
     *  <li> <code>startTime</code> and <code>endTime</code> are independent of
     *       the order of iteration.
     *  </ul>
     */
    private class LogStreamIterator
    implements Iterator<OutputLogEvent>
    {
        private GetLogEventsRequest request;
        private Iterator<OutputLogEvent> curItx = Collections.<OutputLogEvent>emptyList().iterator();
        private boolean atEndOfStream = false;

        public LogStreamIterator()
        {
            request = new GetLogEventsRequest()
                      .withLogGroupName(groupName)
                      .withLogStreamName(streamName)
                      .withStartFromHead(isForward);

            if ((startingAt != null) && isForward)
            {
                request.setStartTime(startingAt.getTime());
            }

            if ((startingAt != null) && !isForward)
            {
                request.setEndTime(startingAt.getTime());
            }
        }

        @Override
        public boolean hasNext()
        {
            while (!curItx.hasNext() && !atEndOfStream)
            {
                curItx = doRead().iterator();
            }

            return curItx.hasNext();
        }

        @Override
        public OutputLogEvent next()
        {
            if (hasNext())
            {
                return curItx.next();
            }

            throw new NoSuchElementException("at end of stream");
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("CloudWatch Logs events cannot be deleted");
        }


        private List<OutputLogEvent> doRead()
        {
            AWSLogsException preserved = null;
            long currentDelay = retryDelay;
            for (int ii = 0 ; ii < maxRetries ; ii++)
            {
                try
                {
                    GetLogEventsResult response = client.getLogEvents(request);
                    String nextToken = isForward ? response.getNextForwardToken() : response.getNextBackwardToken();
                    atEndOfStream = nextToken.equals(request.getNextToken());
                    request.setNextToken(nextToken);
                    ArrayList<OutputLogEvent> result = new ArrayList<>(response.getEvents());
                    Collections.sort(result, new OutputLogEventComparator(isForward));
                    return result;
                }
                catch (ResourceNotFoundException ex)
                {
                    logger.warn("retrieve from missing stream: " + groupName + " / " + streamName);
                    return Collections.emptyList();
                }
                catch (AWSLogsException ex)
                {
                    if ("ThrottlingException".equals(ex.getErrorCode()))
                    {
                        logger.debug("getLogEvents() throttled; curent delay " + currentDelay + " ms");
                        preserved = (AWSLogsException)ex;
                        CommonUtils.sleepQuietly(currentDelay);
                        currentDelay *= 2;
                    }
                    else
                    {
                        throw ex;
                    }
                }
            }
            throw preserved;
        }
    }


    /**
     *  Compares two events based on their timestamp. According to the docs, timestamp
     *  is not required, so nulls are replaced with 0.
     */
    private static class OutputLogEventComparator
    implements Comparator<OutputLogEvent>
    {
        private boolean isForward;

        public OutputLogEventComparator(boolean isForward)
        {
            this.isForward = isForward;
        }

        @Override
        public int compare(OutputLogEvent e1, OutputLogEvent e2)
        {
            // per docs, events do not need to have a timestamp
            Long ts1 = (e1.getTimestamp() != null) ? e1.getTimestamp() : Long.valueOf(0);
            Long ts2 = (e2.getTimestamp() != null) ? e2.getTimestamp() : Long.valueOf(0);
            int cmp = ts1.compareTo(ts2);
            return isForward ? cmp : -cmp;
        }
    }
}
