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

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;


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
 *  operate independenty.
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
    private boolean isForward;
    private Date startingAt;
    private int maxRetries;
    private long retryDelay;


    /**
     *  Base constructor.
     *
     *  @param  client      The AWS client used to perform retrieves.
     *  @param  groupName   The name of a log group.
     *  @param  streamName  The name of a log stream within that group.
     *  @param  isForward   If <code>true</code>, the iterables move forward in time; if
     *                      <code>false</code>, the iterables move backwards.
     *  @param  startingAt  The timestamp to start iterating from. If null, iteration
     *                      starts at the earliest/latest event depending on iterator
     *                      direction.
     *  @param  maxRetries  The maximum number of times that a request will be retried
     *                      if rejected due to throttling. Once this limit is exceeded,
     *                      the exception is propagated.
     *  @param  retryDelay  The base delay, in milliseconds, between retries. Each retry
     *                      will be double the length of the previous (resetting on success).
     */
    public LogStreamIterable(AWSLogs client, String groupName, String streamName, boolean isForward, Date startingAt, int maxRetries, long retryDelay)
    {
        this.client = client;
        this.groupName = groupName;
        this.streamName = streamName;
        this.isForward = isForward;
        this.startingAt = startingAt;
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
    }


    /**
     *  Convenience constructor: forward or backward iteration from beginning/end of stream,
     *  with default retry configuration.
     *
     *  @param  client      The AWS client used to perform retrieves.
     *  @param  groupName   The name of a log group.
     *  @param  streamName  The name of a log stream within that group.
     *  @param  isForward   If <code>true</code>, the iterables move forward in time; if
     *                      <code>false</code>, the iterables move backwards.
     *
     */
    public LogStreamIterable(AWSLogs client, String groupName, String streamName, boolean isForward)
    {
        this(client, groupName, streamName, isForward, null, DEFAULT_RETRIES, DEFAULT_RETRY_DELAY);
    }


    /**
     *  Convenience constructor: forward iteration from a given point, with
     *  default retry configuration.
     *
     *  @param  client      The AWS client used to perform retrieves.
     *  @param  groupName   The name of a log group.
     *  @param  streamName  The name of a log stream within that group.
     *  @param  startingAt  The timestamp to start iterating from.
     */
    public LogStreamIterable(AWSLogs client, String groupName, String streamName, Date startingAt)
    {
        this(client, groupName, streamName, true, startingAt, DEFAULT_RETRIES, DEFAULT_RETRY_DELAY);
    }


    /**
     *  Convenience constructor: forward iteration from beginning of stream, with
     *  default retry configuration.
     *
     *  @param  client      The AWS client used to perform retrieves.
     *  @param  groupName   The name of a log group.
     *  @param  streamName  The name of a log stream within that group.
     */
    public LogStreamIterable(AWSLogs client, String groupName, String streamName)
    {
        this(client, groupName, streamName, true, null, DEFAULT_RETRIES, DEFAULT_RETRY_DELAY);
    }


    @Override
    public Iterator<OutputLogEvent> iterator()
    {
        return new LogStreamIterator();
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

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

            if ((startingAt != null) && ! isForward)
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
            try
            {
                GetLogEventsResult response = client.getLogEvents(request);
                String nextToken = isForward ? response.getNextForwardToken() : response.getNextBackwardToken();
                atEndOfStream = nextToken.equals(request.getNextToken());
                request.setNextToken(nextToken);
                return response.getEvents();
            }
            catch (ResourceNotFoundException ex)
            {
                logger.warn("retrieve from missing stream: " + groupName + " / " + streamName);
                return Collections.emptyList();
            }
        }
    }
}
