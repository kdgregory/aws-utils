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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

import com.kdgregory.aws.utils.CommonUtils;


/**
 *  Retrieves a listing of log groups, automatically handling pagination and
 *  throttling.
 *
 *  When throttled, will retry with exponential backoff; you can control the
 *  base retry delay and number of retries, or use the default (5 retries,
 *  100ms base delay increasing to 1600ms). Once the retry count is exceeded,
 *  the exception is allowed to propagate. After a successful read, the delay
 *  is reset to its base level.
 *
 *  All other exceptions are allowed to propage. Throttling exceptions are
 *  logged at DEBUG level; all other exceptions are unlogged.
 *
 *  This class is not thread-safe.
 */
public class DescribeLogGroupIterable
implements Iterable<LogGroup>
{
    private final static int DEFAULT_RETRIES = 5;
    private final static long DEFAULT_RETRY_DELAY = 100;

    private Log logger = LogFactory.getLog(getClass());

    private AWSLogs client;
    private String prefix;
    private int maxRetries;
    private long retryDelay;


    /**
     *  Base constructor.
     *
     *  @param  client      The AWS client used to describe groups.
     *  @param  prefix      An optional prefix: only groups that start with this prefix
     *                      will be returned. May be null or empty to return all groups.
     *  @param  maxRetries  The maximum number of times that a request will be retried
     *                      if rejected due to throttling. Once this limit is exceeded, 
     *                      the exception is propagated.
     *  @param  retryDelay  The base delay, in milliseconds, between retries. Each retry
     *                      will be double the length of the previous (resetting on success).
     */
    public DescribeLogGroupIterable(AWSLogs client, String prefix, int maxRetries, long retryDelay)
    {
        this.client = client;
        this.prefix = prefix;
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
    }


    /**
     *  Iterates over log groups with a specified prefix, using default retry behavior.
     *
     *  @param  client      The AWS client used to describe groups.
     *  @param  prefix      An optional prefix: only groups that start with this prefix
     *                      will be returned. May be null or empty to return all groups.
     */
    public DescribeLogGroupIterable(AWSLogs client, String prefix)
    {
        this(client, prefix, DEFAULT_RETRIES, DEFAULT_RETRY_DELAY);
    }


    /**
     *  Iterates over all log groups, using default retry behavior.
     *
     *  @param  client      The AWS client used to describe groups.
     */
    public DescribeLogGroupIterable(AWSLogs client)
    {
        this(client, null);
    }


    @Override
    public Iterator<LogGroup> iterator()
    {
        return new LogGroupIterator();
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  Executes the actual describe, with retry logic.
     */
    private DescribeLogGroupsResult doDescribe(DescribeLogGroupsRequest request)
    {
        AWSLogsException preserved = null;
        long currentDelay = retryDelay;

        for (int ii = 0 ; ii < maxRetries ; ii++)
        {
            try
            {
                return client.describeLogGroups(request);
            }
            catch (AWSLogsException ex)
            {
                preserved = (AWSLogsException)ex;
                if ("ThrottlingException".equals(ex.getErrorCode()))
                {
                    logger.debug("describeLogGroups() throttled; curent delay " + currentDelay + " ms");
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


    private class LogGroupIterator
    implements Iterator<LogGroup>
    {
        private DescribeLogGroupsResult currentBatch;
        private Iterator<LogGroup> currentItx;

        @Override
        public boolean hasNext()
        {
            if ((currentItx != null) && currentItx.hasNext())
                return true;

            if ((currentBatch != null) && ((currentBatch.getNextToken() == null) || currentBatch.getNextToken().isEmpty()))
                return false;

            DescribeLogGroupsRequest request = new DescribeLogGroupsRequest();

            if ((prefix != null) && !prefix.isEmpty())
                request.setLogGroupNamePrefix(prefix);

            if (currentBatch != null)
                request.setNextToken(currentBatch.getNextToken());

            currentBatch = doDescribe(request);
            currentItx = currentBatch.getLogGroups().iterator();

            return currentItx.hasNext();
        }

        @Override
        public LogGroup next()
        {
            if (hasNext())
                return currentItx.next();

            throw new NoSuchElementException();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("can not delete log groups via iterator");
        }
    }
}
