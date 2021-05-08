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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;


/**
 *  Reads messages from CloudWatch Logs. Handles pagination and combines results
 *  from multiple streams.
 */
public class CloudWatchLogsReader
{
    private Log logger = LogFactory.getLog(getClass());

    private AWSLogs client;
    private List<Iterator<OutputLogEvent>> iterators = new ArrayList<>();

    private Long startTime;
    private Long endTime;


    /**
     *  Base constructor.
     *
     *  @param  client          The service client. AWS best practice is to share a single
     *                          client instance between all consumers.
     *  @param  logGroupName    The source log group.
     *  @param  logStreamNames  The source log streams.
     */
    public CloudWatchLogsReader(AWSLogs client, String logGroupName, List<String> logStreamNames)
    {
        for (String streamName : logStreamNames)
        {
            iterators.add(new LogStreamIterable(client, logGroupName, streamName).iterator());
        }
    }


    /**
     *  Convenience constructor, for an explicitly named list of streams.
     *
     *  @param  client          The service client. AWS best practice is to share a single
     *                          client instance between all consumers.
     *  @param  logGroupName    The source log group.
     *  @param  logStreamNames  The source log streams.
     */
    public CloudWatchLogsReader(AWSLogs client, String logGroupName, String... logStreamNames)
    {
        this(client, logGroupName, Arrays.asList(logStreamNames));
    }
    
//----------------------------------------------------------------------------
//  Configuration API
//----------------------------------------------------------------------------

    /**
     *  Restricts events to a specific time range. You can omit either start or
     *  end, to read from (respectively) the start or end of the stream.
     *
     *  @param  start       Java timestamp (millis since epoch) to start retrieving
     *                      messages, or <code>null</code> to start from first
     *                      message in stream.
     *  @param  finish      Java timestamp (millis since epoch) to stop retrieving
     *                      messages, or <code>null</code> to stop at end of stream.
     */
    public CloudWatchLogsReader withTimeRange(Long start, Long finish)
    {
        this.startTime = start;
        this.endTime = finish;
        return this;
    }

//----------------------------------------------------------------------------
//  Public API
//----------------------------------------------------------------------------

    /**
     *  Retrieves a single batch of messages from the streams. All messages are
     *  combined into a single list, and sorted ascending by timestamp.
     *  <p>
     *  This is a "best effort" read: it reads until each stream iterator is
     *  empty. If messages are added after the read, they won't be retrieved.
     *  <p>
     *  If the log group or log stream does not exist, it is ignored and the
     *  result will be an empty list.
     */
    public List<OutputLogEvent> retrieve()
    {
        List<OutputLogEvent> result = new ArrayList<OutputLogEvent>();

        for (Iterator<OutputLogEvent> itx : iterators)
        {
            while (itx.hasNext())
            {
                result.add(itx.next());
            }
        }

        Collections.sort(result, new OutputLogEventComparator());
        return result;
    }


    /**
     *  Retrieves messages from the streams, retrying until either the expected
     *  number of records have been read or the timeout expires.
     */
    public List<OutputLogEvent> retrieve(int expectedRecordCount, long timeoutInMillis)
    {
        List<OutputLogEvent> result = Collections.emptyList();

        long timeoutAt = System.currentTimeMillis() + timeoutInMillis;
        while (System.currentTimeMillis() < timeoutAt)
        {
            result = retrieve();
            if (result.size() == expectedRecordCount)
                return result;

            try
            {
                Thread.sleep(250);
            }
            catch (InterruptedException ex)
            {
                return result;
            }
        }
        return result;
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  A comparator to sort log events by timestamp, used to combine events from
     *  multiple streams.
     */
    private static class OutputLogEventComparator
    implements Comparator<OutputLogEvent>
    {
        @Override
        public int compare(OutputLogEvent o1, OutputLogEvent o2)
        {
            return o1.getTimestamp().compareTo(o2.getTimestamp());
        }
    }
}
