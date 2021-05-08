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
    private List<StreamIdentifier> streamIdentifiers = new ArrayList<StreamIdentifier>();

    private Long startTime;
    private Long endTime;


    /**
     *  Creates an instance that reads from one or more named streams, which may
     *  belong to different groups. You should rarely want to do this, as there
     *  is no way to differentiate messages from different groups, but it may be
     *  useful to correlate messages logged from multiple sources.
     *
     *  @param  client          The service client. AWS best practice is to share a single
     *                          client instance between all consumers.
     *  @param  logStreams      The streams to read.
     */
    public CloudWatchLogsReader(AWSLogs client, StreamIdentifier... logStreams)
    {
        this.client = client;
        this.streamIdentifiers.addAll(Arrays.asList(logStreams));
    }


    /**
     *  Creates an instance that reads from one or more named streams in a single group.
     *
     *  @param  client          The service client. AWS best practice is to share a single
     *                          client instance between all consumers.
     *  @param  logGroupName    The source log group.
     *  @param  logStreamNames  The source log streams.
     */
    public CloudWatchLogsReader(AWSLogs client, String logGroupName, String... logStreamNames)
    {
        this(client, StreamIdentifier.fromStreamNames(logGroupName, logStreamNames));
    }


    /**
     *  Creates an instance that reads from one or more named streams in a single group,
     *  where the stream names are extracted from stream descriptions. This is intended
     *  for use with the output of {@link CloudWatchLogsUtil#describeStreams}.
     *
     *  @param  client          The service client. AWS best practice is to share a single
     *                          client instance between all consumers.
     *  @param  logGroupName    The source log group.
     *  @param  logStreams      The source log streams.
     */
    public CloudWatchLogsReader(AWSLogs client, String logGroupName, List<LogStream> logStreams)
    {

        this(client, StreamIdentifier.fromDescriptions(logGroupName, logStreams));
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
     *  Holds a stream name, along with its group name. This is used internally,
     *  and is exposed so that callers can read from independent streams.
     */
    public static class StreamIdentifier
    {
        private String groupName;
        private String streamName;

        public StreamIdentifier(String logGroupName, String logStreamName)
        {
            this.groupName = logGroupName;
            this.streamName = logStreamName;
        }

        // since this class is exposed to the public, it's immutable with getters

        public String getGroupName()
        {
            return groupName;
        }

        public String getStreamName()
        {
            return streamName;
        }

        @Override
        public String toString()
        {
            return groupName + " / " + streamName;
        }

        // the following are used by the constructors

        public static StreamIdentifier[] fromStreamNames(String logGroupName, String... logStreamNames)
        {
            StreamIdentifier[] result = new StreamIdentifier[logStreamNames.length];
            for (int ii = 0 ; ii < logStreamNames.length ; ii++)
            {
                result[ii] = new StreamIdentifier(logGroupName, logStreamNames[ii]);
            }
            return result;
        }

        public static StreamIdentifier[] fromDescriptions(String logGroupName, List<LogStream> logStreams)
        {
            StreamIdentifier[] result = new StreamIdentifier[logStreams.size()];
            int ii = 0;
            for (LogStream stream : logStreams)
            {
                result[ii++] = new StreamIdentifier(logGroupName, stream.getLogStreamName());
            }
            return result;
        }
    }


    /**
     *  Retrieves messages from the streams. All messages are combined into a
     *  single list, and are ordered with the earliest message first.
     *  <p>
     *  This is a "best effort" read: it starts reading at the configured start
     *  point, and continues to request records until the sequence token does
     *  not change between requests. This approach may miss records due to
     *  eventual consistency; it will definitely omit records that were written
     *  with older timestamps than those that are already read.
     *  <p>
     *  If the log group or log stream does not exist, it is ignored and the
     *  result will be an empty list.
     */
    public List<OutputLogEvent> retrieve()
    {
        List<OutputLogEvent> result = new ArrayList<OutputLogEvent>();

        for (StreamIdentifier streamIdentifier : streamIdentifiers)
        {
            result.addAll(readFromStream(streamIdentifier));
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
     *  Reads messages from a single stream. Returns an empty list if the stream
     *  does not exist.
     */
    private List<OutputLogEvent> readFromStream(StreamIdentifier streamIdentifier)
    {
        GetLogEventsRequest request = new GetLogEventsRequest()
                                      .withLogGroupName(streamIdentifier.groupName)
                                      .withLogStreamName(streamIdentifier.streamName)
                                      .withStartFromHead(Boolean.TRUE);
        if (startTime != null)
            request.setStartTime(startTime);

        if (endTime != null)
            request.setEndTime(endTime);

        List<OutputLogEvent> result = new ArrayList<OutputLogEvent>();
        String prevToken = "";
        String nextToken = "";
        do
        {
            try
            {
                GetLogEventsResult response = client.getLogEvents(request);
                result.addAll(response.getEvents());
                prevToken = nextToken;
                nextToken = response.getNextForwardToken();
                request.setNextToken(nextToken);
            }
            catch (ResourceNotFoundException ex)
            {
                logger.warn("retrieve from missing stream: " + streamIdentifier);
                return result;
            }
        }
        while (! prevToken.equals(nextToken));

        return result;
    }


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
