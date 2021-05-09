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

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import net.sf.kdgcommons.lang.StringUtil;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;
import com.sun.webkit.PageCache;

/**
 *  A mock client that knows about a predefined set of groups and streams and
 *  can provide messages.
 */
public class MockAWSLogs
extends AbstractMock<AWSLogs>
{
    // public variables will be inspected by tests
    // protected variables may be changed by subclasses but are not inspected by tests;
    // private variables are for internal state

    private int pageSize = Integer.MAX_VALUE / 2; // same size for all APIs; default is effectively infinite

    protected String uploadSequenceToken = UUID.randomUUID().toString();

    public List<InputLogEvent> allMessages = new ArrayList<InputLogEvent>();

    private Map<String,TreeSet<String>> groupsAndStreams = new TreeMap<String,TreeSet<String>>();

    // TODO - change to a map keyed by group and stream
    private List<OutputLogEvent> retrievableEvents = new ArrayList<>();

    /**
     *  Basic constructor: must call one or more of the configuration methods
     *  for this to be useful.
     */
    public MockAWSLogs()
    {
        super(AWSLogs.class);
    }


    /**
     *  Convenience constructor, for a single log group and stream.
     */
    public MockAWSLogs(String knownLogGroupName, String knownLogStreamName)
    {
        this();
        addStream(knownLogGroupName, knownLogStreamName);
    }

//----------------------------------------------------------------------------
//  Optional configuration
//----------------------------------------------------------------------------

    /**
     *  Adds a group and its list of streams (which may be empty).
     */
    public MockAWSLogs withGroupAndStreams(String groupName, String... streamNames)
    {
        addGroup(groupName);
        for (String streamName : streamNames)
        {
            addStream(groupName, streamName);
        }
        return this;
    }


    /**
     *  Adds a message to the list that are returned. Each message must have a
     *  unique timestamp. There is no differentiation of messages by group or
     *  stream.
     */
    public MockAWSLogs withMessage(long timestamp, String message)
    {
        OutputLogEvent event = new OutputLogEvent().withTimestamp(timestamp).withMessage(message);
        retrievableEvents.add(event);
        return this;
    }



    /**
     *  Sets the page size for paginated operations.
     */
    public MockAWSLogs withPageSize(int value)
    {
        this.pageSize = value;
        return this;
    }

//----------------------------------------------------------------------------
//  Invocation accessors
//----------------------------------------------------------------------------

    public PutLogEventsRequest getLastPutRequest()
    {
        return getMostRecentInvocationArg("putLogEvents", 0, PutLogEventsRequest.class);
    }


    public List<InputLogEvent> getLastPutEvents()
    {
        return getLastPutRequest().getLogEvents();
    }

//----------------------------------------------------------------------------
//  Internals
//---------------------------------------------------------------------------

    protected void addGroup(String groupName)
    {
        if (groupsAndStreams.containsKey(groupName))
            return;

        groupsAndStreams.put(groupName, new TreeSet<String>());
    }


    protected void addStream(String groupName, String streamName)
    {
        addGroup(groupName);
        groupsAndStreams.get(groupName).add(streamName);
    }


    protected void verifyGroup(String groupName)
    {
        if (! groupsAndStreams.containsKey(groupName))
            throw new ResourceNotFoundException("missing log group: " + groupName);
    }


    protected void verifyStream(String groupName, String streamName)
    {
        verifyGroup(groupName);
        if (! groupsAndStreams.get(groupName).contains(streamName))
            throw new ResourceNotFoundException("missing log stream: " + streamName);
    }

//----------------------------------------------------------------------------
//  Mock implementations
//----------------------------------------------------------------------------

    public CreateLogGroupResult createLogGroup(CreateLogGroupRequest request)
    {
        String groupName = request.getLogGroupName();
        if (groupsAndStreams.containsKey(groupName))
            throw new ResourceAlreadyExistsException("resource already exists: " + groupName);

        addGroup(groupName);
        return new CreateLogGroupResult();
    }


    public CreateLogStreamResult createLogStream(CreateLogStreamRequest request)
    {
        String groupName = request.getLogGroupName();
        verifyGroup(groupName);

        String streamName = request.getLogStreamName();
        if (groupsAndStreams.get(groupName).contains(streamName))
            throw new ResourceAlreadyExistsException("stream already exists: " + streamName);

        addStream(groupName, streamName);
        return new CreateLogStreamResult();
    }


    public DeleteLogGroupResult deleteLogGroup(DeleteLogGroupRequest request)
    {
        String groupName = request.getLogGroupName();
        verifyGroup(groupName);

        groupsAndStreams.remove(groupName);
        return new DeleteLogGroupResult();
    }


    public DeleteLogStreamResult deleteLogStream(DeleteLogStreamRequest request)
    {
        String groupName = request.getLogGroupName();
        String streamName = request.getLogStreamName();
        verifyStream(groupName, streamName);

        groupsAndStreams.get(groupName).remove(streamName);
        return new DeleteLogStreamResult();
    }


    public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
    {
        List<String> groupNames = new ArrayList<String>(groupsAndStreams.keySet());

        int startOffset = StringUtil.isEmpty(request.getNextToken())
                        ? 0
                        : Integer.parseInt(request.getNextToken());
        int endOffset = Math.min(groupNames.size(), startOffset + pageSize);
        String nextToken = endOffset == groupNames.size()
                         ? null
                         : String.valueOf(endOffset);

        List<LogGroup> groups = new ArrayList<LogGroup>();
        for (String name : groupNames.subList(startOffset, endOffset))
        {
            boolean include = StringUtil.isEmpty(request.getLogGroupNamePrefix())
                           || name.startsWith(request.getLogGroupNamePrefix());
            if (include)
                groups.add(new LogGroup().withLogGroupName(name));
        }

        return new DescribeLogGroupsResult().withLogGroups(groups).withNextToken(nextToken);
    }


    public DescribeLogStreamsResult describeLogStreams(DescribeLogStreamsRequest request)
    {
        String groupName = request.getLogGroupName();
        verifyGroup(groupName);

        List<String> streamNames = new ArrayList<String>(groupsAndStreams.get(groupName));

        int startOffset = StringUtil.isEmpty(request.getNextToken())
                        ? 0
                        : Integer.parseInt(request.getNextToken());
        int endOffset = Math.min(streamNames.size(), startOffset + pageSize);
        String nextToken = endOffset == streamNames.size()
                         ? null
                         : String.valueOf(endOffset);

        List<LogStream> streams = new ArrayList<LogStream>();
        for (String name : streamNames.subList(startOffset, endOffset))
        {
            boolean include = StringUtil.isEmpty(request.getLogStreamNamePrefix())
                           || name.startsWith(request.getLogStreamNamePrefix());
            if (include)
            {
                LogStream stream = new LogStream().withLogStreamName(name)
                                   .withUploadSequenceToken(uploadSequenceToken);
                streams.add(stream);
            }
        }

        return new DescribeLogStreamsResult().withLogStreams(streams).withNextToken(nextToken);
    }


    public PutLogEventsResult putLogEvents(PutLogEventsRequest request)
    {
        verifyStream(request.getLogGroupName(), request.getLogStreamName());

        if (! uploadSequenceToken.equals(request.getSequenceToken()))
        {
            throw new InvalidSequenceTokenException("received " + request.getSequenceToken() + ", expected " + uploadSequenceToken);
        }

        allMessages.addAll(request.getLogEvents());

        uploadSequenceToken = UUID.randomUUID().toString();
        return new PutLogEventsResult().withNextSequenceToken(uploadSequenceToken);
    }


    public GetLogEventsResult getLogEvents(GetLogEventsRequest request)
    {
        verifyStream(request.getLogGroupName(), request.getLogStreamName());

        boolean isForward = (request.isStartFromHead() != null) && request.isStartFromHead().booleanValue();

        long minTimestamp   = (request.getStartTime() == null)
                            ? -1
                            : request.getStartTime().longValue();
        long maxTimestamp   = (request.getEndTime() == null)
                            ? Long.MAX_VALUE
                            : request.getEndTime().longValue();
        long startTimestamp = (request.getNextToken() != null)
                            ? Long.parseLong(request.getNextToken())
                            : isForward ? minTimestamp : maxTimestamp;
        long curTimestamp = startTimestamp;

        ListIterator<OutputLogEvent> itx = retrievableEvents.listIterator();
        while (itx.hasNext())
        {
            curTimestamp = itx.next().getTimestamp().longValue();
            if (curTimestamp >= startTimestamp)
            {
                // for forward iteration, we've already eaten the element we want to start with
                // for backward iteration we're leaving an invalid element in the iterator
                itx.previous();
                break;
            }
        }

        List<OutputLogEvent> events = new ArrayList<OutputLogEvent>();
        if (isForward)
        {
            for (int count = pageSize ; (count > 0) && itx.hasNext() ; count--)
            {
                OutputLogEvent event = itx.next();
                events.add(event);
                curTimestamp = event.getTimestamp().longValue();
            }
        }
        else
        {
            for (int count = pageSize ; (count > 0) && itx.hasPrevious() ; count--)
            {
                OutputLogEvent event = itx.previous();
                events.add(event);
                curTimestamp = event.getTimestamp().longValue();
            }
        }

        String nextForwardToken = String.valueOf(maxTimestamp);
        if (itx.hasNext())
        {
            nextForwardToken = String.valueOf(itx.next().getTimestamp());
            // move iterator back to "pre-if" state so we can check backward token
            itx.previous();
        }

        String nextBackwardToken = String.valueOf(minTimestamp);
        if (itx.hasPrevious())
        {
            nextBackwardToken = String.valueOf(itx.previous().getTimestamp());
        }

        return new GetLogEventsResult()
               .withEvents(events)
               .withNextForwardToken(nextForwardToken)
               .withNextBackwardToken(nextBackwardToken);
    }


    public void shutdown()
    {
        // nothing happening here
    }
}