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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.Level;

import static net.sf.kdgcommons.test.NumericAsserts.*;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

import com.kdgregory.aws.utils.cloudwatch.CloudWatchLogsUtil;
import com.kdgregory.aws.utils.testhelpers.Log4JCapturingAppender;
import com.kdgregory.aws.utils.testhelpers.mocks.MockAWSLogs;


public class TestCloudWatchLogsUtil
{
    private Log4JCapturingAppender testLog;

//----------------------------------------------------------------------------
//  Per-test boilerplate
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        testLog = Log4JCapturingAppender.getInstance();
        testLog.reset();
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    // note: this is implemented with LogGroupIterable; detailed testing happens there
    public void testDescribeLogGroups() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs()
                                 .withGroupAndStreams("foo")
                                 .withGroupAndStreams("bar")
                                 .withGroupAndStreams("baz");
        AWSLogs client = mock.getInstance();

        List<LogGroup> groups = CloudWatchLogsUtil.describeLogGroups(client, null);
        assertLogGroupNames(groups, "foo", "bar", "baz");

        testLog.assertLogSize(0);
    }


    @Test
    // note: this is implemented with LogGroupIterable; detailed testing happens there
    public void testDescribeLogStreams() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs()
                                 .withGroupAndStreams("foo", "argle", "bargle", "bazzle")
                                 .withGroupAndStreams("bar");
        AWSLogs client = mock.getInstance();

        List<LogStream> streams = CloudWatchLogsUtil.describeLogStreams(client, "foo", null);
        assertLogStreamNames(streams, "argle", "bargle", "bazzle");

        testLog.assertLogSize(0);
    }


    @Test
    public void testWaitUntilGroupCreated() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs()
        {
            @Override
            public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
            {
                if (getInvocationCount("describeLogGroups") < 5)
                    return new DescribeLogGroupsResult().withLogGroups();
                else
                    return super.describeLogGroups(request);
            }
        }
        .withGroupAndStreams("foo")
        .withGroupAndStreams("bar")
        .withGroupAndStreams("baz");
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogGroup group = CloudWatchLogsUtil.waitUntilCreated(client, "bar", 100, 10);
        long finish = System.currentTimeMillis();

        assertNotNull("found log group", group);
        assertTrue("made multiple describe attempts", mock.getInvocationCount("describeLogGroups") > 1);
        assertInRange("waited between attempts", 40, 100, (finish - start));

        testLog.assertLogSize(0);
    }


    @Test
    public void testWaitUntilGroupCreatedTimeout() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs()
                                 .withGroupAndStreams("foo")
                                 .withGroupAndStreams("bar")
                                 .withGroupAndStreams("baz");
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogGroup group = CloudWatchLogsUtil.waitUntilCreated(client, "biff", 100, 10);
        long finish = System.currentTimeMillis();

        assertNull("did not find log group",            group);
        assertTrue("made multiple describe attempts",               mock.getInvocationCount("describeLogGroups") > 1);
        assertInRange("waited between attempts",        80, 120,    (finish - start));

        testLog.assertLogEntry(0, Level.WARN, "timeout expired.*log group.*biff");
    }


    @Test
    public void testWaitUntilGroupCreatedPassingPrefix() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs()
                                 .withGroupAndStreams("foo")
                                 .withGroupAndStreams("bar")
                                 .withGroupAndStreams("baz");
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogGroup group = CloudWatchLogsUtil.waitUntilCreated(client, "ba", 50, 10);
        long finish = System.currentTimeMillis();

        assertNull("did not find log group",            group);
        assertTrue("made multiple describe attempts",               mock.getInvocationCount("describeLogGroups") > 1);
        assertInRange("waited between attempts",        30, 80,     (finish - start));

        testLog.assertLogEntry(0, Level.WARN, "timeout expired.*log group.*ba");
    }


    @Test
    public void testWaitUntilStreamCreated() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs()
        {
            @Override
            public DescribeLogStreamsResult describeLogStreams(DescribeLogStreamsRequest request)
            {
                if (getInvocationCount("describeLogStreams") < 5)
                    return new DescribeLogStreamsResult().withLogStreams();
                else
                    return super.describeLogStreams(request);
            }
        }
        .withGroupAndStreams("bar", "bargle");
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogStream stream = CloudWatchLogsUtil.waitUntilCreated(client, "bar", "bargle", 100, 10);
        long finish = System.currentTimeMillis();

        assertNotNull("found log stream", stream);
        assertTrue("made multiple describe attempts", mock.getInvocationCount("describeLogStreams") > 1);
        assertInRange("waited between attempts", 40, 100, (finish - start));

        testLog.assertLogSize(0);
    }


    @Test
    public void testWaitUntilStreamCreatedTimeout() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs().withGroupAndStreams("foo");
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogStream stream = CloudWatchLogsUtil.waitUntilCreated(client, "foo", "biff", 100, 10);
        long finish = System.currentTimeMillis();

        assertNull("did not find log stream", stream);
        assertTrue("made multiple describe attempts", mock.getInvocationCount("describeLogStreams") > 1);
        assertInRange("waited between attempts", 80, 120, (finish - start));

        testLog.assertLogEntry(0, Level.WARN, "timeout expired.*log stream.*biff");
    }


    @Test
    public void testWaitUntilStreamCreatedPassingPrefix() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs().withGroupAndStreams("foo");
        AWSLogs client = mock.getInstance();

        LogStream stream = CloudWatchLogsUtil.waitUntilCreated(client, "foo", "ba", 50, 10);

        assertNull("did not find log stream", stream);

        testLog.assertLogEntry(0, Level.WARN, "timeout expired.*log stream.*ba");
    }


    @Test
    public void testCreateLogGroup() throws Exception
    {
        // assumes CloudWatchLogsUtil.RESOURCE_TRANSITION_DESCRIBE_INTERVAL == 50

        MockAWSLogs mock = new MockAWSLogs("foo", "argle")
        {
            @Override
            public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
            {
                if (getInvocationCount("describeLogGroups") < 3)
                    return new DescribeLogGroupsResult().withLogGroups();
                else
                    return super.describeLogGroups(request);
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogGroup group = CloudWatchLogsUtil.createLogGroup(client, "bar", 150);
        long finish = System.currentTimeMillis();

        assertEquals("returned LogGroup object",            "bar",      group.getLogGroupName());
        mock.assertInvocationCount("createLogGroup",        1);
        mock.assertInvocationCount("describeLogGroups",     3);
        assertInRange("waited between describes",           80, 120,    (finish - start));

        testLog.assertLogEntry(0, Level.DEBUG, "creating.*log group.*bar");
    }


    @Test
    public void testCreateLogGroupAlreadyExists() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle");
        AWSLogs client = mock.getInstance();

        LogGroup group = CloudWatchLogsUtil.createLogGroup(client, "foo", 100);

        assertEquals("returned LogGroup object",            "foo",  group.getLogGroupName());
        mock.assertInvocationCount("createLogGroup",        1);
        mock.assertInvocationCount("describeLogGroups",     1);

        testLog.assertLogEntry(0, Level.DEBUG, "creating.*log group.*foo");
    }


    @Test
    public void testCreateLogGroupOperationAborted() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle")
        {
            @Override
            public CreateLogGroupResult createLogGroup(CreateLogGroupRequest request)
            {
                // this call won't succeed, but we'll store the group name so that describe works
                addGroup(request.getLogGroupName());
                throw new OperationAbortedException("");
            }
        };
        AWSLogs client = mock.getInstance();

        LogGroup group = CloudWatchLogsUtil.createLogGroup(client, "bar", 100);

        assertEquals("returned LogGroup object",            "bar",      group.getLogGroupName());
        mock.assertInvocationCount("createLogGroup",        1);
        mock.assertInvocationCount("describeLogGroups",     1);

        testLog.assertLogEntry(0, Level.DEBUG, "creating.*log group.*bar");
    }


    @Test
    public void testCreateLogGroupTimeout() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle")
        {
            @Override
            public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
            {
                // we'll never see the group we just created
                return new DescribeLogGroupsResult().withLogGroups();
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogGroup group = CloudWatchLogsUtil.createLogGroup(client, "bar", 200);
        long finish = System.currentTimeMillis();

        assertNull("did not return anything",                           group);
        mock.assertInvocationCount("createLogGroup",        1);
        assertTrue("made multiple describes",                           mock.getInvocationCount("describeLogGroups") > 1);
        assertInRange("waited between describes",           160, 210,   (finish - start));

        testLog.assertLogEntry(0, Level.DEBUG, "creating.*log group.*bar");
        testLog.assertLogEntry(1, Level.WARN, "timeout expired.*log group.*bar");
    }


    @Test
    public void testCreateLogStream() throws Exception
    {
        // assumes CloudWatchLogsUtil.RESOURCE_TRANSITION_DESCRIBE_INTERVAL == 50

        MockAWSLogs mock = new MockAWSLogs("foo", "argle")
        {
            @Override
            public DescribeLogStreamsResult describeLogStreams(DescribeLogStreamsRequest request)
            {
                if (getInvocationCount("describeLogStreams") < 3)
                    return new DescribeLogStreamsResult().withLogStreams();
                else
                    return super.describeLogStreams(request);
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogStream stream = CloudWatchLogsUtil.createLogStream(client, "foo", "bargle", 150);
        long finish = System.currentTimeMillis();

        assertEquals("returned LogStream object",           "bargle",   stream.getLogStreamName());
        mock.assertInvocationCount("describeLogGroups",     1);
        mock.assertInvocationCount("describeLogStreams",    3);
        mock.assertInvocationCount("createLogGroup",        0);
        mock.assertInvocationCount("createLogStream",       1);
        assertInRange("waited between describes",           80, 120,    (finish - start));

        testLog.assertLogEntry(0, Level.DEBUG, "creating.*log stream.*foo.*bargle");
    }


    @Test
    public void testCreateLogStreamAlreadyExists() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle");
        AWSLogs client = mock.getInstance();

        LogStream stream = CloudWatchLogsUtil.createLogStream(client, "foo", "argle", 100);

        assertEquals("returned LogStream object",           "argle",    stream.getLogStreamName());
        mock.assertInvocationCount("describeLogGroups",     1);
        mock.assertInvocationCount("describeLogStreams",    1);
        mock.assertInvocationCount("createLogGroup",        0);
        mock.assertInvocationCount("createLogStream",       1);

        testLog.assertLogEntry(0, Level.DEBUG, "creating.*log stream.*foo.*argle");
    }


    @Test
    public void testCreateLogStreamTimeout() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle")
        {
            @Override
            public DescribeLogStreamsResult describeLogStreams(DescribeLogStreamsRequest request)
            {
                return new DescribeLogStreamsResult().withLogStreams();
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogStream stream = CloudWatchLogsUtil.createLogStream(client, "foo", "bargle", 200);
        long finish = System.currentTimeMillis();

        assertNull("did not return LogStream object",                   stream);
        mock.assertInvocationCount("describeLogGroups",     1);
        assertTrue("multiple describeLogStreams calls",                 mock.getInvocationCount("describeLogStreams") > 2);
        mock.assertInvocationCount("createLogGroup",        0);
        mock.assertInvocationCount("createLogStream",       1);
        assertInRange("waited between describes",           160, 210,   (finish - start));

        testLog.assertLogEntry(0, Level.DEBUG, "creating.*log stream.*foo.*bargle");
        testLog.assertLogEntry(1, Level.WARN, "timeout expired.*log stream.*bargle");
    }


    @Test
    public void testCreateLogGroupAndStream() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle");
        AWSLogs client = mock.getInstance();

        LogStream stream = CloudWatchLogsUtil.createLogStream(client, "bar", "bargle", 100);

        assertEquals("returned LogStream object",           "bargle",   stream.getLogStreamName());
        mock.assertInvocationCount("describeLogGroups",     2);
        mock.assertInvocationCount("describeLogStreams",    1);
        mock.assertInvocationCount("createLogGroup",        1);
        mock.assertInvocationCount("createLogStream",       1);

        testLog.assertLogEntry(0, Level.DEBUG, "creating.*log stream.*bar.*bargle");
    }


    @Test
    public void testDeleteLogGroup() throws Exception
    {
        // assumes CloudWatchLogsUtil.RESOURCE_TRANSITION_DESCRIBE_INTERVAL == 50

        MockAWSLogs mock = new MockAWSLogs("foo", "argle")
        {
            @Override
            public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
            {
                // delete will have removed known group name so we must simulate it
                if (getInvocationCount("describeLogGroups") < 3)
                {
                    LogGroup group = new LogGroup().withLogGroupName("foo");
                    return new DescribeLogGroupsResult().withLogGroups(group);
                }
                else
                    return new DescribeLogGroupsResult().withLogGroups();
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        assertTrue(CloudWatchLogsUtil.deleteLogGroup(client, "foo", 200));
        long finish = System.currentTimeMillis();

        mock.assertInvocationCount("deleteLogGroup",        1);
        mock.assertInvocationCount("describeLogGroups",     3);
        assertInRange("waited between describes",           80, 120,    (finish - start));

        testLog.assertLogEntry(0, Level.DEBUG, "deleting.*log group.*foo");
    }


    @Test
    public void testDeleteNonExistantLogGroup() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle");
        AWSLogs client = mock.getInstance();

        assertTrue(CloudWatchLogsUtil.deleteLogGroup(client, "bar", 200));

        mock.assertInvocationCount("deleteLogGroup",        1);
        mock.assertInvocationCount("describeLogGroups",     0);

        testLog.assertLogEntry(0, Level.DEBUG, "deleting.*log group.*bar");
    }


    @Test
    public void testDeleteLogGroupOperatiionAborted() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle")
        {
            @Override
            public DeleteLogGroupResult deleteLogGroup(DeleteLogGroupRequest request)
            {
                throw new OperationAbortedException("");
            }
        };
        AWSLogs client = mock.getInstance();

        assertTrue(CloudWatchLogsUtil.deleteLogGroup(client, "bar", 200));

        mock.assertInvocationCount("deleteLogGroup",        1);
        mock.assertInvocationCount("describeLogGroups",     1);

        testLog.assertLogEntry(0, Level.DEBUG, "deleting.*log group.*bar");
    }


    @Test
    public void testDeleteLogGroupTimeout() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle")
        {
            @Override
            public DeleteLogGroupResult deleteLogGroup(DeleteLogGroupRequest request)
            {
                // we're going to make this happen by not actually deleting the group
                return new DeleteLogGroupResult();
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        assertFalse(CloudWatchLogsUtil.deleteLogGroup(client, "foo", 200));
        long finish = System.currentTimeMillis();

        mock.assertInvocationCount("deleteLogGroup",        1);
        mock.assertInvocationCount("describeLogGroups",     4);
        assertInRange("waited between describes",           150, 250,   (finish - start));

        testLog.assertLogEntry(0, Level.DEBUG, "deleting.*log group.*foo");
        testLog.assertLogEntry(1, Level.WARN, "timeout.*foo");
    }


    @Test
    public void testDeleteLogStream() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle")
        {

            @Override
            public DescribeLogStreamsResult describeLogStreams(DescribeLogStreamsRequest request)
            {
                if (getInvocationCount("describeLogStreams") < 3)
                {
                    LogStream stream = new LogStream().withLogStreamName("foo").withLogStreamName("argle");
                    return new DescribeLogStreamsResult().withLogStreams(stream);
                }
                else
                {
                    return new DescribeLogStreamsResult().withLogStreams();
                }
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        assertTrue(CloudWatchLogsUtil.deleteLogStream(client, "foo", "argle", 200));
        long finish = System.currentTimeMillis();

        mock.assertInvocationCount("deleteLogStream",           1);
        mock.assertInvocationCount("describeLogStreams",        3);
        assertInRange("waited between describes",           80, 120,    (finish - start));

        testLog.assertLogEntry(0, Level.DEBUG, "deleting.*log stream.*foo.*argle");
    }


    @Test
    public void testDeleteNonExistantLogStream() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle");
        AWSLogs client = mock.getInstance();

        assertTrue(CloudWatchLogsUtil.deleteLogStream(client, "foo", "bargle", 200));

        mock.assertInvocationCount("deleteLogStream",           1);
        mock.assertInvocationCount("describeLogStreams",        0);

        testLog.assertLogEntry(0, Level.DEBUG, "deleting.*log stream.*foo.*bargle");
    }


    @Test
    public void testDeleteLogStreamOperationAborted() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle")
        {
            @Override
            public DeleteLogStreamResult deleteLogStream(DeleteLogStreamRequest request)
            {
                throw new OperationAbortedException("");
            }
        };
        AWSLogs client = mock.getInstance();

        assertTrue(CloudWatchLogsUtil.deleteLogStream(client, "foo", "bargle", 200));

        mock.assertInvocationCount("deleteLogStream",           1);
        mock.assertInvocationCount("describeLogStreams",        1);

        testLog.assertLogEntry(0, Level.DEBUG, "deleting.*log stream.*foo.*bargle");
    }


    @Test
    public void testDeleteLogStreamTimeout() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "argle")
        {
            @Override
            public DeleteLogStreamResult deleteLogStream(DeleteLogStreamRequest request)
            {
                return new DeleteLogStreamResult();
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        assertFalse(CloudWatchLogsUtil.deleteLogStream(client, "foo", "argle", 200));
        long finish = System.currentTimeMillis();

        mock.assertInvocationCount("deleteLogStream",           1);
        mock.assertInvocationCount("describeLogStreams",        4);
        assertInRange("waited between describes",               150, 250,   (finish - start));

        testLog.assertLogEntry(0, Level.DEBUG, "deleting.*log stream.*foo.*argle");
        testLog.assertLogEntry(1, Level.DEBUG, "timeout.*foo.*argle");
    }

//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    /**
     *  Asserts that a list of log groups contains all of the expected names.
     */
    public static void assertLogGroupNames(List<LogGroup> groups, String... names)
    {
        Set<String> lookup = new TreeSet<String>(Arrays.asList(names));
        for (LogGroup group : groups)
        {
            String name = group.getLogGroupName();
            assertTrue("unexpected log group: " + name, lookup.contains(name));
            lookup.remove(name); // duplicates will fail
        }

        assertTrue("missing log groups: " + lookup, lookup.isEmpty());
    }


    /**
     *  Asserts that a list of log streams contains all of the expected names.
     */
    public static void assertLogStreamNames(List<LogStream> streams, String... names)
    {
        Set<String> lookup = new TreeSet<String>(Arrays.asList(names));
        for (LogStream stream : streams)
        {
            String name = stream.getLogStreamName();
            assertTrue("unexpected log stream: " + name, lookup.contains(name));
            lookup.remove(name); // duplicates will fail
        }

        assertTrue("missing log streams: " + lookup, lookup.isEmpty());
    }
}
