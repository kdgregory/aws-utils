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

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.Level;

import net.sf.kdgcommons.collections.CollectionUtil;
import static net.sf.kdgcommons.test.NumericAsserts.*;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

import com.kdgregory.aws.utils.testhelpers.Log4JCapturingAppender;
import com.kdgregory.aws.utils.testhelpers.mocks.MockAWSLogs;


public class TestDescribeLogGroupIterable
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
    public void testLogGroupIterableBasicOperation() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs()
                                 .withGroupAndStreams("foo")
                                 .withGroupAndStreams("bar")
                                 .withGroupAndStreams("baz");
        AWSLogs client = mock.getInstance();

        Set<String> groupNames = readLogGroupIterable(new DescribeLogGroupIterable(client));

        assertEquals(CollectionUtil.asSet("foo", "bar", "baz"), groupNames);
        assertEquals("describeLogGroups invocation count", 1, mock.getInvocationCount("describeLogGroups"));

        testLog.assertLogSize(0);
    }


    @Test
    public void testLogGroupIterableWithPrefix() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs()
                                 .withGroupAndStreams("foo")
                                 .withGroupAndStreams("bar")
                                 .withGroupAndStreams("baz");
        AWSLogs client = mock.getInstance();

        Set<String> groupNames = readLogGroupIterable(new DescribeLogGroupIterable(client, "ba"));

        assertEquals(CollectionUtil.asSet("bar", "baz"), groupNames);
        assertEquals("describeLogGroups invocation count", 1, mock.getInvocationCount("describeLogGroups"));

        testLog.assertLogSize(0);
    }


    @Test
    public void testLogGroupIterableWithPagination() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs()
                                 .withGroupAndStreams("foo")
                                 .withGroupAndStreams("bar")
                                 .withGroupAndStreams("baz")
                                 .withPageSize(2);
        AWSLogs client = mock.getInstance();

        Set<String> groupNames = readLogGroupIterable(new DescribeLogGroupIterable(client));

        assertEquals(CollectionUtil.asSet("foo", "bar", "baz"), groupNames);
        assertEquals("describeLogGroups invocation count", 2, mock.getInvocationCount("describeLogGroups"));

        testLog.assertLogSize(0);
    }


    @Test
    public void testLogGroupIterableWithThrottling() throws Exception
    {
        // note: we test throttling and recovery by setting a page size smaller than the
        //       number of known groups
        MockAWSLogs mock = new MockAWSLogs()
        {
            @Override
            public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
            {
                if (getInvocationCount("describeLogGroups") % 2 == 1)
                {
                    // exception contents determined by experimentation
                    AWSLogsException ex = new AWSLogsException("message doesn't matter");
                    ex.setErrorCode("ThrottlingException");
                    throw ex;
                }
                else
                    return super.describeLogGroups(request);
            }
        }
        .withGroupAndStreams("foo")
        .withGroupAndStreams("bar")
        .withGroupAndStreams("baz")
        .withPageSize(2);
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        Set<String> groupNames = readLogGroupIterable(new DescribeLogGroupIterable(client));
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(CollectionUtil.asSet("foo", "bar", "baz"), groupNames);
        assertInRange("execution time", 175, 225, elapsed);
        assertEquals("describeLogGroups invocation count", 4, mock.getInvocationCount("describeLogGroups"));

        testLog.assertLogSize(2);
        testLog.assertLogEntry(0, Level.DEBUG, "describeLogGroups.*throttled.*delay.*100 ms");
        testLog.assertLogEntry(1, Level.DEBUG, "describeLogGroups.*throttled.*delay.*100 ms");
    }


    @Test
    public void testLogGroupIterableWithThrottlingTimeout() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs()
        {
            @Override
            public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
            {
                // exception contents determined by experimentation
                AWSLogsException ex = new AWSLogsException("message doesn't matter");
                ex.setErrorCode("ThrottlingException");
                throw ex;
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        try
        {
            // note: to avoid long-running tests, we explicitly set delay and max retries to small numbers
            readLogGroupIterable(new DescribeLogGroupIterable(client, null, 3, 50L));
            fail("should have thrown");
        }
        catch (AWSLogsException ex)
        {
            long elapsed = System.currentTimeMillis() - start;
            assertInRange("execution time", 300, 400, elapsed);
            assertEquals("describeLogGroups invocation count", 3, mock.getInvocationCount("describeLogGroups"));
            assertEquals("exception error code", "ThrottlingException", ex.getErrorCode());
        }

        testLog.assertLogSize(3);
        testLog.assertLogEntry(0, Level.DEBUG, "describeLogGroups.*throttled.*delay.*50 ms");
        testLog.assertLogEntry(1, Level.DEBUG, "describeLogGroups.*throttled.*delay.*100 ms");
        testLog.assertLogEntry(2, Level.DEBUG, "describeLogGroups.*throttled.*delay.*200 ms");
    }


    @Test
    public void testLogGroupIterableWithPropatedLogsException() throws Exception
    {
        // we don't set the error code, which (1) should cause exception to propagate,
        // and (2) verifies that we do a null-safe check internally
        final AWSLogsException thrownException = new AWSLogsException("nope");

        MockAWSLogs mock = new MockAWSLogs()
        {
            @Override
            public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
            {
                throw thrownException;
            }
        };
        AWSLogs client = mock.getInstance();

        try
        {
            readLogGroupIterable(new DescribeLogGroupIterable(client));
            fail("should have thrown");
        }
        catch (AWSLogsException ex)
        {
            assertEquals("describeLogGroups invocation count", 1, mock.getInvocationCount("describeLogGroups"));
            assertSame("exception was propagated", thrownException, ex);
        }

        testLog.assertLogSize(0);
    }


    @Test
    public void testLogGroupIterableWithRuntimeException() throws Exception
    {
        final RuntimeException thrownException = new RuntimeException("nope");

        MockAWSLogs mock = new MockAWSLogs()
        {
            @Override
            public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
            {
                throw thrownException;
            }
        };
        AWSLogs client = mock.getInstance();

        try
        {
            readLogGroupIterable(new DescribeLogGroupIterable(client));
            fail("should have thrown");
        }
        catch (Exception ex)
        {
            assertEquals("describeLogGroups invocation count", 1, mock.getInvocationCount("describeLogGroups"));
            assertSame("exception was propagated", thrownException, ex);
        }

        testLog.assertLogSize(0);
    }

//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    /**
     *  Reads iterates a LogGroupIterable and collects the names of the groups
     *  it returns.
     */
    private static Set<String> readLogGroupIterable(DescribeLogGroupIterable itx)
    {
        Set<String> result = new HashSet<>();
        for (LogGroup group : itx)
        {
            result.add(group.getLogGroupName());
        }
        return result;
    }
}
