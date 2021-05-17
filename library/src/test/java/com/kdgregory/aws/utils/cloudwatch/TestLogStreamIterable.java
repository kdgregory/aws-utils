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

import static net.sf.kdgcommons.test.NumericAsserts.*;

import java.util.Date;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.log4j.Level;

import com.amazonaws.services.logs.model.AWSLogsException;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;

import com.kdgregory.aws.utils.testhelpers.Log4JCapturingAppender;
import com.kdgregory.aws.utils.testhelpers.mocks.MockAWSLogs;


public class TestLogStreamIterable
{
    private Log4JCapturingAppender logCapture;

//----------------------------------------------------------------------------
//  Per-test boilerplate
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        logCapture = Log4JCapturingAppender.getInstance();
        logCapture.reset();
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testForwardOperationHappyPath() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
                           .withMessage(10, "first")
                           .withMessage(20, "second")
                           .withMessage(30, "third");

        LogStreamIterable iterable = new LogStreamIterable(mock.getInstance(), "foo", "bar");
        Iterator<OutputLogEvent> itx = iterable.iterator();

        int index = 0;
        assertEvent(index++, itx.next(), 10, "first");
        assertEvent(index++, itx.next(), 20, "second");
        assertEvent(index++, itx.next(), 30, "third");

        assertFalse("at end of iterator", itx.hasNext());

        // after finishing the first batch of events, we'll try again
        mock.assertInvocationCount("getLogEvents",  2);

        // nothing should be logged in normal operation
        logCapture.assertLogSize(0);
    }


    @Test
    public void testForwardOperationPaginated() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
                           .withMessage(10, "first")
                           .withMessage(20, "second")
                           .withMessage(30, "third")
                           .withPageSize(2);

        LogStreamIterable iterable = new LogStreamIterable(mock.getInstance(), "foo", "bar");
        Iterator<OutputLogEvent> itx = iterable.iterator();

        int index = 0;
        assertEvent(index++, itx.next(), 10, "first");
        assertEvent(index++, itx.next(), 20, "second");
        assertEvent(index++, itx.next(), 30, "third");

        assertFalse("at end of iterator", itx.hasNext());

        // after finishing the first batch of events, we'll try again
        mock.assertInvocationCount("getLogEvents",  3);

        // nothing should be logged in normal operation
        logCapture.assertLogSize(0);
    }


    @Test
    public void testForwardOperationStartingAt() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
                           .withMessage(10, "first")
                           .withMessage(20, "second")
                           .withMessage(30, "third");

        LogStreamIterable iterable = new LogStreamIterable(mock.getInstance(), "foo", "bar", true, new Date(20));
        Iterator<OutputLogEvent> itx = iterable.iterator();

        int index = 0;
        assertEvent(index++, itx.next(), 20, "second");
        assertEvent(index++, itx.next(), 30, "third");

        assertFalse("at end of iterator", itx.hasNext());

        // after finishing the first batch of events, we'll try again
        mock.assertInvocationCount("getLogEvents",  2);

        // nothing should be logged in normal operation
        logCapture.assertLogSize(0);
    }


    @Test
    public void testBackwardOperationHappyPath() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
                           .withMessage(10, "first")
                           .withMessage(20, "second")
                           .withMessage(30, "third");

        LogStreamIterable iterable = new LogStreamIterable(mock.getInstance(), "foo", "bar", false);
        Iterator<OutputLogEvent> itx = iterable.iterator();

        int index = 0;
        assertEvent(index++, itx.next(), 30, "third");
        assertEvent(index++, itx.next(), 20, "second");
        assertEvent(index++, itx.next(), 10, "first");

        assertFalse("at end of iterator", itx.hasNext());

        // after finishing the first batch of events, we'll try again
        mock.assertInvocationCount("getLogEvents",  2);

        // nothing should be logged in normal operation
        logCapture.assertLogSize(0);
    }


    @Test
    public void testBackwardOperationPaginated() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
                           .withMessage(10, "first")
                           .withMessage(20, "second")
                           .withMessage(30, "third")
                           .withPageSize(2);

        LogStreamIterable iterable = new LogStreamIterable(mock.getInstance(), "foo", "bar", false);
        Iterator<OutputLogEvent> itx = iterable.iterator();

        int index = 0;
        assertEvent(index++, itx.next(), 30, "third");
        assertEvent(index++, itx.next(), 20, "second");
        assertEvent(index++, itx.next(), 10, "first");

        assertFalse("at end of iterator", itx.hasNext());

        // after finishing the first batch of events, we'll try again
        mock.assertInvocationCount("getLogEvents",  3);

        // nothing should be logged in normal operation
        logCapture.assertLogSize(0);
    }


    @Test
    public void testBackwardOperationStartingAt() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
                           .withMessage(10, "first")
                           .withMessage(20, "second")
                           .withMessage(30, "third");

        LogStreamIterable iterable = new LogStreamIterable(mock.getInstance(), "foo", "bar", false, new Date(20));
        Iterator<OutputLogEvent> itx = iterable.iterator();

        int index = 0;
        assertEvent(index++, itx.next(), 20, "second");
        assertEvent(index++, itx.next(), 10, "first");

        assertFalse("at end of iterator", itx.hasNext());

        // after finishing the first batch of events, we'll try again
        mock.assertInvocationCount("getLogEvents",  2);

        // nothing should be logged in normal operation
        logCapture.assertLogSize(0);
    }


    @Test
    // this may be testing the mock rather than the class
    public void testNoElements() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar");

        LogStreamIterable iterable = new LogStreamIterable(mock.getInstance(), "foo", "bar");
        Iterator<OutputLogEvent> itx = iterable.iterator();

        assertFalse("at end of iterator", itx.hasNext());

        // we need at least two calls to see that the iterator hasn't moved
        mock.assertInvocationCount("getLogEvents",  2);

        // nothing should be logged in normal operation
        logCapture.assertLogSize(0);
    }


    @Test
    public void testThrottling() throws Exception
    {
        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
        {
            @Override
            public GetLogEventsResult getLogEvents(GetLogEventsRequest request)
            {
                if (getInvocationCount("getLogEvents") == 1)
                {
                    // exception contents determined by experimentation
                    AWSLogsException ex = new AWSLogsException("message doesn't matter");
                    ex.setErrorCode("ThrottlingException");
                    throw ex;
                }
                else
                    return super.getLogEvents(request);
            }
        }
        .withMessage(10, "first")
        .withMessage(20, "second")
        .withMessage(30, "third");

        // use explicit retry parameters so that this doesn't take a long time
        LogStreamIterable iterable = new LogStreamIterable(mock.getInstance(), "foo", "bar", true, null, 3, 50L);
        Iterator<OutputLogEvent> itx = iterable.iterator();

        long start = System.currentTimeMillis();
        int index = 0;
        assertEvent(index++, itx.next(), 10, "first");
        assertEvent(index++, itx.next(), 20, "second");
        assertEvent(index++, itx.next(), 30, "third");
        assertFalse("at end of iterator", itx.hasNext());
        long elapsed = System.currentTimeMillis() - start;

        // first try gets throttled, second works, third says we're at the end
        mock.assertInvocationCount("getLogEvents",  3);

        assertInRange("execution time", 25, 75, elapsed);

        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, "getLogEvents.*throttled.*delay.*50 ms");
    }


    @Test
    public void testUnrecoveredThrottling() throws Exception
    {
        final AWSLogsException expected = new AWSLogsException("message doesn't matter");
        expected.setErrorCode("ThrottlingException");

        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
        {
            @Override
            public GetLogEventsResult getLogEvents(GetLogEventsRequest request)
            {
                throw expected;
            }
        };

        // use explicit retry parameters so that this doesn't take a long time
        LogStreamIterable iterable = new LogStreamIterable(mock.getInstance(), "foo", "bar", true, null, 3, 50L);
        Iterator<OutputLogEvent> itx = iterable.iterator();

        long start = System.currentTimeMillis();
        try
        {
            itx.next();
            fail("should have thrown");
        }
        catch (AWSLogsException ex)
        {
            long elapsed = System.currentTimeMillis() - start;
            assertInRange("execution time", 325, 375, elapsed);
            mock.assertInvocationCount("getLogEvents",  3);
            assertSame("thrown exception", expected, ex);
        }

        logCapture.assertLogSize(3);
        logCapture.assertLogEntry(0, Level.DEBUG, "getLogEvents.*throttled.*delay.*50 ms");
        logCapture.assertLogEntry(1, Level.DEBUG, "getLogEvents.*throttled.*delay.*100 ms");
        logCapture.assertLogEntry(2, Level.DEBUG, "getLogEvents.*throttled.*delay.*200 ms");
    }


    @Test
    public void testArbitraryAWSLogsException() throws Exception
    {
        final AWSLogsException expected = new AWSLogsException("message doesn't matter");

        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
        {
            @Override
            public GetLogEventsResult getLogEvents(GetLogEventsRequest request)
            {
                throw expected;
            }
        };

        LogStreamIterable iterable = new LogStreamIterable(mock.getInstance(), "foo", "bar");
        Iterator<OutputLogEvent> itx = iterable.iterator();

        try
        {
            itx.next();
            fail("should have thrown");
        }
        catch (AWSLogsException ex)
        {
            assertSame("thrown exception", expected, ex);
            mock.assertInvocationCount("getLogEvents",  1);
        }

        // this library follows the "don't log and throw" rule
        logCapture.assertLogSize(0);
    }


    @Test
    public void testArbitraryException() throws Exception
    {
        final RuntimeException expected = new RuntimeException("message doesn't matter");

        MockAWSLogs mock = new MockAWSLogs("foo", "bar")
        {
            @Override
            public GetLogEventsResult getLogEvents(GetLogEventsRequest request)
            {
                throw expected;
            }
        };

        LogStreamIterable iterable = new LogStreamIterable(mock.getInstance(), "foo", "bar");
        Iterator<OutputLogEvent> itx = iterable.iterator();

        try
        {
            itx.next();
            fail("should have thrown");
        }
        catch (RuntimeException ex)
        {
            assertSame("thrown exception", expected, ex);
            mock.assertInvocationCount("getLogEvents",  1);
        }

        // this library follows the "don't log and throw" rule
        logCapture.assertLogSize(0);
    }

//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    private void assertEvent(int index, OutputLogEvent event, long expectedTimestamp, String expectedMessage)
    {
        assertEquals("event " + index + ", timestamp",  expectedTimestamp,  event.getTimestamp().longValue());
        assertEquals("event " + index + ", message",    expectedMessage,    event.getMessage());
    }
}
