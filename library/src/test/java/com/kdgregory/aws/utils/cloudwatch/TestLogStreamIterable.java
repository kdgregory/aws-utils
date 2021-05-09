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

import java.util.Date;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

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

        LogStreamIterable iterable = new LogStreamIterable(mock.getInstance(), "foo", "bar", new Date(20));
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


    // TODO - test with no elements

    // TODO - test timeouts and exceptions


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

//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    private void assertEvent(int index, OutputLogEvent event, long expectedTimestamp, String expectedMessage)
    {
        assertEquals("event " + index + ", timestamp",  expectedTimestamp,  event.getTimestamp().longValue());
        assertEquals("event " + index + ", message",    expectedMessage,    event.getMessage());
    }

}
