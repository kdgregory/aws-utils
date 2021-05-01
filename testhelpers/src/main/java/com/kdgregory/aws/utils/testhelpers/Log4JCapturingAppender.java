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

package com.kdgregory.aws.utils.testhelpers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

import static org.junit.Assert.*;

import net.sf.kdgcommons.test.StringAsserts;


/**
 *  This class acts as a Log4J appender, capturing log events into a list
 *  and providing assertions on that list. For tests that care about such
 *  things, call {@link #reset} in a @Before method. To retrieve the
 *  appender, call {@link #getInstance}.
 */
public class Log4JCapturingAppender
extends AppenderSkeleton
{
    // these must match configuration in log4j.properties
    private final static String LOGGER_NAME = "com.kdgregory.aws.utils";
    private final static String APPENDER_NAME = "test";

    public List<LoggingEvent> events = new ArrayList<LoggingEvent>();

    /**
     *  Retrieves this appender from the current logging context.
     */
    public static Log4JCapturingAppender getInstance()
    {
        Logger testOutputLogger = Logger.getLogger(LOGGER_NAME);
        return (Log4JCapturingAppender)testOutputLogger.getAppender(APPENDER_NAME);
    }

//----------------------------------------------------------------------------
//  Public API
//----------------------------------------------------------------------------

    public synchronized void reset()
    {
        events.clear();
    }

//----------------------------------------------------------------------------
//  AppenderSkeleton overrides
//----------------------------------------------------------------------------

    @Override
    public void close()
    {
        // nothing happening here
    }

    @Override
    public boolean requiresLayout()
    {
        return false;
    }

    @Override
    protected synchronized void append(LoggingEvent event)
    {
        events.add(event);
    }

//----------------------------------------------------------------------------
//  Assertions
//----------------------------------------------------------------------------

    /**
     *  Asserts the number of rows in the log (normally called with 0).
     */
    public void assertLogSize(int expectedSize)
    {
        assertEquals("number of log entries", expectedSize, events.size());
    }


    /**
     *  Assets that a particular log entry matches the specified regex.
     */
    public void assertLogEntry(int idx, Level expectedLevel, String expectedRegex)
    {
        assertEquals("internal logging entry " + idx + " level", expectedLevel, events.get(idx).getLevel());
        
        String logMessage =  String.valueOf(events.get(idx).getMessage());
        StringAsserts.assertRegex(
            "internal logging line " + idx + " message (expected \"" + expectedRegex + "\", was: \"" + logMessage + "\"",
            expectedRegex, logMessage);
    }
}
