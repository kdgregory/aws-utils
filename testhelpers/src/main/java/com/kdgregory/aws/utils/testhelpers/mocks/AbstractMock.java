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

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

import net.sf.kdgcommons.test.SelfMock;


/**
 *  Common superclass for mock objects. This class extends <code>SelfMock</code>,
 *  adding common assertions (which are only available in JUnit 4.x and above).
 */
public abstract class AbstractMock<T>
extends SelfMock<T>
{
    private ConcurrentHashMap<String,Thread> lastInvocationThreads = new ConcurrentHashMap<String,Thread>();


    public AbstractMock(Class<T> mockedClass)
    {
        super(mockedClass);
    }


    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
        lastInvocationThreads.put(method.getName(), Thread.currentThread());
        return super.invoke(proxy, method, args);
    }


//----------------------------------------------------------------------------
//  Assertions
//----------------------------------------------------------------------------

    /**
     *  Asserts that the last invocation did not occur on the current thread
     *  (verifies background operation).
     */
    public void assertLastInvocationNotOnCurrentThread(String methodName)
    {
        Thread invocationThread = lastInvocationThreads.get(methodName);
        assertNotNull("last invocation was tracked", invocationThread);
        assertTrue("invocation was not on current thread", Thread.currentThread() != invocationThread);
    }


    /**
     *  Asserts the number of times that a particular method was invoked.
     */
    public void assertInvocationCount(String methodName, int expected)
    {
        assertInvocationCount(null, methodName, expected);
    }


    /**
     *  Asserts the number of times that a particular method was invoked. The
     *  assertion method will include the provided message suffix (if not null),
     *  which may be used to provide additional information about the assertion.
     */
    public void assertInvocationCount(String messageSuffix, String methodName, int expected)
    {
        String message = "invocation count: " + methodName;
        if (messageSuffix != null)
            message += ", " + messageSuffix;

        assertEquals(message, expected, getInvocationCount(methodName));
    }
}