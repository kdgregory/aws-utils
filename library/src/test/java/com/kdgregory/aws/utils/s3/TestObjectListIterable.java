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

package com.kdgregory.aws.utils.s3;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.Level;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.kdgregory.aws.utils.testhelpers.Log4JCapturingAppender;
import com.kdgregory.aws.utils.testhelpers.mocks.MockAmazonS3;


public class TestObjectListIterable
{
    private Log4JCapturingAppender logCapture;

//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------



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
    public void testBasicOperation() throws Exception
    {
        MockAmazonS3 mock = new MockAmazonS3()
                            .withEmptyObjects("mybucket", "foo", "bar", "baz");

        ObjectListIterable oli = new ObjectListIterable(mock.getInstance(), "mybucket");

        Iterator<S3ObjectSummary> itx = oli.iterator();
        assertEquals("bar", itx.next().getKey());
        assertEquals("baz", itx.next().getKey());
        assertEquals("foo", itx.next().getKey());
        assertFalse("iterator consumed", itx.hasNext());

        mock.assertInvocationCount("listObjects", 1);
        mock.assertLastListBucket("mybucket");
        mock.assertLastListPrefix(null);
        mock.assertLastListMarker(null);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testEmptyObjectList() throws Exception
    {
        MockAmazonS3 mock = new MockAmazonS3()
                            .withBucket("mybucket");

        ObjectListIterable oli = new ObjectListIterable(mock.getInstance(), "mybucket");

        Iterator<S3ObjectSummary> itx = oli.iterator();
        assertFalse("iterator consumed", itx.hasNext());

        mock.assertInvocationCount("listObjects", 1);
        mock.assertLastListBucket("mybucket");
        mock.assertLastListPrefix(null);
        mock.assertLastListMarker(null);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testPaginatedList() throws Exception
    {
        MockAmazonS3 mock = new MockAmazonS3()
                            .withEmptyObjects("mybucket", "foo", "bar", "baz")
                            .withListingPageSize(2);

        ObjectListIterable oli = new ObjectListIterable(mock.getInstance(), "mybucket");

        Iterator<S3ObjectSummary> itx = oli.iterator();
        assertEquals("bar", itx.next().getKey());
        assertEquals("baz", itx.next().getKey());

        // slide these assertions in just before we retrieve the second batch
        mock.assertInvocationCount("listObjects", 1);
        mock.assertInvocationCount("listNextBatchOfObjects", 0);
        mock.assertLastListBucket("mybucket");
        mock.assertLastListPrefix(null);
        mock.assertLastListMarker(null);

        assertEquals("foo", itx.next().getKey());
        assertFalse("iterator consumed", itx.hasNext());

        mock.assertInvocationCount("listObjects", 1);
        mock.assertInvocationCount("listNextBatchOfObjects", 1);
        mock.assertLastListBucket("mybucket");
        mock.assertLastListPrefix(null);
        mock.assertLastListMarker("baz");

        logCapture.assertLogSize(0);
    }


    @Test
    public void testRetrieveByPrefix() throws Exception
    {
        MockAmazonS3 mock = new MockAmazonS3()
                            .withEmptyObjects("mybucket", "foo", "bar", "baz");

        ObjectListIterable oli = new ObjectListIterable(mock.getInstance(), "mybucket", "ba");

        Iterator<S3ObjectSummary> itx = oli.iterator();
        assertEquals("bar", itx.next().getKey());
        assertEquals("baz", itx.next().getKey());
        assertFalse("iterator consumed", itx.hasNext());

        mock.assertInvocationCount("listObjects", 1);
        mock.assertLastListBucket("mybucket");
        mock.assertLastListPrefix("ba");
        mock.assertLastListMarker(null);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testDebugging() throws Exception
    {
        MockAmazonS3 mock = new MockAmazonS3()
                            .withEmptyObjects("mybucket", "foo", "bar", "baz")
                            .withListingPageSize(2);

        ObjectListIterable oli = new ObjectListIterable(mock.getInstance(), "mybucket").withDebugLogging(true);

        Iterator<S3ObjectSummary> itx = oli.iterator();
        assertEquals("bar", itx.next().getKey());
        assertEquals("baz", itx.next().getKey());
        assertEquals("foo", itx.next().getKey());
        assertFalse("iterator consumed", itx.hasNext());

        mock.assertInvocationCount("listObjects", 1);
        mock.assertInvocationCount("listNextBatchOfObjects", 1);

        logCapture.assertLogSize(4);
        logCapture.assertLogEntry(0, Level.DEBUG, "retrieving initial batch from s3://mybucket");
        logCapture.assertLogEntry(1, Level.DEBUG, "retrieved 2 keys from s3://mybucket");
        logCapture.assertLogEntry(2, Level.DEBUG, "retrieving subsequent batch from s3://mybucket");
        logCapture.assertLogEntry(3, Level.DEBUG, "retrieved 1 keys from s3://mybucket");
    }


    @Test
    public void testDebuggingWithPrefix() throws Exception
    {
        MockAmazonS3 mock = new MockAmazonS3()
                            .withEmptyObjects("mybucket", "foo", "bar", "bargle", "baz", "biff")
                            .withListingPageSize(2);

        ObjectListIterable oli = new ObjectListIterable(mock.getInstance(), "mybucket", "ba").withDebugLogging(true);

        Iterator<S3ObjectSummary> itx = oli.iterator();
        assertEquals("bar", itx.next().getKey());
        assertEquals("bargle", itx.next().getKey());
        assertEquals("baz", itx.next().getKey());
        assertFalse("iterator consumed", itx.hasNext());

        mock.assertInvocationCount("listObjects", 1);
        mock.assertInvocationCount("listNextBatchOfObjects", 1);

        logCapture.assertLogSize(4);
        logCapture.assertLogEntry(0, Level.DEBUG, "retrieving initial batch from s3://mybucket/ba\\*");
        logCapture.assertLogEntry(1, Level.DEBUG, "retrieved 2 keys from s3://mybucket/ba\\*");
        logCapture.assertLogEntry(2, Level.DEBUG, "retrieving subsequent batch from s3://mybucket/ba\\*");
        logCapture.assertLogEntry(3, Level.DEBUG, "retrieved 1 keys from s3://mybucket/ba\\*");
    }

    
    @Test
    public void testRepeatability() throws Exception
    {
        MockAmazonS3 mock = new MockAmazonS3()
                            .withEmptyObjects("mybucket", "foo", "bar", "baz");

        ObjectListIterable oli = new ObjectListIterable(mock.getInstance(), "mybucket");

        Iterator<S3ObjectSummary> itx1 = oli.iterator();
        assertEquals("bar", itx1.next().getKey());
        assertEquals("baz", itx1.next().getKey());
        assertEquals("foo", itx1.next().getKey());
        assertFalse("iterator consumed", itx1.hasNext());

        Iterator<S3ObjectSummary> itx2 = oli.iterator();
        assertEquals("bar", itx2.next().getKey());
        assertEquals("baz", itx2.next().getKey());
        assertEquals("foo", itx2.next().getKey());
        assertFalse("iterator consumed", itx2.hasNext());

        mock.assertInvocationCount("listObjects", 2);

        logCapture.assertLogSize(0);
    }
}
