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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.*;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;


/**
 *  Mocks the following AmazonS3 methods:
 *  <ul>
 *  <li> listObjects(various)
 *  </u>
 *
 *  Some of the ways in which this mock differs from a real S3 bucket:
 *  <ul>
 *  <li> Bucket names and object keys are returned in alphabetical order.
 *  <li> All changes are immediately consistent: in particular, GET after DELETE
 *       will always return nothing. Override to simulate eventual consistency.
 *  </ul>
 */
public class MockAmazonS3
extends AbstractMock<AmazonS3>
{
    private Map<String,MockS3Bucket> repository = new TreeMap<String,MockAmazonS3.MockS3Bucket>();

    // configuration values
    private int pageSize = Integer.MAX_VALUE;

    // these variables track the most recent invocation arguments, across
    // methods that perform similar operations
    private String lastListBucket;
    private String lastListPrefix;
    private String lastListMarker;


    public MockAmazonS3()
    {
        super(AmazonS3.class);
    }

//----------------------------------------------------------------------------
//  Configuration API
//----------------------------------------------------------------------------

    /**
     *  Sets the page size for object listings (since most tests don't want to
     *  create 1,000 objects).
     */
    public MockAmazonS3 withListingPageSize(int value)
    {
        pageSize = value;
        return this;
    }


    /**
     *  Adds a bucket without any content. You can subsequently add objects
     *  to the bucket.
     */
    public MockAmazonS3 withBucket(String bucketName)
    {
        getOrCreateBucket(bucketName);
        return this;
    }


    /**
     *  Adds a list of keys to a bucket with empty content. This is useful for
     *  testing bucket listing utilities such as <code>ObjectListIterable</code>.
     */
    public MockAmazonS3 withEmptyObjects(String bucketName, String... keys)
    {
        for (String key : keys)
        {
            getOrCreateBucket(bucketName).addObject(new MockS3Object(bucketName, key));
        }
        return this;
    }

//----------------------------------------------------------------------------
//  Mock Implementations
//----------------------------------------------------------------------------

    public ObjectListing listObjects(String bucketName)
    {
        return internalListObjects(bucketName, null, null);
    }


    public ObjectListing listObjects(String bucketName, String prefix)
    {
        return internalListObjects(bucketName, prefix, null);
    }


    public ObjectListing listNextBatchOfObjects(ObjectListing prevListing)
    {
        return internalListObjects(prevListing.getBucketName(), prevListing.getPrefix(), prevListing.getNextMarker());
    }

//----------------------------------------------------------------------------
//  Assertion API
//----------------------------------------------------------------------------

    /**
     *  Asserts the name of the bucket passed to the most recent "list"
     *  request (any of the variants).
     */
    public void assertLastListBucket(String expected)
    {
        assertEquals("bucket passed to most recent list request", expected, lastListBucket);
    }


    /**
     *  Asserts the key prefix passed to the most recent "list" request
     *  (any of the variants).
     */
    public void assertLastListPrefix(String expected)
    {
        assertEquals("prefix passed to most recent list request", expected, lastListPrefix);
    }


    /**
     *  Asserts the name of the marker passed to the most recent
     *  <code>listNextBatchOfObjects()</code> invocation.
     */
    public void assertLastListMarker(String expected)
    {
        assertEquals("prefix passed to most recent listNextBatchOfObjects()", expected, lastListMarker);
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  The actual ObjectListing class does not support a mechanism for creating
     *  new values on the client side: it is populated using magic (also known
     *  as internal marshalling code). This subclass provides what we need for
     *  mock operations.
     */
    private static class MockObjectListing
    extends ObjectListing
    {
        private static final long serialVersionUID = 1L;

        private String bucketName;
        private String prefix;
        private List<S3ObjectSummary> objectSummaries;
        private String currentMarker;
        private String nextMarker;

        /**
         *  Constructor variant for non-existent list.
         */
        public MockObjectListing(String bucketName, String prefix)
        {
            this.bucketName = bucketName;
            this.prefix = prefix;
            this.objectSummaries = Collections.<S3ObjectSummary>emptyList();
        }

        /**
         *  Constructor variant for non-paginated list.
         */
        public MockObjectListing(String bucketName, String prefix, Collection<MockS3Object> objects)
        {
            this(bucketName, prefix);
            this.objectSummaries = new ArrayList<S3ObjectSummary>(objects.size());
            for (MockS3Object obj : objects)
            {
                this.objectSummaries.add(obj.toSummary());
            }
        }

        /**
         *  Constructor variant for paginated list.
         */
        public MockObjectListing(String bucketName, String prefix, Collection<MockS3Object> objects, String currentMarker, String nextMarker)
        {
            this(bucketName, prefix, objects);
            this.currentMarker = currentMarker;
            this.nextMarker = nextMarker;
        }

        // overrides below this point

        @Override
        public String getBucketName()
        {
            return bucketName;
        }

        @Override
        public String getPrefix()
        {
            return prefix;
        }

        @Override
        public List<S3ObjectSummary> getObjectSummaries()
        {
            return objectSummaries;
        }

        @Override
        public String getMarker()
        {
            return currentMarker;
        }

        @Override
        public String getNextMarker()
        {
            return nextMarker;
        }

        @Override
        public boolean isTruncated()
        {
            return nextMarker != null;
        }

        @Override
        public List<String> getCommonPrefixes()
        {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public int getMaxKeys()
        {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public String getDelimiter()
        {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public String getEncodingType()
        {
            throw new UnsupportedOperationException("not implemented");
        }
    }


    /**
     *  Holds a bucket and all of its contents.
     */
    private static class MockS3Bucket
    {
        private String name;
        private TreeMap<String,MockS3Object> objects = new TreeMap<String,MockAmazonS3.MockS3Object>();

        public MockS3Bucket(String name)
        {
            this.name = name;
        }

        public void addObject(MockS3Object obj)
        {
            if (! name.equals(obj.bucket))
            {
                throw new IllegalArgumentException(
                    "internal error: tried to add object to wrong bucket"
                    + " (was: " + obj.bucket + ", expected: " + name + ")");
            }

            objects.put(obj.key, obj);
        }


        /**
         *  Extracts all objects that match the provided prefix, starting at the
         *  provided marker.
         */
        public List<MockS3Object> getObjects(String prefix, String prevMarker)
        {
            if (prefix == null)
                prefix = "";
            if (prevMarker == null)
                prevMarker = "";

            List<MockS3Object> result = new ArrayList<MockS3Object>();
            for (Map.Entry<String,MockS3Object> entry : objects.tailMap(prevMarker, false).entrySet())
            {
                if (entry.getKey().startsWith(prefix))
                {
                    result.add(entry.getValue());
                }
            }
            return result;
        }


        /**
         *  Given a list of objects (assumed returned by {@link #getObjects}),
         *  determines whether the last object in the list is the last object in
         *  the bucket. If no, returns the last object's key as a marker for the
         *  next retrieve; if yes, returns null.
         */
        public String getNextMarker(List<MockS3Object> objlist)
        {
            if (objects.isEmpty())
                return null;

            MockS3Object lastObject = objlist.get(objlist.size() - 1);
            String lastKey = lastObject.key;
            return lastKey.equals(objects.lastKey())
                 ? null
                 : lastKey;
        }
    }

    /**
     *  Holds a representation of an object for internal use.
     */
    private static class MockS3Object
    {
        private static byte[] EMPTY_OBJECT = new byte[0];

        public String bucket;
        public String key;
        public String contentType;
        public byte[] data;
        public Date lastModified;

        public MockS3Object(String bucket, String key)
        {
            this.bucket = bucket;
            this.key = key;
            this.contentType = "application/octet-stream";
            this.data = EMPTY_OBJECT;
        }

        public MockS3Object(String bucket, String key, String data)
        throws Exception
        {
            this(bucket, key);
            this.contentType = "text/plain";
            this.data = data.getBytes("UTF-8");
        }

        public S3ObjectSummary toSummary()
        {
            S3ObjectSummary result = new S3ObjectSummary();
            result.setBucketName(bucket);
            result.setKey(key);
            result.setLastModified(lastModified);
            result.setSize(data.length);
            return result;
        }
    }


    /**
     *  Retrieves a mock bucket from the repository. Synchronized because it might
     *  be called after the mock has been configured.
     */
    private synchronized MockS3Bucket getOrCreateBucket(String name)
    {
        MockS3Bucket bucket = repository.get(name);
        if (bucket == null)
        {
            bucket = new MockS3Bucket(name);
            repository.put(name, bucket);
        }
        return bucket;
    }


    /**
     *  Common code for producing an object list.
     */
    private ObjectListing internalListObjects(String bucketName, String prefix, String prevMarker)
    {
        lastListBucket = bucketName;
        lastListPrefix = prefix;
        lastListMarker = prevMarker;

        MockS3Bucket bucket = getOrCreateBucket(bucketName);
        List<MockS3Object> objects =  bucket.getObjects(prefix, prevMarker);

        int limit = Math.min(objects.size(), pageSize);
        String nextMarker = (objects.size() <= limit)
                          ? null
                          : objects.get(limit - 1).key;

        return new MockObjectListing(bucketName, prefix, objects.subList(0, limit), prevMarker, nextMarker);
    }

}