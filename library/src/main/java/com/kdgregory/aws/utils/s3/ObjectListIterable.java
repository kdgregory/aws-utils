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
import java.util.NoSuchElementException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ObjectListing;


/**
 *  Produces iterators to list the objects in a bucket, handling pagination.
 *  <p>
 *  Instances normally short-lived: used as the source of an enhanced for loop,
 *  or to produce a Java8 stream.
 *  <p>
 *  The iterators produced by this object do not support removal.
 */
public class ObjectListIterable
implements Iterable<S3ObjectSummary>
{
    private AmazonS3 client;
    private String bucket;
    private String prefix;


    /**
     *  Constructs an instance that iterates all objects in the bucket.
     *
     *  @param client   Used to invoke SDK methods.
     *  @param bucket   Identifies the bucket.
     */
    public ObjectListIterable(AmazonS3 client, String bucket)
    {
        this(client, bucket, null);
    }


    /**
     *  Constructs an instance that iterates only keys with the specifed prefix.
     *
     *  @param client   Used to invoke SDK methods.
     *  @param bucket   Identifies the bucket.
     *  @param prefix   Specifies the prefix for retrieved keys.
     */
    public ObjectListIterable(AmazonS3 client, String bucket, String prefix)
    {
        this.client = client;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    @Override
    public Iterator<S3ObjectSummary> iterator()
    {
        return new ObjectListIterator();
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    private class ObjectListIterator
    implements Iterator<S3ObjectSummary>
    {
        ObjectListing currentBatch;
        Iterator<S3ObjectSummary> currentItx;

        @Override
        public boolean hasNext()
        {
            if (currentBatch == null)
            {
                currentBatch = (prefix == null)
                             ? client.listObjects(bucket)
                             : client.listObjects(bucket, prefix);
                currentItx = currentBatch.getObjectSummaries().iterator();
            }
            else if (currentItx.hasNext())
            {
                return true;
            }
            else if (currentBatch.isTruncated())
            {
                currentBatch = client.listNextBatchOfObjects(currentBatch);
                currentItx = currentBatch.getObjectSummaries().iterator();
            }

            return currentItx.hasNext();
        }

        @Override
        public S3ObjectSummary next()
        {
            if (hasNext())
            {
                return currentItx.next();
            }
            else
            {
                throw new NoSuchElementException("ObjectListIterator exhausted (bucket = " + bucket + ")");
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("remove not supported by ObjectListIterator");
        }
    }
}
