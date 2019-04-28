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

package com.kdgregory.aws.utils.examples.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.kdgregory.aws.utils.s3.ObjectListIterable;


/**
 *  Retrieves a listing of keys from a bucket, with date, size and storage class.
 *  Optionally restricts listing to keys with a given prefix. Unlike the AWS CLI,
 *  this listing shows the entire key.
 *  <p>
 *  Invocation:
 *  <pre>
 *      BucketListExample BUCKET_NAME [ KEY_PREFIX ]
 *  </pre>
 */
public class ObjectListExample
{
    public static void main(String[] argv)
    throws Exception
    {
        AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
        ObjectListIterable list = (argv.length == 2)
                                ? new ObjectListIterable(client, argv[0], argv[1])
                                : new ObjectListIterable(client, argv[0]);

        for (S3ObjectSummary obj : list)
        {
            System.out.println(String.format("%-12.12s  %tF %tT  %15d  %s",
                               obj.getStorageClass(),
                               obj.getLastModified(),
                               obj.getLastModified(),
                               obj.getSize(),
                               obj.getKey()));
        }
    }
}
