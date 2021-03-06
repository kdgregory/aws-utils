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

import java.util.List;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.*;


/**
 *  Mocks the following AmazonCloudWatch methods:
 *  <ul>
 *  <li> putMetricData()
 *  </u>
 */
public class MockAmazonCloudWatch
extends AbstractMock<AmazonCloudWatch>
{
    public MockAmazonCloudWatch()
    {
        super(AmazonCloudWatch.class);
    }


    public PutMetricDataResult putMetricData(PutMetricDataRequest request)
    {
        return new PutMetricDataResult();
    }

//----------------------------------------------------------------------------
//  Accessor API
//----------------------------------------------------------------------------

    public PutMetricDataRequest getLastPutRequest()
    {
        return getMostRecentInvocationArg("putMetricData", 0, PutMetricDataRequest.class);
    }


    public MetricDatum getDatumFromLastPut(int index)
    {
        return getLastPutRequest().getMetricData().get(index);
    }


    public List<MetricDatum> getDataFromSavedPut(int index)
    {
         return getInvocationArg("putMetricData", index, 0, PutMetricDataRequest.class).getMetricData();
    }
}