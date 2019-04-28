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

package com.kdgregory.aws.utils.examples.cloudwatch;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.kdgcommons.lang.StringUtil;
import net.sf.kdgcommons.lang.ThreadUtil;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.util.EC2MetadataUtils;

import com.kdgregory.aws.utils.cloudwatch.MetricReporter;


/**
 *  Demonstrates usage of the metric reporter in background mode, by setting
 *  multiple threads on a "random walk": each thread manages a value, which
 *  starts at 50, and is incremented or decremented each second. If the value
 *  goes above 100 or below 0, it is capped at that number. The value is also
 *  logged at each step, normally with DEBUG, but with WARN as it approaches
 *  the limit, and with ERROR if it exceeds the limit.
 *  <p>
 *  Invocation: <code>MetricReporterExample [NUM_THREADS]</code>
 *  <p>
 *  This program's package name will be used as the metric's namespace, and
 *  its classname will be used as the name of the metric. Each thread will
 *  be reported as a separate dimension, and if running on EC2 the instance
 *  ID will also be reported as a dimension.
 *  <p>
 *  WARNING: this example creates custom metrics, which are charged at $0.30
 *  each after the first 10 per month. It also incurs costs for the number of
 *  reports, and for using high-resolution metrics.
 *
 *  YOU ARE RESPONSIBLE FOR THESE CHARGES.
 */
public class MetricReporterExample
{
    private static Logger logger = LoggerFactory.getLogger(MetricReporterExample.class);


    public static void main(String[] argv)
    throws Exception
    {
        int numThreads = (argv.length == 1)
                       ? Integer.parseInt(argv[0])
                       : 1;

        final AmazonCloudWatch client = AmazonCloudWatchClientBuilder.defaultClient();
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        final String namespace = MetricReporterExample.class.getPackage().getName();
        final String metricName = MetricReporterExample.class.getName();

        // note: we schedule the executor to run every second, even though we only write
        //       metrics every 10 seconds; if there aren't any metrics queued it will do
        //       nothing, and the short interval guarantees that metrics will be written
        //       soon after thery're generated

        final MetricReporter reporter = new MetricReporter(client, namespace, executor, 1000)
                                        .withHighResolution(true);

        // this will increase startup time if you're not running on EC2; it also increases the
        // cost of running, as each instance results in a new metric

        String instanceId = EC2MetadataUtils.getInstanceId();
        if (! StringUtil.isEmpty(instanceId))
        {
            reporter.withDimension("instanceId", instanceId);
        }

        for (int ii = 0 ; ii < numThreads ; ii++)
        {
            new Thread(new RandomWalk(ii, reporter, metricName)).start();
        }

        logger.info("main thread sleeping forever");
        Thread.sleep(Long.MAX_VALUE);
    }


    private static class RandomWalk
    implements Runnable
    {
        private MetricReporter reporter;
        private String metricName;
        private Map<String,String> addedDimensions = new HashMap<String,String>();

        private Random rnd;
        private int value;

        public RandomWalk(int threadNum, MetricReporter reporter, String metricName)
        {
            this.reporter = reporter;
            this.metricName = metricName;

            rnd = new Random(threadNum);
            value = 50;
        }

        @Override
        public void run()
        {
            // must set this dimension from the running thread, not the ctor
            // BEWARE: each thread is a separate metric, charged at $0.30

            addedDimensions.put("thread", Thread.currentThread().getName());

            while (true)
            {
                takeStep();
                reporter.report(metricName, value, addedDimensions);
                ThreadUtil.sleepQuietly(10000L);
            }
        }


        private void takeStep()
        {
            int step = 2 - rnd.nextInt(5);
            value += step;

            if (value < 0)
            {
                logger.error("value is " + value + "; was reset to 0");
                value = 0;
            }
            else if (value > 100)
            {
                logger.error("value is " + value + "; was reset to 100");
                value = 100;
            }
            else if ((value <= 10) || (value >= 90))
            {
                logger.warn("value is " + value);
            }
            else
            {
                logger.debug("value is " + value);
            }
        }
    }
}
