/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * Modifications to this file have been made by Jordan Tipton
 */

package com.jordantipton.kinesisexample.processor

import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Region
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import com.jordantipton.kinesisexample.utils.ConfigurationUtils
import com.jordantipton.kinesisexample.utils.CredentialUtils

/**
 * Uses the Kinesis Client Library (KCL) to continuously consume and process stock trade
 * records from the stock trades stream. KCL monitors the number of shards and creates
 * record processor instances to read and process records from each shard. KCL also
 * load balances shards across all the instances of this processor.
 *
 */
object StockTradesProcessor {

    private val LOG = LogFactory.getLog(StockTradesProcessor::class.java)

    private val ROOT_LOGGER = Logger.getLogger("")
    private val PROCESSOR_LOGGER = Logger.getLogger("com.amazonaws.services.kinesis.samples.stocktrades.processor")

    private fun checkUsage(args: Array<String>) {
        if (args.size != 3) {
            System.err.println("Usage: " + StockTradesProcessor::class.java.simpleName
                    + " <application name> <stream name> <region>")
            System.exit(1)
        }
    }

    /**
     * Sets the global log level to WARNING and the log level for this package to INFO,
     * so that we only see INFO messages for this processor. This is just for the purpose
     * of this tutorial, and should not be considered as best practice.
     *
     */
    private fun setLogLevels() {
        ROOT_LOGGER.level = Level.WARNING
        PROCESSOR_LOGGER.level = Level.INFO
    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        checkUsage(args)

        val applicationName = args[0]
        val streamName = args[1]
        val region = RegionUtils.getRegion(args[2])
        if (region == null) {
            System.err.println(args[2] + " is not a valid AWS region.")
            System.exit(1)
        }

        setLogLevels()

        val credentialUtils = CredentialUtils()
        val configurationUtils = ConfigurationUtils()

        val credentialsProvider = credentialUtils.getCredentialsProvider()

        val workerId = UUID.randomUUID().toString()
        val kclConfig = KinesisClientLibConfiguration(applicationName, streamName, credentialsProvider, workerId)
                .withRegionName(region!!.name)
                .withCommonClientConfig(configurationUtils.getClientConfigWithUserAgent())

        val recordProcessorFactory = StockTradeRecordProcessorFactory()

        // Create the KCL worker with the stock trade record processor factory
        val worker = Worker(recordProcessorFactory, kclConfig)

        var exitCode = 0
        try {
            worker.run()
        } catch (t: Throwable) {
            LOG.error("Caught throwable while processing data.", t)
            exitCode = 1
        }

        System.exit(exitCode)

    }

}