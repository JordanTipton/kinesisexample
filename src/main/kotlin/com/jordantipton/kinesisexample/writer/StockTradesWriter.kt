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

package com.jordantipton.kinesisexample.writer

import org.apache.commons.logging.LogFactory

import com.amazonaws.AmazonClientException
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.google.gson.GsonBuilder
import com.jordantipton.kinesisexample.model.StockTrade
import com.jordantipton.kinesisexample.utils.ConfigurationUtils
import com.jordantipton.kinesisexample.utils.CredentialUtils
import java.nio.ByteBuffer


object StockTradesWriter {

    private val LOG = LogFactory.getLog(StockTradesWriter::class.java)

    private fun checkUsage(args: Array<String>) {
        if (args.size != 2) {
            System.err.println("Usage: " + StockTradesWriter::class.java.simpleName
                    + " <stream name> <region>")
            System.exit(1)
        }
    }

    /**
     * Checks if the stream exists and is active
     *
     * @param kinesisClient Amazon Kinesis client instance
     * @param streamName Name of stream
     */
    private fun validateStream(kinesisClient: AmazonKinesis, streamName: String) {
        try {
            val result = kinesisClient.describeStream(streamName)
            if ("ACTIVE" != result.streamDescription.streamStatus) {
                System.err.println("Stream $streamName is not active. Please wait a few moments and try again.")
                System.exit(1)
            }
        } catch (e: ResourceNotFoundException) {
            System.err.println("Stream $streamName does not exist. Please create it in the console.")
            System.err.println(e)
            System.exit(1)
        } catch (e: Exception) {
            System.err.println("Error found while describing the stream $streamName")
            System.err.println(e)
            System.exit(1)
        }

    }

    /**
     * Uses the Kinesis client to send the stock trade to the given stream.
     *
     * @param trade instance representing the stock trade
     * @param kinesisClient Amazon Kinesis client
     * @param streamName Name of stream
     */
    private fun sendStockTrade(trade: StockTrade, kinesisClient: AmazonKinesis,
                               streamName: String) {
        val gson = GsonBuilder().create()

        val bytes = gson.toJson(trade).toByteArray()
        // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
        if (bytes == null) {
            LOG.warn("Could not get JSON bytes for stock trade")
            return
        }

        LOG.info("Putting trade: " + trade.toString())
        val putRecord = PutRecordRequest()
        putRecord.streamName = streamName
        // We use the ticker symbol as the partition key, as explained in the tutorial.
        putRecord.partitionKey = trade.tickerSymbol
        putRecord.data = ByteBuffer.wrap(bytes)

        try {
            kinesisClient.putRecord(putRecord)
        } catch (ex: AmazonClientException) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex)
        }

    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        checkUsage(args)

        val streamName = args[0]
        val regionName = args[1]
        val region = RegionUtils.getRegion(regionName)
        if (region == null) {
            System.err.println("$regionName is not a valid AWS region.")
            System.exit(1)
        }

        val clientBuilder = AmazonKinesisClientBuilder.standard()
        val credentialUtils = CredentialUtils()
        val configurationUtils = ConfigurationUtils()

        clientBuilder.region = regionName
        clientBuilder.credentials = credentialUtils.getCredentialsProvider()
        clientBuilder.clientConfiguration = configurationUtils.getClientConfigWithUserAgent()

        val kinesisClient = clientBuilder.build()

        // Validate that the stream exists and is active
        validateStream(kinesisClient, streamName)

        // Repeatedly send stock trades with a 100 milliseconds wait in between
        val stockTradeGenerator = StockTradeGenerator()
        while (true) {
            val trade = stockTradeGenerator.getRandomTrade()
            sendStockTrade(trade, kinesisClient, streamName)
            Thread.sleep(100)
        }
    }
}