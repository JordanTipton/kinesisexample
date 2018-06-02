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

package com.amazonaws.services.kinesis.samples.stocktrades.processor

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import com.google.gson.GsonBuilder
import com.jordantipton.kinesisexample.model.StockTrade

/**
 * Processes records retrieved from stock trades stream.
 *
 */
class StockTradeRecordProcessor : IRecordProcessor {
    private var kinesisShardId: String? = null
    private var nextReportingTimeInMillis: Long = 0
    private var nextCheckpointTimeInMillis: Long = 0

    // Aggregates stats for stock trades
    private var stockStats = StockStats()

    /**
     * {@inheritDoc}
     */
    override fun initialize(shardId: String) {
        LOG.info("Initializing record processor for shard: $shardId")
        this.kinesisShardId = shardId
        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS
    }

    /**
     * {@inheritDoc}
     */
    override fun processRecords(records: List<Record>, checkpointer: IRecordProcessorCheckpointer) {
        for (record in records) {
            // process record
            processRecord(record)
        }

        // If it is time to report stats as per the reporting interval, report stats
        if (System.currentTimeMillis() > nextReportingTimeInMillis) {
            reportStats()
            resetStats()
            nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS
        }

        // Checkpoint once every checkpoint interval
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer)
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS
        }
    }

    private fun reportStats() {
        println("****** Shard " + kinesisShardId + " stats for last 1 minute ******\n" +
                stockStats + "\n" +
                "****************************************************************\n")
    }

    private fun resetStats() {
        stockStats = StockStats()
    }

    private fun processRecord(record: Record) {
        val gson = GsonBuilder().create()

        val trade = gson.fromJson(record.data.toString(), StockTrade::class.java)
        if (trade == null) {
            LOG.warn("Skipping record. Unable to parse record into StockTrade. Partition Key: " + record.partitionKey)
            return
        }
        stockStats.addStockTrade(trade!!)
    }

    /**
     * {@inheritDoc}
     */
    override fun shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId!!)
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer)
        }
    }

    private fun checkpoint(checkpointer: IRecordProcessorCheckpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId!!)
        try {
            checkpointer.checkpoint()
        } catch (se: ShutdownException) {
            // Ignore checkpoint if the processor instance has been shutdown (fail over).
            LOG.info("Caught shutdown exception, skipping checkpoint.", se)
        } catch (e: ThrottlingException) {
            // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
            LOG.error("Caught throttling exception, skipping checkpoint.", e)
        } catch (e: InvalidStateException) {
            // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
            LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e)
        }

    }

    companion object {

        private val LOG = LogFactory.getLog(StockTradeRecordProcessor::class.java)

        // Reporting interval
        private val REPORTING_INTERVAL_MILLIS = 60000L // 1 minute

        // Checkpointing interval
        private val CHECKPOINT_INTERVAL_MILLIS = 60000L // 1 minute
    }

}