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

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput
import com.amazonaws.services.kinesis.model.Record
import org.apache.commons.logging.LogFactory
import com.jordantipton.kinesisexample.model.StockTrade
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import java.io.IOException

/**
 * Processes records retrieved from stock trades stream.
 *
 */
class StockTradeRecordProcessor : IRecordProcessor {
    private var kinesisShardId: String? = null
    private var nextReportingTimeInMillis: Long = 0
    private var nextCheckpointTimeInMillis: Long = 0

    // Aggregates stats for stock trades
    internal var stockStats = StockStats()

    /**
     * {@inheritDoc}
     */
    override fun initialize(initializationInput: InitializationInput) {
        LOG.info("Initializing record processor for shard: ${initializationInput.shardId}")
        this.kinesisShardId = initializationInput.shardId
        nextReportingTimeInMillis = System.currentTimeMillis() + REPORTING_INTERVAL_MILLIS
        nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS
    }

    /**
     * {@inheritDoc}
     */
    override fun processRecords(processRecordsInput : ProcessRecordsInput) {
        for (record in processRecordsInput.records) {
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
            checkpoint(processRecordsInput.checkpointer)
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
        var trade: StockTrade
        val datumReader = SpecificDatumReader<StockTrade>(StockTrade.getClassSchema())
        val decoder = DecoderFactory.get().binaryDecoder(record.data.array(), null)
        try {
            trade = datumReader.read(null, decoder)
        } catch (e: IOException) {
            LOG.warn("Skipping record. Unable to parse record into StockTrade. Partition Key: \" + record.partitionKey")
            return
        }

        stockStats.addStockTrade(trade!!)
    }

    /**
     * {@inheritDoc}
     */
    override fun shutdown(shutdownInput : ShutdownInput) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId!!)
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (shutdownInput.shutdownReason == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.checkpointer)
        }
    }

    private fun checkpoint(checkpointer : IRecordProcessorCheckpointer) {
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