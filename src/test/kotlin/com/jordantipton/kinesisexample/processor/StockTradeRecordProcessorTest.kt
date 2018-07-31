package com.jordantipton.kinesisexample.processor

import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.jordantipton.kinesisexample.model.StockTrade
import com.jordantipton.kinesisexample.model.TradeType
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class StockTradeRecordProcessorTest {
    @Test
    fun processRecordsTest() {
        val processor = StockTradeRecordProcessor()
        val mostPopularBuyStock = "POPB"
        val mostPopularSellStock = "POPS"
        processor.initialize(InitializationInput())
        val stockTrades = arrayOf(
                StockTrade(mostPopularBuyStock, TradeType.BUY, 1.0, 15, 1),
                StockTrade(mostPopularBuyStock, TradeType.BUY, 1.0, 15, 1),
                StockTrade("NPOPB", TradeType.BUY, 1.0, 15, 1),
                StockTrade(mostPopularSellStock, TradeType.SELL, 1.0, 15, 1),
                StockTrade(mostPopularSellStock, TradeType.SELL, 1.0, 15, 1),
                StockTrade("NPOPS", TradeType.SELL, 1.0, 20, 1)
        )
        val records = mutableListOf<Record>()
        val datumWriter = SpecificDatumWriter<StockTrade>(StockTrade.getClassSchema())
        val output = ByteArrayOutputStream()
        val encoder = EncoderFactory.get().binaryEncoder(output, null)
        var bytes : ByteArray
        output.use {
            for(trade in stockTrades) {
                datumWriter.write(trade, encoder)
                encoder.flush()
                bytes = output.toByteArray()
                output.reset()
                val record = Record()
                record.withData(ByteBuffer.wrap(bytes))
                records.add(record)
            }
        }

        processor.processRecords(ProcessRecordsInput().withRecords(records))
        val stockStats = processor.stockStats
        assertEquals(mostPopularBuyStock, stockStats.getMostPopularStock(TradeType.BUY))
        assertEquals(mostPopularSellStock, stockStats.getMostPopularStock(TradeType.SELL))
        assertEquals(2, stockStats.getMostPopularStockCount(TradeType.BUY))
        assertEquals(2, stockStats.getMostPopularStockCount(TradeType.SELL))
    }
}