package com.jordantipton.kinesisexample.writer

import org.junit.Assert
import org.junit.Test

class StockTradeGeneratorTest {
    @Test
    fun getRandomStockTradeTest() {
        val stockTradeGenerator = StockTradeGenerator()
        val randomStockTrade = stockTradeGenerator.getRandomTrade()
        Assert.assertNotNull(randomStockTrade.getId())
        Assert.assertNotNull(randomStockTrade.getTickerSymbol())
        Assert.assertNotNull(randomStockTrade.getPrice())
        Assert.assertNotNull(randomStockTrade.getQuantity())
        Assert.assertNotNull(randomStockTrade.getTradeType())
    }
}