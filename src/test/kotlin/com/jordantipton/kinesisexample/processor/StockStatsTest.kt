package com.jordantipton.kinesisexample.processor

import com.jordantipton.kinesisexample.model.StockTrade
import com.jordantipton.kinesisexample.model.TradeType
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class StockStatsTest {
    @Test
    fun addStockTradeBuy() {
        val trade = StockTrade("tickerSymbol", TradeType.BUY, 1.0, 15, 1)
        val stats = StockStats()

        stats.addStockTrade(trade)
    }

    @Test
    fun addStockTradeSell() {
        val trade = StockTrade("tickerSymbol", TradeType.SELL, 1.0, 15, 1)
        val stats = StockStats()

        stats.addStockTrade(trade)
    }

    @Test
    fun getMostPopularStockBuyEmpty() {
        val stats = StockStats()

        val mostPopular = stats.getMostPopularStock(TradeType.BUY)
        assertNull(mostPopular)
    }

    @Test
    fun getMostPopularStockSellEmpty() {
        val stats = StockStats()

        val mostPopular = stats.getMostPopularStock(TradeType.SELL)
        assertNull(mostPopular)
    }

    @Test
    fun getMostPopularStockBuySingle() {
        val trade = StockTrade("tickerSymbol", TradeType.BUY, 1.0, 15, 1)
        val stats = StockStats()
        stats.addStockTrade(trade)

        val mostPopular = stats.getMostPopularStock(TradeType.BUY)
        assertNotNull(mostPopular)
        assertEquals(trade.getTickerSymbol(), mostPopular)
    }

    @Test
    fun getMostPopularStockSellSingle() {
        val trade = StockTrade("tickerSymbol", TradeType.BUY, 1.0, 15, 1)
        val stats = StockStats()
        stats.addStockTrade(trade)

        val mostPopular = stats.getMostPopularStock(TradeType.BUY)
        assertNotNull(mostPopular)
        assertEquals(trade.getTickerSymbol(), mostPopular)
    }

    @Test
    fun getMostPopularStockBuyMultipleStocks() {
        val mostPopularTickerSymbol = "POP"
        val lessPopularTickerSymbol = "NPOP"
        val trade1 = StockTrade(mostPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)
        val trade2 = StockTrade(lessPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)
        val trade3 = StockTrade(mostPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)
        val trade4 = StockTrade(lessPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)
        val trade5 = StockTrade(mostPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)

        val stats = StockStats()
        stats.addStockTrade(trade1)
        stats.addStockTrade(trade2)
        stats.addStockTrade(trade3)
        stats.addStockTrade(trade4)
        stats.addStockTrade(trade5)

        val mostPopular = stats.getMostPopularStock(TradeType.BUY)
        assertNotNull(mostPopular)
        assertEquals(mostPopularTickerSymbol, mostPopular)
    }

    @Test
    fun getMostPopularStockSellMultipleStocks() {
        val mostPopularTickerSymbol = "POP"
        val lessPopularTickerSymbol = "NPOP"
        val trade1 = StockTrade(mostPopularTickerSymbol, TradeType.SELL, 1.0, 15, 1)
        val trade2 = StockTrade(lessPopularTickerSymbol, TradeType.SELL, 1.0, 15, 1)
        val trade3 = StockTrade(mostPopularTickerSymbol, TradeType.SELL, 1.0, 15, 1)
        val trade4 = StockTrade(lessPopularTickerSymbol, TradeType.SELL, 1.0, 15, 1)
        val trade5 = StockTrade(mostPopularTickerSymbol, TradeType.SELL, 1.0, 15, 1)

        val stats = StockStats()
        stats.addStockTrade(trade1)
        stats.addStockTrade(trade2)
        stats.addStockTrade(trade3)
        stats.addStockTrade(trade4)
        stats.addStockTrade(trade5)

        val mostPopular = stats.getMostPopularStock(TradeType.SELL)
        assertNotNull(mostPopular)
        assertEquals(mostPopularTickerSymbol, mostPopular)
    }

    @Test
    fun getMostPopularStockLessCountMoreTrades() {
        val mostPopularTickerSymbol = "POP"
        val lessPopularTickerSymbol = "NPOP"
        val trade1 = StockTrade(mostPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)
        val trade2 = StockTrade(lessPopularTickerSymbol, TradeType.BUY, 1.0, 100, 1)
        val trade3 = StockTrade(mostPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)

        val stats = StockStats()
        stats.addStockTrade(trade1)
        stats.addStockTrade(trade2)
        stats.addStockTrade(trade3)

        val mostPopular = stats.getMostPopularStock(TradeType.BUY)
        assertNotNull(mostPopular)
        assertEquals(mostPopularTickerSymbol, mostPopular)
    }

    @Test
    fun getMostPopularStockCountBuyEmpty() {
        val stats = StockStats()

        val mostPopular = stats.getMostPopularStockCount(TradeType.BUY)
        assertNull(mostPopular)
    }

    @Test
    fun getMostPopularStockCountSellEmpty() {
        val stats = StockStats()

        val mostPopular = stats.getMostPopularStockCount(TradeType.SELL)
        assertNull(mostPopular)
    }

    @Test
    fun getMostPopularStockCountBuySingle() {
        val trade = StockTrade("tickerSymbol", TradeType.BUY, 1.0, 15, 1)
        val stats = StockStats()
        stats.addStockTrade(trade)

        val mostPopular = stats.getMostPopularStockCount(TradeType.BUY)
        assertNotNull(mostPopular)
        assertEquals(1, mostPopular)
    }

    @Test
    fun getMostPopularStockCountSellSingle() {
        val trade = StockTrade("tickerSymbol", TradeType.SELL, 1.0, 15, 1)
        val stats = StockStats()
        stats.addStockTrade(trade)

        val mostPopular = stats.getMostPopularStockCount(TradeType.SELL)
        assertNotNull(mostPopular)
        assertEquals(1, mostPopular)
    }

    @Test
    fun getMostPopularStockCountBuyMultipleStocks() {
        val mostPopularTickerSymbol = "POP"
        val lessPopularTickerSymbol = "NPOP"
        val trade1 = StockTrade(mostPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)
        val trade2 = StockTrade(lessPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)
        val trade3 = StockTrade(mostPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)
        val trade4 = StockTrade(lessPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)
        val trade5 = StockTrade(mostPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)

        val stats = StockStats()
        stats.addStockTrade(trade1)
        stats.addStockTrade(trade2)
        stats.addStockTrade(trade3)
        stats.addStockTrade(trade4)
        stats.addStockTrade(trade5)

        val mostPopular = stats.getMostPopularStockCount(TradeType.BUY)
        assertNotNull(mostPopular)
        assertEquals(3, mostPopular)
    }

    @Test
    fun getMostPopularStockCountSellMultipleStocks() {
        val mostPopularTickerSymbol = "POP"
        val lessPopularTickerSymbol = "NPOP"
        val trade1 = StockTrade(mostPopularTickerSymbol, TradeType.SELL, 1.0, 15, 1)
        val trade2 = StockTrade(lessPopularTickerSymbol, TradeType.SELL, 1.0, 15, 1)
        val trade3 = StockTrade(mostPopularTickerSymbol, TradeType.SELL, 1.0, 15, 1)
        val trade4 = StockTrade(lessPopularTickerSymbol, TradeType.SELL, 1.0, 15, 1)
        val trade5 = StockTrade(mostPopularTickerSymbol, TradeType.SELL, 1.0, 15, 1)

        val stats = StockStats()
        stats.addStockTrade(trade1)
        stats.addStockTrade(trade2)
        stats.addStockTrade(trade3)
        stats.addStockTrade(trade4)
        stats.addStockTrade(trade5)

        val mostPopular = stats.getMostPopularStockCount(TradeType.SELL)
        assertNotNull(mostPopular)
        assertEquals(3, mostPopular)
    }

    @Test
    fun getMostPopularStockCountLessCountMoreTrades() {
        val mostPopularTickerSymbol = "POP"
        val lessPopularTickerSymbol = "NPOP"
        val trade1 = StockTrade(mostPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)
        val trade2 = StockTrade(lessPopularTickerSymbol, TradeType.BUY, 1.0, 100, 1)
        val trade3 = StockTrade(mostPopularTickerSymbol, TradeType.BUY, 1.0, 15, 1)

        val stats = StockStats()
        stats.addStockTrade(trade1)
        stats.addStockTrade(trade2)
        stats.addStockTrade(trade3)

        val mostPopular = stats.getMostPopularStockCount(TradeType.BUY)
        assertNotNull(mostPopular)
        assertEquals(2, mostPopular)
    }

    @Test
    fun stockStatsToString() {
        val mostPopularBuyStock = "POPB"
        val mostPopularSellStock = "POPS"
        val trade1 = StockTrade(mostPopularBuyStock, TradeType.BUY, 1.0, 15, 1)
        val trade2 = StockTrade(mostPopularBuyStock, TradeType.BUY, 1.0, 15, 1)
        val trade3 = StockTrade("NPOPB", TradeType.BUY, 1.0, 15, 1)
        val trade4 = StockTrade(mostPopularSellStock, TradeType.SELL, 1.0, 15, 1)
        val trade5 = StockTrade(mostPopularSellStock, TradeType.SELL, 1.0, 15, 1)
        val trade6 = StockTrade("NPOPS", TradeType.SELL, 1.0, 20, 1)

        val stats = StockStats()
        stats.addStockTrade(trade1)
        stats.addStockTrade(trade2)
        stats.addStockTrade(trade3)
        stats.addStockTrade(trade4)
        stats.addStockTrade(trade5)
        stats.addStockTrade(trade6)

        val expected = String.format(
                "Most popular stock being bought: %s, %d buys.%n" +
                "Most popular stock being sold: %s, %d sells.%n" +
                "Largest sell order: %d shares of %s.",
                mostPopularBuyStock, 2,
                mostPopularSellStock, 2,
                20, "NPOPS")

        assertEquals(expected, stats.toString())
    }
}