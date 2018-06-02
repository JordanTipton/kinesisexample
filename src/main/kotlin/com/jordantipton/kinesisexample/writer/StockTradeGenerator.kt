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

import java.util.concurrent.atomic.AtomicLong
import java.util.Random
import com.jordantipton.kinesisexample.model.StockTrade

/**
 * Generates random stock trades by picking randomly from a collection of stocks, assigning a
 * random price based on the mean, and picking a random quantity for the shares.
 *
 */
class StockTradeGenerator {
    private val STOCK_PRICES = listOf(
            StockPrice("AAPL", 119.72),
            StockPrice("XOM", 91.56),
            StockPrice("GOOG", 527.83),
            StockPrice("BRK.A", 223999.88),
            StockPrice("MSFT", 42.36),
            StockPrice("WFC", 54.21),
            StockPrice("JNJ", 99.78),
            StockPrice("WMT", 85.91),
            StockPrice("CHL", 66.96),
            StockPrice("GE", 24.64),
            StockPrice("NVS", 102.46),
            StockPrice("PG", 85.05),
            StockPrice("JPM", 57.82),
            StockPrice("RDS.A", 66.72),
            StockPrice("CVX", 110.43),
            StockPrice("PFE", 33.07),
            StockPrice("FB", 74.44),
            StockPrice("VZ", 49.09),
            StockPrice("PTR", 111.08),
            StockPrice("BUD", 120.39),
            StockPrice("ORCL", 43.40),
            StockPrice("KO", 41.23),
            StockPrice("T", 34.64),
            StockPrice("DIS", 101.73),
            StockPrice("AMZN", 370.56)
    )

    /** The ratio of the deviation from the mean price  */
    private val MAX_DEVIATION = 0.2 // ie 20%

    /** The number of shares is picked randomly between 1 and the MAX_QUANTITY  */
    private val MAX_QUANTITY = 10000

    /** Probability of trade being a sell  */
    private val PROBABILITY_SELL = 0.4 // ie 40%

    private val random = Random()
    private val id = AtomicLong(1)

    /**
     * Return a random stock trade with a unique id every time.
     *
     */
    fun getRandomTrade(): StockTrade {
        // pick a random stock
        val stockPrice = STOCK_PRICES[random.nextInt(STOCK_PRICES.count())]
        // pick a random deviation between -MAX_DEVIATION and +MAX_DEVIATION
        val deviation = (random.nextDouble() - 0.5) * 2.0 * MAX_DEVIATION
        // set the price using the deviation and mean price
        var price = stockPrice.price * (1 + deviation)
        // round price to 2 decimal places
        price = Math.round(price * 100.0) / 100.0

        // set the trade type to buy or sell depending on the probability of sell
        var tradeType: StockTrade.TradeType = StockTrade.TradeType.BUY
        if (random.nextDouble() < PROBABILITY_SELL) {
            tradeType = StockTrade.TradeType.SELL
        }

        // randomly pick a quantity of shares
        val quantity = random.nextInt(MAX_QUANTITY) + 1 // add 1 because nextInt() will return between 0 (inclusive)
        // and MAX_QUANTITY (exclusive). we want at least 1 share.

        return StockTrade(stockPrice.tickerSymbol, tradeType, price, quantity.toLong(), id.getAndIncrement())
    }

    internal inner class StockPrice (val tickerSymbol : String, val price : Double)
}
