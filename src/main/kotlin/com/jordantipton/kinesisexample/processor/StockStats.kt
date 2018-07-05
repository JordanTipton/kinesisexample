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

import java.util.EnumMap
import java.util.HashMap

import com.jordantipton.kinesisexample.model.StockTrade
import com.jordantipton.kinesisexample.model.StockTrade.TradeType

/**
 * Maintains running statistics of stock trades passed to it.
 *
 */
class StockStats {

    // Keeps count of trades for each ticker symbol for each trade type
    private val countsByTradeType:EnumMap<TradeType, MutableMap<String, Long>>

    // Keeps the ticker symbol for the most popular stock for each trade type
    private val mostPopularByTradeType:EnumMap<TradeType, String>

     // Ticker symbol of the stock that had the largest quantity of shares sold
    private var largestSellOrderStock:String? = null

    // Quantity of shares for the largest sell order trade
    private var largestSellOrderQuantity:Long = 0
    /**
     * Constructor.
     */
    init{
        countsByTradeType = EnumMap(TradeType::class.java)
        for (tradeType in TradeType.values())
        {
            countsByTradeType[tradeType] = HashMap()
        }

        mostPopularByTradeType = EnumMap(TradeType::class.java)
    }

    /**
     * Updates the statistics taking into account the new stock trade received.
     *
     * @param trade Stock trade instance
     */
    fun addStockTrade(trade:StockTrade) {
        // update buy/sell count
        val type = trade.tradeType
        val counts = countsByTradeType[type]
        var count:Long? = counts?.get(trade.tickerSymbol)
        if (count == null)
        {
            count = 0L
        }
        counts?.put(trade.tickerSymbol, ++count)

        // update most popular stock
        val mostPopular = mostPopularByTradeType[type]
        if (mostPopular == null || countsByTradeType?.get(type)?.get(mostPopular)!! < count)
        {
            mostPopularByTradeType[type] = trade.tickerSymbol
        }

        // update largest sell order
        if (type === TradeType.SELL)
        {
            if (largestSellOrderStock == null || trade.quantity > largestSellOrderQuantity)
            {
                largestSellOrderStock = trade.tickerSymbol
                largestSellOrderQuantity = trade.quantity
            }
        }
    }

    override fun toString():String {
        return String.format(
            "Most popular stock being bought: %s, %d buys.%n" +
            "Most popular stock being sold: %s, %d sells.%n" +
            "Largest sell order: %d shares of %s.",
            getMostPopularStock(TradeType.BUY), getMostPopularStockCount(TradeType.BUY),
            getMostPopularStock(TradeType.SELL), getMostPopularStockCount(TradeType.SELL),
            largestSellOrderQuantity, largestSellOrderStock)
}

    private fun getMostPopularStock(tradeType:TradeType):String {
        return mostPopularByTradeType[tradeType]!!
    }

    private fun getMostPopularStockCount(tradeType:TradeType):Long? {
        val mostPopular = getMostPopularStock(tradeType)
        return countsByTradeType[tradeType]?.get(mostPopular)
    }
}