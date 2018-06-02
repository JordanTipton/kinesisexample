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

package com.jordantipton.kinesisexample.model

import com.google.gson.annotations.SerializedName

/**
 * Captures the key elements of a stock trade, such as the ticker symbol, price,
 * number of shares, the type of the trade (buy or sell), and an id uniquely identifying
 * the trade.
 */
data class StockTrade (@SerializedName("tickerSymbol") val tickerSymbol : String,
                       @SerializedName("tradeType") val tradeType : TradeType,
                       @SerializedName("price") val price : Double,
                       @SerializedName("quantity") val quantity :Long,
                       @SerializedName("id") val id : Long) {

    /**
     * Represents the type of the stock trade eg buy or sell.
     */
    enum class TradeType {
        BUY,
        SELL
    }

    override fun toString() : String {
        return "ID %d: %s %d shares of %s for \$%.02f".format(id, tradeType, quantity, tickerSymbol, price)
    }
}