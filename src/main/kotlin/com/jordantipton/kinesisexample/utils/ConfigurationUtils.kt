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

package com.jordantipton.kinesisexample.utils

import com.amazonaws.ClientConfiguration;

/**
 * Provides configuration related utilities common to both the producer and consumer.
 */
class ConfigurationUtils {
    private val APPLICATION_NAME = "amazon-kinesis-learning"
    private val VERSION = "1.0.0"

    fun getClientConfigWithUserAgent(): ClientConfiguration {
        val config = ClientConfiguration()
        val userAgent = StringBuilder(ClientConfiguration.DEFAULT_USER_AGENT)

        // Separate fields of the user agent with a space
        userAgent.append(" ")
        // Append the application name followed by version number of the sample
        userAgent.append(APPLICATION_NAME)
        userAgent.append("/")
        userAgent.append(VERSION)

        config.userAgentPrefix = userAgent.toString()
        config.userAgentSuffix = null

        return config
    }
}