/*---------------------------------------------------------------------------*
 * Copyright (c) 2019 McAfee, LLC - All Rights Reserved.                     *
 *---------------------------------------------------------------------------*/

package com.opendxl.databus.cli.entity;

import java.util.Map;

/**
 * <p>Helper class use to deserialize the Consumer instance </p>
 *
 */
public class ConsumerRecordResult {

    /**
     * The related sharing key
     */
    private final String shardingKey;

    /**
     * The related message payload
     */
    private final String payload;

    /**
     * The topic name including the tenant group if apply
     */
    private final String composedTopic;

    /**
     * The topic name
     */
    private final String topic;

    /**
     * The tenant group
     */
    private final String tenantGroup;

    /**
     * The related headers
     */
    private final Map<String, String> headers;

    /**
     * The offset value number
     */
    private final long offset;

    /**
     * The partition number
     */
    private final int partition;

    /**
     * The timestamp when the message is produced
     */
    private final long timestamp;

    /**
     *  Constructor
     *
     * @param shardingKey Sharding Key
     * @param payload message
     * @param composedTopic topic name including the tenant group if apply
     * @param topic topic name
     * @param tenantGroup tenant group
     * @param headers headers
     * @param offset message offset
     * @param partition partition
     * @param timestamp message timestamp
     */
    public ConsumerRecordResult(final String shardingKey,
                                final String payload,
                                final String composedTopic,
                                final String topic,
                                final String tenantGroup,
                                final Map<String, String> headers,
                                final long offset,
                                final int partition,
                                final long timestamp) {

        this.shardingKey = shardingKey;
        this.payload = payload;
        this.composedTopic = composedTopic;
        this.topic = topic;
        this.tenantGroup = tenantGroup;
        this.headers = headers;
        this.offset = offset;
        this.partition = partition;
        this.timestamp = timestamp;

    }
}
