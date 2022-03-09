package com.zdjizhi.common;


import com.zdjizhi.utils.system.RadiusRelationshipConfigurations;

/**
 * @author Administrator
 */
public  class   RadiusRelationshipConfig {


    /**
     * 4- Accounting-Request(账户授权)
     */
    public static final int ACCOUNTING_REQUEST = 4;


    /**
     * radius_packet_type
     */
    public static final String PACKET_TYPE = "radius_packet_type";

    /**
     * 1、开始计费
     */
    public static final int START_BILLING = 1;

    public static final int UPDATE_BILLING = 3;

    /**
     * radius_acct_status_type
     */
    public static final String STATUS_TYPE = "radius_acct_status_type";

    /**
     * System
     */
    public static final String HBASE_FRAMEDIP_TABLE_NAME = RadiusRelationshipConfigurations.getStringProperty(0, "hbase.framedip.table.name");

    public static final String HBASE_ACCOUNT_TABLE_NAME = RadiusRelationshipConfigurations.getStringProperty(0, "hbase.account.table.name");

    /**
     * kafka
     */
    public static final String INPUT_KAFKA_SERVERS = RadiusRelationshipConfigurations.getStringProperty(0, "input.kafka.servers");
    public static final String HBASE_ZOOKEEPER_SERVERS = RadiusRelationshipConfigurations.getStringProperty(0, "hbase.zookeeper.servers");
    //public static final String HBASE_ZOOKEEPER_PORT = RadiusRelationshipConfigurations.getStringProperty(0, "hbase.zookeeper.port");
    public static final String SESSION_TIMEOUT_MS = RadiusRelationshipConfigurations.getStringProperty(1, "session.timeout.ms");
    public static final String MAX_POLL_RECORDS = RadiusRelationshipConfigurations.getStringProperty(1, "max.poll.records");
    public static final String MAX_PARTITION_FETCH_BYTES = RadiusRelationshipConfigurations.getStringProperty(1, "max.partition.fetch.bytes");



    public static final String GROUP_ID = RadiusRelationshipConfigurations.getStringProperty(0, "group.id");
    public static final String INPUT_KAFKA_TOPIC = RadiusRelationshipConfigurations.getStringProperty(0, "input.kafka.topic");

    public static final String TOOLS_LIBRARY = RadiusRelationshipConfigurations.getStringProperty(0, "tools.library");
    public static final String KAFKA_USER = RadiusRelationshipConfigurations.getStringProperty(1, "kafka.user");
    public static final String KAFKA_PIN = RadiusRelationshipConfigurations.getStringProperty(1, "kafka.pin");




}