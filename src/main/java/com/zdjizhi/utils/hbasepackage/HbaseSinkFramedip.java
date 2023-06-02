package com.zdjizhi.utils.hbasepackage;

import com.zdjizhi.common.RadiusRelationshipConfig;
import com.zdjizhi.pojo.Radius;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HbaseSinkFramedip extends RichSinkFunction<Tuple7<String,String,String,String,Long,Integer,Integer>> implements Serializable, SinkFunction<Tuple7<String,String,String,String,Long,Integer,Integer>> {
    private Logger log;

    private String hbase_zookeeper_host;
    public  Map<String, Radius> IpWithAccountMap = new ConcurrentHashMap<>(80000);

    private Connection connection;
    private Admin admin;

    public HbaseSinkFramedip(String hbase_zookeeper_host) {
        this.hbase_zookeeper_host = hbase_zookeeper_host;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log = Logger.getLogger(HbaseSinkFramedip.class);

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", hbase_zookeeper_host);
        configuration.set("hbase.rpc.timeout", String.valueOf(RadiusRelationshipConfig.HBASE_RPC_TIMEOUT));
        configuration.set("zookeeper.recovery.retry", "1");
        configuration.set("hbase.client.retries.number", "1");
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();

        try {

            Table table = connection.getTable(TableName.valueOf(RadiusRelationshipConfig.HBASE_FRAMEDIP_TABLE_NAME));
            Scan scan = new Scan();
            scan.addColumn("radius".getBytes(), "framed_ip".getBytes());
            scan.addColumn("radius".getBytes(), "account".getBytes());
            scan.addColumn("radius".getBytes(), "first_found_time".getBytes());
            scan.addColumn("radius".getBytes(), "last_update_time".getBytes());
            scan.addColumn("radius".getBytes(), "acct_status_type".getBytes());
            scan.addColumn("common".getBytes(), "vsys_id".getBytes());
            if (RadiusRelationshipConfig.HBASE_SCAN_LIMIT != 0) {
                scan.setLimit(RadiusRelationshipConfig.HBASE_SCAN_LIMIT);
            }
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                if( result.containsColumn("radius".getBytes(), "framed_ip".getBytes())  && result.containsColumn("radius".getBytes(), "account".getBytes()) && result.containsColumn("radius".getBytes(), "last_update_time".getBytes())   && result.containsColumn("radius".getBytes(), "first_found_time".getBytes()) && result.containsColumn("radius".getBytes(), "acct_status_type".getBytes()) ) {

                    Radius radius = new Radius();
                    String key = Bytes.toString(result.getRow());
                    String framedip = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("radius"), Bytes.toBytes("framed_ip"))));
                    String account = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("radius"), Bytes.toBytes("account"))));
                    int acct_status_type = Bytes.toInt(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("radius"), Bytes.toBytes("acct_status_type"))));
                    int vsys_id =Bytes.toInt(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("common"), Bytes.toBytes("vsys_id"))));

                    Long first_found_time = Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("radius"), Bytes.toBytes("first_found_time"))));
                    Long last_update_time = Bytes.toLong(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("radius"), Bytes.toBytes("last_update_time"))));
                    radius.setAccount(account);
                    radius.setFramed_ip(framedip);
                    radius.setFirst_found_time(first_found_time);
                    radius.setLast_update_time(last_update_time);
                    radius.setAcct_status_type(acct_status_type);
                    radius.setVsys_id(vsys_id);
                    IpWithAccountMap.put(key, radius);
                }
            }

            log.warn("HBaseUtils Get fullAmount List size->AccountWithIpMap.size(): " + IpWithAccountMap.size());
            scanner.close();
        } catch (IOException ioe) {
            log.error("HBaseUtils getAll() is IOException===>{" + ioe + "}<===");
        } catch (RuntimeException e) {
            log.error("HBaseUtils getAll() is Exception===>{" + e + "}<===");
        }
    }

    public void invoke(Tuple7<String,String,String ,String,Long,Integer,Integer> value, Context context) throws Exception {
        // 按 project:table 归纳

        String key = value.f0;
        String framedIp = value.f2;
        String account = value.f3;
        Long event_time = value.f4;
        int acct_status_type = value.f5;
        int vsys_id = value.f6;

            if (IpWithAccountMap.containsKey(key)) {
                Radius radius = IpWithAccountMap.get(key);

                if (radius.getLast_update_time() <= event_time) {

                    if(acct_status_type==radius.getAcct_status_type()) {

                    if (!radius.getAccount().equals(account)) {
                        radius.setLast_update_time(event_time);
                        radius.setAccount(account);
                        radius.setAcct_status_type(acct_status_type);
                        radius.setVsys_id(vsys_id);

                        Table table = null;
                        try {
                            table = connection.getTable(TableName.valueOf(RadiusRelationshipConfig.HBASE_FRAMEDIP_TABLE_NAME));
                            Put put = new Put(key.getBytes());
                            put.addColumn("radius".getBytes(), "framed_ip".getBytes(), framedIp.getBytes());
                            put.addColumn("radius".getBytes(), "account".getBytes(), account.getBytes());
                            put.addColumn("radius".getBytes(), "last_update_time".getBytes(), Bytes.toBytes(event_time));
                            put.addColumn("radius".getBytes(), "first_found_time".getBytes(), Bytes.toBytes(radius.getFirst_found_time()));
                            put.addColumn("radius".getBytes(), "acct_status_type".getBytes(), Bytes.toBytes(acct_status_type));
                            put.addColumn("common".getBytes(), "vsys_id".getBytes(), Bytes.toBytes(vsys_id));

                            table.put(put);
                            IpWithAccountMap.put(key, radius);
                        } catch (Exception e) {
                            log.error(e.toString());
                        } finally {
                            table.close();
                        }
                    }
                }
                    else{
                        radius.setLast_update_time(event_time);
                        radius.setFramed_ip(framedIp);
                        radius.setAcct_status_type(acct_status_type);
                        radius.setVsys_id(vsys_id);
                        Table table = null;
                        try {
                            table = connection.getTable(TableName.valueOf(RadiusRelationshipConfig.HBASE_FRAMEDIP_TABLE_NAME));
                            Put put = new Put(key.getBytes());
                            put.addColumn("radius".getBytes(), "framed_ip".getBytes(), framedIp.getBytes());
                            put.addColumn("radius".getBytes(), "account".getBytes(), account.getBytes());
                            put.addColumn("radius".getBytes(), "last_update_time".getBytes(), Bytes.toBytes(event_time));
                            put.addColumn("radius".getBytes(), "first_found_time".getBytes(), Bytes.toBytes(radius.getFirst_found_time()));
                            put.addColumn("radius".getBytes(), "acct_status_type".getBytes(), Bytes.toBytes(acct_status_type));
                            put.addColumn("common".getBytes(), "vsys_id".getBytes(), Bytes.toBytes(vsys_id));

                            table.put(put);
                            IpWithAccountMap.put(key, radius);
                        } catch (Exception e) {
                            log.error(e.toString());
                        } finally {
                            table.close();
                        }

                    }


                }
            }
            else {
                Radius radius = new Radius();
                radius.setFramed_ip(framedIp);
                radius.setLast_update_time(event_time);
                radius.setAccount(account);
                radius.setFirst_found_time(event_time);
                radius.setAcct_status_type(acct_status_type);
                radius.setVsys_id(vsys_id);
                Table table = null;
                try {
                    table = connection.getTable(TableName.valueOf(RadiusRelationshipConfig.HBASE_FRAMEDIP_TABLE_NAME));
                    Put put = new Put(key.getBytes());
                    put.addColumn("radius".getBytes(), "framed_ip".getBytes(), framedIp.getBytes());
                    put.addColumn("radius".getBytes(), "account".getBytes(), account.getBytes());
                    put.addColumn("radius".getBytes(), "last_update_time".getBytes(), Bytes.toBytes(event_time));
                    put.addColumn("radius".getBytes(), "first_found_time".getBytes(), Bytes.toBytes(event_time));
                    put.addColumn("radius".getBytes(), "acct_status_type".getBytes(), Bytes.toBytes(acct_status_type));
                    put.addColumn("common".getBytes(), "vsys_id".getBytes(), Bytes.toBytes(vsys_id));

                    table.put(put);

                    IpWithAccountMap.put(key, radius);
                } catch (Exception e) {
                    log.error(e.toString());
                } finally {
                    table.close();
                }
            }

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    /**
     * 创建 hbase 表
     */
  /*  private void createTable(String tableName) throws Exception {
        createNamespace(tableName.split(":")[0]);
        TableName table = TableName.valueOf(tableName);
        if (! admin.tableExists(table)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
            // 固定只有 data 列簇
            hTableDescriptor.addFamily(new HColumnDescriptor("data"));
            admin.createTable(hTableDescriptor);
        }
    }*/

    /**
     * 创建命名空间
     */
    /*private void createNamespace(String namespace) throws Exception {
        try {
            admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        }
    }*/
}
