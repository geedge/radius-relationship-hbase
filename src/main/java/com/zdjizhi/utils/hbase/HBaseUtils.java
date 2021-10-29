package com.zdjizhi.utils.hbase;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.RadiusRelationshipConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HBase 工具类
 *
 * @author qidaijie
 */

public class HBaseUtils {
    private static final Log logger = LogFactory.get();
    private static Map<String, String> subIdMap = new ConcurrentHashMap<>(83334);
    private static Connection connection;

    private static HBaseUtils hBaseUtils;

    private static void getInstance() {
        hBaseUtils = new HBaseUtils();
    }


    /**
     * 构造函数-新
     */
    private HBaseUtils() {
        //获取连接
        getConnection();
        //拉取所有
        getAll();
    }

    private static void getConnection() {
        try {
            // 管理Hbase的配置信息
            Configuration configuration = HBaseConfiguration.create();
            // 设置zookeeper节点
            configuration.set("hbase.zookeeper.quorum", RadiusRelationshipConfig.HBASE_ZOOKEEPER_SERVERS);
            configuration.set("hbase.client.retries.number", "3");
            configuration.set("hbase.bulkload.retries.number", "3");
            configuration.set("zookeeper.recovery.retry", "3");
            connection = ConnectionFactory.createConnection(configuration);
            logger.warn("HBaseUtils get HBase connection,now to getAll().");
        } catch (IOException ioe) {
            logger.error("HBaseUtils getHbaseConn() IOException===>{" + ioe + "}<===");
        } catch (RuntimeException e) {
            logger.error("HBaseUtils getHbaseConn() Exception===>{" + e + "}<===");
        }
    }

    /**
     * 更新变量
     */
    private static void change() {
        if (hBaseUtils == null) {
            getInstance();
        }
    }

    /**
     * 写入数据到HBase
     *
     * @param putList puts list
     */
    public static void insertData(List<Put> putList,String tablename) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tablename));
            table.put(putList);
            logger.warn("Update HBase data SUCCESS! Update size ：" + putList.size());
            putList.clear();
        } catch (IOException e) {
            logger.error("Update HBase data ERROR! Message :" + e);
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                logger.error("Close HBase.table ERROR! Message:" + e);
            }
        }

    }

    public static void insertData(Put put,String tablename) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tablename));
            table.put(put);
           // logger.warn("Update HBase data SUCCESS! Update size ：" + putList.size());
        } catch (IOException e) {
            logger.error("Update HBase data ERROR! Message :" + e);
        } finally {
            try {
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                logger.error("Close HBase.table ERROR! Message:" + e);
            }
        }

    }
    /**
     * 获取所有的 key value
     */
    private static void getAll() {
        long begin = System.currentTimeMillis();
        try {
            Table table = connection.getTable(TableName.valueOf(RadiusRelationshipConfig.HBASE_TABLE_NAME));
            Scan scan2 = new Scan();
            ResultScanner scanner = table.getScanner(scan2);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    subIdMap.put(Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            logger.warn("HBaseUtils Get fullAmount List size->subIdMap.size(): " + subIdMap.size());
            logger.warn("HBaseUtils Get fullAmount List size->subIdMap.size() timeConsuming is: " + (System.currentTimeMillis() - begin));
            scanner.close();
        } catch (IOException ioe) {
            logger.error("HBaseUtils getAll() is IOException===>{" + ioe + "}<===");
        } catch (RuntimeException e) {
            logger.error("HBaseUtils getAll() is Exception===>{" + e + "}<===");
        }
    }

    /**
     * 验证数据并与内存中的对比
     *
     * @param ip      framed_ip
     * @param account account
     */
    public static boolean dataValidation(String ip, String account) {
        if (subIdMap.size() == 0) {
            if (hBaseUtils == null) {
                getInstance();
            }
        }
        boolean checkResult = false;
        if (subIdMap.containsKey(ip)) {
            if (!subIdMap.get(ip).equals(account)) {
                checkResult = true;
                subIdMap.put(ip, account);
            }
        } else {
            checkResult = true;
            subIdMap.put(ip, account);
        }
        return checkResult;
    }

}
