package com.zdjizhi.utils.functions;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.zdjizhi.common.RadiusRelationshipConfig;
import com.zdjizhi.pojo.RadiusMassage;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;

import static cn.hutool.crypto.SecureUtil.md5;


/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/5/2715:01
 */

public class ParseFunction implements MapFunction<String, Tuple7<String,String,String, String,Long, Integer,Integer>> {
    private static final Log logger = LogFactory.get();


    @Override
    public Tuple7<String,String ,String, String,Long,Integer,Integer> map(String message) {


        RadiusMassage radiusMassage = new RadiusMassage();
            try {
                if (StringUtil.isNotBlank(message)) {
                    radiusMassage = JSON.parseObject(message, RadiusMassage.class);

                        if(radiusMassage.getRadius_framed_ip()!=null && radiusMassage.getRadius_account()!=null ){

                        if (RadiusRelationshipConfig.ACCOUNTING_REQUEST == radiusMassage.getRadius_packet_type()){
                            String framedIp=radiusMassage.getRadius_framed_ip();
                            String account=radiusMassage.getRadius_account();
                            int vsys_id=radiusMassage.getCommon_vsys_id();
                            if(vsys_id==0){

                                radiusMassage.setCommon_vsys_id(1);
                            }
                            long event_time = radiusMassage.getRadius_event_timestamp();
                            if(event_time==0){
                                event_time=radiusMassage.getCommon_end_time();
                            }
                            int status =radiusMassage.getRadius_acct_status_type();
                            int onff_status = 1;
                            if (status == 2) {
                                onff_status = 2;
                            }
                            String key_framedIp = md5(framedIp+vsys_id);
                            String key_account = md5(account)+"|"+vsys_id;
                            return Tuple7.of(key_framedIp, key_account, framedIp, account, event_time, onff_status,vsys_id);

                      }
                    }
                }
            } catch (JSONException jse) {
                logger.error("数据转换JSON格式异常,原始日志为:" + message);
            } catch (RuntimeException re) {
                logger.error("Radius日志条件过滤异常,异常信息为：" + re);
            }

        return Tuple7.of("","","","",0L,0,0);
    }
}