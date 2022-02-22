package com.zdjizhi.utils.functions;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.zdjizhi.common.RadiusRelationshipConfig;
import com.zdjizhi.pojo.RadiusMassage;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;

import static cn.hutool.crypto.SecureUtil.md5;


/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/5/2715:01
 */

public class ParseFunction implements MapFunction<String, Tuple6<String,String,String, String,Long, Integer>> {
    private static final Log logger = LogFactory.get();


    @Override
    public Tuple6<String,String ,String, String,Long,Integer> map(String message) {


        RadiusMassage radiusMassage = new RadiusMassage();
            try {
                if (StringUtil.isNotBlank(message)) {
                    radiusMassage = JSON.parseObject(message, RadiusMassage.class);

                    if(radiusMassage.getRadius_framed_ip()!=null && radiusMassage.getRadius_account()!=null && radiusMassage.getRadius_event_timestamp()!=null){

                        if (RadiusRelationshipConfig.ACCOUNTING_REQUEST == radiusMassage.getRadius_packet_type()){
                            String framedIp=radiusMassage.getRadius_framed_ip();
                            String account=radiusMassage.getRadius_account();
                            Long event_time = radiusMassage.getRadius_event_timestamp();
                            int status =radiusMassage.getRadius_acct_status_type();
                            int onff_status = 1;
                            if (status == 2) {
                                onff_status = 2;
                            }
                            String key_framedIp = md5(framedIp);
                            String key_account = md5(account);
                            return Tuple6.of(key_framedIp, key_account, framedIp, account, event_time, onff_status);

                      }
                    }
                }
            } catch (JSONException jse) {
                logger.error("数据转换JSON格式异常,原始日志为:" + message);
            } catch (RuntimeException re) {
                logger.error("Radius日志条件过滤异常,异常信息为：" + re);
            }

        return Tuple6.of("","","","",0L,0);
    }
}