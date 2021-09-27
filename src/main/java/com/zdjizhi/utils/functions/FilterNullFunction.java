package com.zdjizhi.utils.functions;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.common.RadiusRelationshipConfig;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/5/2715:01
 */
public class FilterNullFunction implements FilterFunction<String> {
    private static final Log logger = LogFactory.get();

    @Override
    public boolean filter(String message) {
        boolean isFilter = false;
        try {
            if (StringUtil.isNotBlank(message)) {
                JSONObject jsonObject = JSONObject.parseObject(message);
                if (jsonObject.containsKey(RadiusRelationshipConfig.PACKET_TYPE) && jsonObject.containsKey(RadiusRelationshipConfig.STATUS_TYPE)) {
                    if (RadiusRelationshipConfig.ACCOUNTING_REQUEST == jsonObject.getInteger(RadiusRelationshipConfig.PACKET_TYPE)
                            && RadiusRelationshipConfig.START_BILLING == jsonObject.getInteger(RadiusRelationshipConfig.STATUS_TYPE)) {
                        isFilter = true;
                    }
                }
            }
        } catch (JSONException jse) {
            logger.error("数据转换JSON格式异常,原始日志为:" + message);
        } catch (RuntimeException re) {
            logger.error("Radius日志条件过滤异常,异常信息为：" + re);
        }
        return isFilter;
    }
}
