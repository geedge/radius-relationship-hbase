package com.zdjizhi.utils.system;

import com.zdjizhi.utils.StringUtil;

import java.io.IOException;
import java.util.Locale;
import java.util.Properties;


/**
 * @author Administrator
 */

public final class RadiusRelationshipConfigurations {

    private static Properties propDefault = new Properties();
    private static Properties propService = new Properties();


    public static String getStringProperty(Integer type, String key) {
        if (type == 0) {
            return propService.getProperty(key);
        } else if (type == 1) {
            return propDefault.getProperty(key);
        } else {
            return null;
        }

    }

    public static Integer getIntProperty(Integer type, String key) {
        if (type == 0) {
            return Integer.parseInt(propService.getProperty(key));
        } else if (type == 1) {
            return Integer.parseInt(propDefault.getProperty(key));
        } else {
            return null;
        }
    }

    public static Long getLongProperty(Integer type, String key) {
        if (type == 0) {
            return Long.parseLong(propService.getProperty(key));
        } else if (type == 1) {
            return Long.parseLong(propDefault.getProperty(key));
        } else {
            return null;
        }
    }

    public static Boolean getBooleanProperty(Integer type, String key) {
        if (type == 0) {
            return StringUtil.equals(propService.getProperty(key).toLowerCase().trim().toUpperCase(Locale.ENGLISH), "true");
        } else if (type == 1) {
            return StringUtil.equals(propDefault.getProperty(key).toLowerCase().trim().toUpperCase(Locale.ENGLISH), "true");
        } else {
            return null;
        }
    }

    static {
        try {
            propService.load(RadiusRelationshipConfigurations.class.getClassLoader().getResourceAsStream("service_flow_config.properties"));
            propDefault.load(RadiusRelationshipConfigurations.class.getClassLoader().getResourceAsStream("default_config.properties"));
        } catch (IOException | RuntimeException e) {
            propDefault = null;
            propService = null;
        }
    }
}
