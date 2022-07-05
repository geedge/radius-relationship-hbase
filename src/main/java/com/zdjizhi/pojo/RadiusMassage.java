package com.zdjizhi.pojo;

public class RadiusMassage {

    private String radius_framed_ip;
    private String radius_account;
    private long radius_event_timestamp;
    private int radius_acct_status_type;
    private int radius_packet_type;
    private long common_end_time;

    public int getRadius_acct_status_type() {
        return radius_acct_status_type;
    }

    public void setRadius_acct_status_type(int radius_acct_status_type) {
        this.radius_acct_status_type = radius_acct_status_type;
    }

    public int getRadius_packet_type() {
        return radius_packet_type;
    }

    public void setRadius_packet_type(int radius_packet_type) {
        this.radius_packet_type = radius_packet_type;
    }

    public String getRadius_framed_ip() {
        return radius_framed_ip;
    }

    public void setRadius_framed_ip(String radius_framed_ip) {
        this.radius_framed_ip = radius_framed_ip;
    }

    public String getRadius_account() {
        return radius_account;
    }

    public void setRadius_account(String radius_account) {
        this.radius_account = radius_account;
    }

    public long getRadius_event_timestamp() {
        return radius_event_timestamp;
    }

    public void setRadius_event_timestamp(long radius_event_timestamp) {
        this.radius_event_timestamp = radius_event_timestamp;
    }

    public long getCommon_end_time() {
        return common_end_time;
    }

    public void setCommon_end_time(long common_end_time) {
        this.common_end_time = common_end_time;
    }
}
