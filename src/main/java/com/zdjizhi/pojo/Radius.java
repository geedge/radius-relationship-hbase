package com.zdjizhi.pojo;

public class Radius {

    private String framed_ip;
    private String account;
    private Long last_update_time;
    private Long first_found_time;
    private Integer acct_status_type;


    public String getFramed_ip() {
        return framed_ip;
    }

    public void setFramed_ip(String framed_ip) {
        this.framed_ip = framed_ip;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public Long getLast_update_time() {
        return last_update_time;
    }

    public void setLast_update_time(Long last_update_time) {
        this.last_update_time = last_update_time;
    }

    public Long getFirst_found_time() {
        return first_found_time;
    }

    public void setFirst_found_time(Long first_found_time) {
        this.first_found_time = first_found_time;
    }

    public Integer getAcct_status_type() {
        return acct_status_type;
    }

    public void setAcct_status_type(Integer acct_status_type) {
        this.acct_status_type = acct_status_type;
    }
}
