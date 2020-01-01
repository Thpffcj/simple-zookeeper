package cn.edu.nju.configurationManagement.curator;

/**
 * Created by thpffcj on 2019/12/27.
 */
public class RedisConfig {

    // add 新增配置，update 更新配置，delete 删除配置
    private String type;
    // 如果是add或update，则提供下载地址
    private String url;
    // 备注
    private String remark;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }
}

