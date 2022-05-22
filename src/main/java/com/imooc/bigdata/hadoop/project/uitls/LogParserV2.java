package com.imooc.bigdata.hadoop.project.uitls;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 把每行需要的字段保存到map
 */
public class LogParserV2 {
    public Map<String,String> parseLog(String line){
        Map<String,String> map = new HashMap<>();
        IPParser ipParser = IPParser.getInstance();

        if (StringUtils.isNotBlank(line)) {
            String[] fields = line.split("\t");
            String time = fields[0];
            String ip = fields[1];
            String country = fields[2];
            String province = fields[3];
            String city = fields[4];
            String topicId = fields[5];
            String url = fields[6];

            map.put("time",StringUtils.isBlank(time) ? "-" :time);
            map.put("ip",StringUtils.isBlank(ip) ? "-" :ip);
            map.put("country",StringUtils.isBlank(country) ? "-" :country);
            map.put("province",StringUtils.isBlank(province) ? "-" :province);
            map.put("city",StringUtils.isBlank(city) ? "-" :city);
            map.put("topicId",StringUtils.isBlank(topicId) ? "-" :topicId);
            map.put("url",StringUtils.isBlank(url) ? "-" :url);
        }

        return map;
    }
}
