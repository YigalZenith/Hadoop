package com.imooc.bigdata.hadoop.project.uitls;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 把每行需要的字段保存到map
 */
public class LogParser {
    public Map<String,String> parseLog(String line){
        Map<String,String> map = new HashMap<>();
        IPParser ipParser = IPParser.getInstance();

        if (StringUtils.isNotBlank(line)) {

            String[] fields = line.split("\001");

            // 获取IP及相关字段
            String ip = fields[13];
            String country = "";
            String province = "";
            String city = "";
            if (StringUtils.isNotBlank(ip)){
                IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);
                if (regionInfo!=null){
                    country= regionInfo.getCountry();
                    province = regionInfo.getProvince();
                    city = regionInfo.getCity();
                }
            }
            map.put("ip",StringUtils.isBlank(ip) ? "-" :ip);
            map.put("country",StringUtils.isBlank(country) ? "-" :country);
            map.put("province",StringUtils.isBlank(province) ? "-" :province);
            map.put("city",StringUtils.isBlank(city) ? "-" :city);

            // 获取URL及相关字段
            String url = fields[1];
            map.put("url",StringUtils.isBlank(url) ? "-" :url);

            String topicId = ContentUtils.getID(url);
            map.put("topicId",StringUtils.isBlank(topicId) ? "-" :topicId);

            // 获取time字段
            String time = fields[17];
            map.put("time",StringUtils.isBlank(time) ? "-" :time);
        }

        return map;
    }
}
