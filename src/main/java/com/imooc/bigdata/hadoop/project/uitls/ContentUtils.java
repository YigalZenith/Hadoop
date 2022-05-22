package com.imooc.bigdata.hadoop.project.uitls;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 从URL中提取topicId
 */
public class ContentUtils {
    public static String getID(String url) {
        Pattern pattern = Pattern.compile("topicId=[0-9]+");
        Matcher matcher = pattern.matcher(url);
        String topicID = "";
        if (matcher.find()) {
            topicID = matcher.group().split("topicId=")[1];
        }
        return StringUtils.isBlank(topicID) ? "-" : topicID;
    }
}
