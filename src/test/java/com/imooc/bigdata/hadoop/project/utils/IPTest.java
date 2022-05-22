package com.imooc.bigdata.hadoop.project.utils;

import com.imooc.bigdata.hadoop.project.uitls.IPParser;
import org.junit.Test;

public class IPTest {
    @Test
    public void getIP(){
        IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("211.151.212.1");
        System.out.println(regionInfo.getCountry());
        System.out.println(regionInfo.getProvince());
        System.out.println(regionInfo.getCity());
    }
}
