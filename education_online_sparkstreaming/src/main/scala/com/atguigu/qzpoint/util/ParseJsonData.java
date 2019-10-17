package com.atguigu.qzpoint.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class ParseJsonData {

	public static JSONObject getJsonData(String data) {
		try{
			return JSON.parseObject(data);
		} catch (Exception e) {
			return null;
		}
	}
}
