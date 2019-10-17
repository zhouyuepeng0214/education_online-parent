package com.atguigu.qzpoint.util;

import java.io.InputStream;
import java.util.Properties;

/**
 *
 * 读取配置文件工具类
 */

public class ConfigurationManager {

	private  static Properties prop = new Properties();

	static {
		try {
			InputStream resourceAsStream = ConfigurationManager.class.getClassLoader()
					.getResourceAsStream("config.properties");
			prop.load(resourceAsStream);
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	//获取配置项
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}

	//获取布尔类型的配置项
	public static boolean getBoolean(String key) {
		String property = prop.getProperty(key);
		try{
//			System.out.println("can");
			return Boolean.valueOf(property);
		} catch(Exception e) {
//			System.out.println("not");
			e.printStackTrace();
		}
		return false;
	}


}
