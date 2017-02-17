package com.alibaba.middleware.race.construct;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Globals {
	public static HashMap<String,String> buyerMemoryIndexMap;
	public static HashMap<String,String> goodMemoryIndexMap; 
	
	public static List<String> orderFiles;
	public static List<String> goodFiles;
	public static List<String> buyerFiles;
	
	public static String query1Path;
	public static String query2Path;
	public static String query3Path;

	public static ExecutorService multiQueryPool2;
	public static ExecutorService multiQueryPool3;
	public static ExecutorService multiQueryPool4;
	
	
	public static HashMap<String, String> getBuyerMemoryIndexMap() {
		return buyerMemoryIndexMap;
	}
	public static HashMap<String, String> getGoodMemoryIndexMap() {
		return goodMemoryIndexMap;
	}
	public static List<String> getOrderFiles() {
		return orderFiles;
	}
	public static List<String> getGoodFiles() {
		return goodFiles;
	}
	public static List<String> getBuyerFiles() {
		return buyerFiles;
	}
	public static String getQuery1Path() {
		return query1Path;
	}
	public static String getQuery2Path() {
		return query2Path;
	}
	public static String getQuery3Path() {
		return query3Path;
	}
	public static ExecutorService getMultiQueryPool2() {
		return multiQueryPool2;
	}
	public static ExecutorService getMultiQueryPool3() {
		return multiQueryPool3;
	}
	public static ExecutorService getMultiQueryPool4() {
		return multiQueryPool4;
	}

}
