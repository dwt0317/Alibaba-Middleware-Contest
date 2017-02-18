package com.alibaba.middleware.race.construct;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.alibaba.middleware.race.utils.ExtendBufferedWriter;

/**
 * 管理索引构建和查询的共享变量
 */
public enum IndexVariables {
	INSTANCE;
	
	static HashMap<String,String> buyerMemoryIndexMap;
	static HashMap<String,String> goodMemoryIndexMap; 
	static ExecutorService multiQueryPool2;
	static ExecutorService multiQueryPool3;
	static ExecutorService multiQueryPool4;
	
	static List<String> orderFiles;
	static List<String> goodFiles;
	static List<String> buyerFiles;
	
	
	//记录每个文件的行当前写入的record数量，当大于INDEX_LINE_RECORDS的时候，换行
	static int[] query1LineRecords;
	static int[] query2LineRecords;
	static int[] query3LineRecords;
	
	static String query1Path;
	static String query2Path;
	static String query3Path;
	
	static ExtendBufferedWriter[] query1IndexWriters;	
	static ExtendBufferedWriter[] query2IndexWriters;
	static ExtendBufferedWriter[] query3IndexWriters;
	
	
	public HashMap<String, String> getBuyerMemoryIndexMap() {
		return buyerMemoryIndexMap;
	}
	public void setBuyerMemoryIndexMap(HashMap<String, String> buyerMemoryIndexMap) {
		IndexVariables.buyerMemoryIndexMap = buyerMemoryIndexMap;
	}
	public HashMap<String, String> getGoodMemoryIndexMap() {
		return goodMemoryIndexMap;
	}
	public void setGoodMemoryIndexMap(HashMap<String, String> goodMemoryIndexMap) {
		IndexVariables.goodMemoryIndexMap = goodMemoryIndexMap;
	}
	public ExecutorService getMultiQueryPool2() {
		return multiQueryPool2;
	}
	public void setMultiQueryPool2(ExecutorService multiQueryPool2) {
		IndexVariables.multiQueryPool2 = multiQueryPool2;
	}
	public ExecutorService getMultiQueryPool3() {
		return multiQueryPool3;
	}
	public void setMultiQueryPool3(ExecutorService multiQueryPool3) {
		IndexVariables.multiQueryPool3 = multiQueryPool3;
	}
	public ExecutorService getMultiQueryPool4() {
		return multiQueryPool4;
	}
	public void setMultiQueryPool4(ExecutorService multiQueryPool4) {
		IndexVariables.multiQueryPool4 = multiQueryPool4;
	}
	public List<String> getOrderFiles() {
		return orderFiles;
	}
	public void setOrderFiles(List<String> orderFiles) {
		IndexVariables.orderFiles = orderFiles;
	}
	public List<String> getGoodFiles() {
		return goodFiles;
	}
	public void setGoodFiles(List<String> goodFiles) {
		IndexVariables.goodFiles = goodFiles;
	}
	public List<String> getBuyerFiles() {
		return buyerFiles;
	}
	public void setBuyerFiles(List<String> buyerFiles) {
		IndexVariables.buyerFiles = buyerFiles;
	}
	public int[] getQuery1LineRecords() {
		return query1LineRecords;
	}
	public void setQuery1LineRecords(int[] query1LineRecords) {
		IndexVariables.query1LineRecords = query1LineRecords;
	}
	public int[] getQuery2LineRecords() {
		return query2LineRecords;
	}
	public void setQuery2LineRecords(int[] query2LineRecords) {
		IndexVariables.query2LineRecords = query2LineRecords;
	}
	public int[] getQuery3LineRecords() {
		return query3LineRecords;
	}
	public void setQuery3LineRecords(int[] query3LineRecords) {
		IndexVariables.query3LineRecords = query3LineRecords;
	}
	public String getQuery1Path() {
		return query1Path;
	}

	public String getQuery2Path() {
		return query2Path;
	}

	public String getQuery3Path() {
		return query3Path;
	}

	public ExtendBufferedWriter[] getQuery1IndexWriters() {
		return query1IndexWriters;
	}

	public ExtendBufferedWriter[] getQuery2IndexWriters() {
		return query2IndexWriters;
	}

	public ExtendBufferedWriter[] getQuery3IndexWriters() {
		return query3IndexWriters;
	}


}
