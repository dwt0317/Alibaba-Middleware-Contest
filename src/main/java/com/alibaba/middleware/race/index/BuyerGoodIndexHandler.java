package com.alibaba.middleware.race.index;

import java.util.HashMap;


/**
 * Handle memory index and cache of good and buyer
 */
public class BuyerGoodIndexHandler {
	private HashMap<String,String> buyerMemoryIndexMap;
	private HashMap<String,String> goodMemoryIndexMap; 
	private boolean buyerGoodInMemory = true;
	
	
	public BuyerGoodIndexHandler(){
		this.goodMemoryIndexMap = new HashMap<>(4194304, 1f);	//测试得到good的数目为4194304
		this.buyerMemoryIndexMap = new HashMap<>(8388608, 1f);
	}

	public HashMap<String, String> getBuyerMemoryIndexMap() {
		return buyerMemoryIndexMap;
	}

	public HashMap<String, String> getGoodMemoryIndexMap() {
		return goodMemoryIndexMap;
	}


	public boolean isBuyerGoodInMemory() {
		return buyerGoodInMemory;
	}
	
	
	
}
