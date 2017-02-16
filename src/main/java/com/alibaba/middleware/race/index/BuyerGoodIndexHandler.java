package com.alibaba.middleware.race.index;

import java.util.HashMap;

import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.ExtendBufferedWriter;
import com.alibaba.middleware.race.utils.SimpleLRUCache;

public class BuyerGoodIndexHandler {
	private ExtendBufferedWriter[] buyersIndexWriters;
	private HashMap<String,String> buyerMemoryIndexMap;
	private SimpleLRUCache<String, String> buyersCache;
	
	private ExtendBufferedWriter[] goodsIndexWriters;
	private SimpleLRUCache<String, String> goodsCache;
	private HashMap<String,String> goodMemoryIndexMap; 
	private boolean buyerGoodInMemory = true;
	
	
	public BuyerGoodIndexHandler(){
		goodsCache = new SimpleLRUCache<>(65536);
		this.goodMemoryIndexMap = new HashMap<>(4194304, 1f);	//测试得到good的数目为4194304
		buyersCache = new SimpleLRUCache<>(65536);
		this.buyerMemoryIndexMap = new HashMap<>(8388608, 1f);
	}

	
	public ExtendBufferedWriter[] getBuyersIndexWriters() {
		return buyersIndexWriters;
	}


	public HashMap<String, String> getBuyerMemoryIndexMap() {
		return buyerMemoryIndexMap;
	}

	
	public SimpleLRUCache<String, String> getBuyersCache() {
		return buyersCache;
	}


	public ExtendBufferedWriter[] getGoodsIndexWriters() {
		return goodsIndexWriters;
	}


	public SimpleLRUCache<String, String> getGoodsCache() {
		return goodsCache;
	}


	public HashMap<String, String> getGoodMemoryIndexMap() {
		return goodMemoryIndexMap;
	}


	public boolean isBuyerGoodInMemory() {
		return buyerGoodInMemory;
	}
	
	
	
}
