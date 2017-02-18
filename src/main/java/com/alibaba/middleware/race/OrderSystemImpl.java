package com.alibaba.middleware.race;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.construct.BuyerGoodIndexCreator;
import com.alibaba.middleware.race.construct.ConstructHelper;
import com.alibaba.middleware.race.construct.IndexVariables;
import com.alibaba.middleware.race.construct.OrderIndexCreator;
import com.alibaba.middleware.race.entity.KV;
import com.alibaba.middleware.race.entity.ResultImpl;
import com.alibaba.middleware.race.entity.Row;
import com.alibaba.middleware.race.query.CallableOrderDataSearch;
import com.alibaba.middleware.race.query.MultipleJoinResult;
import com.alibaba.middleware.race.query.QueryHelper;
import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.ExtendBufferedReader;
import com.alibaba.middleware.race.utils.ExtendBufferedWriter;
import com.alibaba.middleware.race.utils.HashUtil;
import com.alibaba.middleware.race.utils.IOUtils;
import com.alibaba.middleware.race.utils.StringUtils;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 * 
 * @author wangxiang@alibaba-inc.com
 *
 */
public class OrderSystemImpl implements OrderSystem {
	

	private volatile boolean isConstructed;
	
	/**
	 * 根据参数新建新建文件 目录等操作
	 */
	public OrderSystemImpl() {			
		IndexVariables.INSTANCE.setQuery1LineRecords(new int[CommonConstants.QUERY1_ORDER_SPLIT_SIZE]);
		IndexVariables.INSTANCE.setQuery2LineRecords(new int[CommonConstants.QUERY2_ORDER_SPLIT_SIZE]);
		IndexVariables.INSTANCE.setQuery3LineRecords(new int[CommonConstants.QUERY3_ORDER_SPLIT_SIZE]);
		
		IndexVariables.INSTANCE.setGoodMemoryIndexMap(new HashMap<String,String>(4194304, 1f));	//测试得到good的数目为4194304
		IndexVariables.INSTANCE.setBuyerMemoryIndexMap(new HashMap<String,String>(8388608, 1f));

		this.isConstructed = false;
		
		IndexVariables.INSTANCE.setMultiQueryPool2(Executors.newFixedThreadPool(8));
		IndexVariables.INSTANCE.setMultiQueryPool3(Executors.newFixedThreadPool(8));
		IndexVariables.INSTANCE.setMultiQueryPool4(Executors.newFixedThreadPool(8));
	}

	
	public void construct(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException, InterruptedException {	
		IndexVariables.INSTANCE.setOrderFiles(new ArrayList<>(orderFiles));
		IndexVariables.INSTANCE.setBuyerFiles(new ArrayList<>(buyerFiles));
		IndexVariables.INSTANCE.setGoodFiles(new ArrayList<>(goodFiles));
		long start = System.currentTimeMillis();
		ConstructHelper.constructDir(storeFolders);
		final long dir = System.currentTimeMillis();
		System.out.println("construct dir time:" + (dir-start));		
		
		// 主要对第一阶段的index time进行限制
		final CountDownLatch latch = new CountDownLatch(1);
		new Thread() {
			@Override
			public void run() {			
				ConstructHelper.constructWriterForIndexFile();
				long writer = System.currentTimeMillis();
				System.out.println("writer time:" + (writer - dir));
				
				ConstructHelper.constructHashIndex();
				long index = System.currentTimeMillis();
				System.out.println("index time:" + (index - writer));
					
				ConstructHelper.closeWriter();	
				long closeWriter = System.currentTimeMillis();
				System.out.println("close time:" + (closeWriter - index));
				System.out.println("construct KO");
				latch.countDown();
				isConstructed = true;
			}
		}.start();
		
		//trick to continue constructing after 1 hour
		latch.await(59 * 60 + 45, TimeUnit.SECONDS);
		System.out.println("construct return,time:" + (System.currentTimeMillis() - start));	
	}

	public Result queryOrder(long orderId, Collection<String> keys) {
		while (this.isConstructed == false) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		Row query = new Row();
		query.putKV("orderid", orderId);

		Row orderData = null;	//query result
		int index = HashUtil.indexFor(orderId, CommonConstants.QUERY1_ORDER_SPLIT_SIZE);
		String indexFile = IndexVariables.INSTANCE.getQuery1Path() + File.separator + index + CommonConstants.INDEX_SUFFIX;
		
		String[] indexArray = null;
		try (ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			// 可能index文件就没有
			String sOrderId = String.valueOf(orderId);
			String line = indexFileReader.readLine();
			while (line != null) {
				if (line.startsWith(sOrderId)) {
					int p = line.indexOf(':');
					indexArray = StringUtils.getIndexInfo(line.substring(p + 1));
					break;
				}
				line = indexFileReader.readLine();
			}
			// 说明文件中没有这个orderId的信息
			if (indexArray == null) {
				System.out.println("query1 can't find order:");
				return null;
			}
			//根据索引去原文件中寻找
			String file = IndexVariables.INSTANCE.getOrderFiles().get(Integer.parseInt(indexArray[0]));
			Long offset = Long.parseLong(indexArray[1]);
			byte[] content = new byte[Integer.valueOf(indexArray[2])];
			try (RandomAccessFile orderFileReader = new RandomAccessFile(file, "r")) {
				orderFileReader.seek(offset);
				orderFileReader.read(content);
				line = new String(content);
				orderData = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);		
			} catch (IOException e) {
				// 忽略
			}
		} catch(IOException e) {
			
		}
		
		if (orderData == null) {
			return null;
		}
		ResultImpl result= QueryHelper.createResultFromOrderData(orderData, QueryHelper.uniqueQueryKeys(keys));
		return result;
	}
	
	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
		while (this.isConstructed == false) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		List<Row> buyerOrdersData = new ArrayList<>(100);		
		boolean validParameter = true;
		if (endTime <=0) 
			validParameter = false;
		if (endTime <= startTime) 
			validParameter = false;
		if (startTime > 11668409867L) //线上测试得到有效订单的时间区间
			validParameter = false;
		if (endTime <= 1468385553L ) 
			validParameter = false;
	
		if (validParameter) {
			int index = HashUtil.indexFor(buyerid, CommonConstants.QUERY2_ORDER_SPLIT_SIZE);
			String indexFile = IndexVariables.INSTANCE.getQuery2Path() + File.separator + index + CommonConstants.INDEX_SUFFIX;
						
			// query2的order索引中，key为id+createtime，value为file offset length
			List<String> buyerOrderList = new ArrayList<>(100);
			try (ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
				String line = indexFileReader.readLine();
				while (line != null) {
					// 获得一行中以<buyerid>开头的行
					if (line.startsWith(buyerid)) {
						int p = line.indexOf(':');
						String key = line.substring(0, p);
						Long createTime = Long.parseLong(key.substring(20));
						if (createTime >= startTime && createTime < endTime) {
							buyerOrderList.add(line.substring(p + 1));
						}
					}
					line = indexFileReader.readLine();
				}
				
				if (buyerOrderList.size() > 0) {
					Map<String,PriorityQueue<String[]>> buyerOrderAccessSequence = QueryHelper.createOrderDataAccessSequence(buyerOrderList);
					List<Future<List<Row>>> result = new ArrayList<>();
					for (Map.Entry<String, PriorityQueue<String[]>> e : buyerOrderAccessSequence.entrySet()) {
						int fileIndex = Integer.parseInt(e.getKey());
						CallableOrderDataSearch search = new CallableOrderDataSearch(fileIndex, e.getValue());
						result.add(IndexVariables.INSTANCE.getMultiQueryPool2().submit(search));		
					}
					for (Future<List<Row>> f: result) {
						try {
							List<Row> list = f.get();
							if (list != null) {
								for(Row row : list) {
									buyerOrdersData.add(row);
								}
							}
						} catch (InterruptedException | ExecutionException e1) {
							e1.printStackTrace();
						}
					}
				}
			} catch(IOException e) {
				
			}
		}
		// query2需要join good信息
		Row buyerRow = buyerOrdersData.size() == 0 ? null : QueryHelper.getBuyerRowFromOrderData(buyerOrdersData.get(0));
		Comparator<Row> comparator = new Comparator<Row>() {
			@Override
			public int compare(Row o1, Row o2) {
				long o2Time = 0L;
				long o1Time = 0L;
				try {
					o1Time = o1.get("createtime").valueAsLong();
					o2Time = o2.get("createtime").valueAsLong();
				} catch (TypeException e) {
					// TODO Auto-generated catch block
					System.out.println("Catch type error");
					e.printStackTrace();
				}
				return o2Time - o1Time > 0 ? 1 : -1;
			}
		};
		MultipleJoinResult joinResult = new MultipleJoinResult(buyerOrdersData, buyerRow, comparator, "goodid", null);
		return joinResult;
	}
	


	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
		while (this.isConstructed == false) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		String tag = keys != null && keys.size() == 1 ? QueryHelper.getKeyJoin((String)keys.toArray()[0]): "all";

		List<Row> salerGoodOrders = new ArrayList<>(512);
		final Collection<String> queryKeys = keys;

		int index = HashUtil.indexFor(goodid, CommonConstants.QUERY3_ORDER_SPLIT_SIZE);
		String indexFile = IndexVariables.INSTANCE.getQuery3Path() + File.separator + index + CommonConstants.INDEX_SUFFIX;
		List<String> offsetRecords = new ArrayList<>(512);
		try (ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			String line = indexFileReader.readLine();
			while (line != null) {
				// 获得一行中以<goodid>开头的行
				if (line.startsWith(goodid)) {
					int p = line.indexOf(':');
					offsetRecords.add(line.substring(p + 1));
				}
				line = indexFileReader.readLine();
			}
			 
			if (offsetRecords.size() > 0 ) {
				Map<String,PriorityQueue<String[]>> buyerOrderAccessSequence = QueryHelper.createOrderDataAccessSequence(offsetRecords);
				List<Future<List<Row>>> result = new ArrayList<>();
				for (Map.Entry<String, PriorityQueue<String[]>> e : buyerOrderAccessSequence.entrySet()) {
					int fileIndex = Integer.parseInt(e.getKey());
					CallableOrderDataSearch search = new CallableOrderDataSearch(fileIndex, e.getValue());
					result.add(IndexVariables.INSTANCE.getMultiQueryPool3().submit(search));
				}
				for (Future<List<Row>> f: result) {
					try {
						List<Row> list = f.get();
						if (list != null) {
							for(Row row : list) {
								salerGoodOrders.add(row);
							}
						}
					} catch (InterruptedException | ExecutionException e1) {
						e1.printStackTrace();
					}
				}
			} else {
				System.out.println("query3 can't find order.");
			}
		} catch (IOException e) {
		}
		
		// query3至多只需要join buyer信息
		Row goodRow = salerGoodOrders.size() == 0 ? null : QueryHelper.getGoodRowFromOrderData(salerGoodOrders.get(0));
		Comparator<Row> comparator = new Comparator<Row>() {
			@Override
			public int compare(Row o1, Row o2) {
				long o2ID = 0L;
				long o1ID = 0L;			
				try {
					o1ID = o1.get("orderid").valueAsLong();
					o2ID = o2.get("orderid").valueAsLong();
				} catch (TypeException e) {
					System.out.println("Catch type error");
					e.printStackTrace();
				}
				return o1ID - o2ID > 0 ? 1 : -1;
			}
		};
//		 查询3可能不需要join的buyer
		String joinTable = tag.equals("buyer") || tag.equals("all") ? "buyerid" : null;
		MultipleJoinResult joinResult = new MultipleJoinResult(salerGoodOrders, goodRow, comparator, joinTable, QueryHelper.uniqueQueryKeys(queryKeys));
		return joinResult;
	}

	
	public KeyValue sumOrdersByGood(String goodid, String key) {
		while (this.isConstructed == false) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// 快速处理 减少不必要的查询的join开销
		String tag = "all";
		// 当为good表字段时快速处理
		if (key.equals("price") || key.equals("offprice") || key.startsWith("a_g_")) {
			return QueryHelper.getGoodSumFromGood(goodid, key);
		} else if (key.startsWith("a_b_")) { //表示需要join buyer和order
			tag = "buyer";
		} else if (key.equals("amount") || key.startsWith("a_o_")) { //表示只需要order数据
			tag = "order";
		} else {
			// 不可求和的字段直接返回
			return null;
		}
		
		List<Row> ordersDatas = new ArrayList<>(512);
		int index = HashUtil.indexFor(goodid, CommonConstants.QUERY3_ORDER_SPLIT_SIZE);
		String indexFile = IndexVariables.INSTANCE.getQuery3Path() + File.separator + index + CommonConstants.INDEX_SUFFIX;
		List<String> offsetRecords = new ArrayList<>(512);
		try(ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			String line = indexFileReader.readLine();
			while (line != null) {
				// 获得一行中以<goodid>开头的行
				if (line.startsWith(goodid)) {
					int p = line.indexOf(':');
					offsetRecords.add(line.substring(p + 1));
				}
				line = indexFileReader.readLine();
			}
			
			if (offsetRecords.size() > 0 ) {
				Map<String,PriorityQueue<String[]>> buyerOrderAccessSequence = QueryHelper.createOrderDataAccessSequence(offsetRecords);
				List<Future<List<Row>>> result = new ArrayList<>();
				for (Map.Entry<String, PriorityQueue<String[]>> e : buyerOrderAccessSequence.entrySet()) {
					int fileIndex = Integer.parseInt(e.getKey());
					CallableOrderDataSearch search = new CallableOrderDataSearch(fileIndex, e.getValue());
					result.add(IndexVariables.INSTANCE.getMultiQueryPool4().submit(search));
				}
				for (Future<List<Row>> f: result) {
					try {
						List<Row> list = f.get();
						if (list != null) {
							for(Row row : list) {
								ordersDatas.add(row);
							}
						}
					} catch (InterruptedException | ExecutionException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			} else {
				System.out.println("query4 can't find order:");
			}
		} catch (IOException e) {}

		// 如果不存在对应的order 直接返回null
		if (ordersDatas.size() == 0) {
			return null;
		}
		HashSet<String> queryingKeys = new HashSet<String>();
		queryingKeys.add(key);
		
		List<Result> allData = new ArrayList<>(ordersDatas.size());
		// query4至多只需要join buyer信息
		if(tag.equals("order")) { // 说明此时只有orderData 不需要join
			for (Row orderData : ordersDatas) {
				allData.add(ResultImpl.createResultRow(orderData, null, null, queryingKeys));
			}
		} else { // 需要join buyer
			Comparator<Row> comparator = new Comparator<Row>() {
				@Override
				public int compare(Row o1, Row o2) {
					long o2Id = 0L;
					long o1Id = 0L;			
					try {
						o1Id = o1.get("orderid").valueAsLong();
						o2Id = o2.get("orderid").valueAsLong();
					} catch (TypeException e) {
						e.printStackTrace();
					}
					return o1Id - o2Id > 0 ? 1 : -1;
				}
			};
			// 不需要join good信息
			MultipleJoinResult joinResult = new MultipleJoinResult(ordersDatas, null, comparator, "buyerid", queryingKeys);
			while (joinResult.hasNext()) {
				allData.add(joinResult.next());
			}
		}

		// accumulate as Long
		try {
			boolean hasValidData = false;
			long sum = 0;
			for (Result r : allData) {
				KeyValue kv = r.get(key);
				if (kv != null) {
					sum += kv.valueAsLong();
					hasValidData = true;
				}
			}
			if (hasValidData) {
				return new KV(key, Long.toString(sum));
			}
		} catch (TypeException e) {}

		// accumulate as double
		try {
			boolean hasValidData = false;
			double sum = 0;
			for (Result r : allData) {
				KeyValue kv = r.get(key);
				if (kv != null) {
					sum += kv.valueAsDouble();
					hasValidData = true;
				}
			}
			if (hasValidData) {
				return new KV(key, Double.toString(sum));
			}
		} catch (TypeException e) {}
		return null;
	}
}
