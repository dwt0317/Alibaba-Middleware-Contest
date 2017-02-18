package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;

/**
 * 测试用例类
 */
public class TestCase {
	public static void main(String[] args) throws IOException, InterruptedException {
		// init order system
		List<String> orderFiles = new ArrayList<String>();
		List<String> buyerFiles = new ArrayList<String>();

		List<String> goodFiles = new ArrayList<String>();
		List<String> storeFolders = new ArrayList<String>();

		orderFiles.add("prerun_data\\order.0.0");
		orderFiles.add("prerun_data\\order.1.1");
		orderFiles.add("prerun_data\\order.2.2");
		orderFiles.add("prerun_data\\order.0.3");
		buyerFiles.add("prerun_data\\buyer.0.0");
		buyerFiles.add("prerun_data\\buyer.1.1");
		goodFiles.add("prerun_data\\good.0.0");
		goodFiles.add("prerun_data\\good.1.1");
		goodFiles.add("prerun_data\\good.2.2");
		storeFolders.add("./data2");

		storeFolders.add("./data");
		OrderSystemImpl os = new OrderSystemImpl();
		os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

		// 用例
		if (true){
			long start = System.currentTimeMillis();
			long orderid = 609670049;
			System.out.println("\n查询订单号为" + orderid + "的订单");
			List<String> keys = new ArrayList<>();
			keys.add("description");
			System.out.println(os.queryOrder(orderid, keys));
			System.out.println(System.currentTimeMillis()-start);
			System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
			System.out.println(os.queryOrder(orderid, new ArrayList<String>()));
		}

		if (true){
			long start = System.currentTimeMillis();
			long orderid = 609670049;
			System.out.println("\n查询订单号为" + orderid + "的订单的contactphone, buyerid,foo, done, price字段");
			List<String> queryingKeys = new ArrayList<String>();
			queryingKeys.add("contactphone");
			queryingKeys.add("buyerid");
			queryingKeys.add("foo");
			queryingKeys.add("done");
			queryingKeys.add("price");
			Result result = os.queryOrder(orderid, queryingKeys);
			System.out.println(result);
			System.out.println("\n查询订单号不存在的订单");
			result = os.queryOrder(1111, queryingKeys);
			if (result == null) {
				System.out.println(1111 + " order not exist");
			}
			System.out.println(System.currentTimeMillis() - start);
		}

		if (true){
			long start = System.currentTimeMillis();
			String buyerid = "tp-b0a2-fd0ca6720971";
			long startTime = 1467791748;
			long endTime = 1481816836;		
			Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
			
			System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
			while (it.hasNext()) {
//				System.out.println(it); 
				it.next();
			}
			System.out.println("time:"+(System.currentTimeMillis() - start));
		}

		if (true){
			String goodid = "aye-8837-3aca358bfad3";
			String salerid = "almm-b250-b1880d628b9a";
			System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
			long start = System.currentTimeMillis();
			List<String> keys = new ArrayList<>();
			keys.add("address");
			Iterator<Result> it = os.queryOrdersBySaler(salerid, goodid, keys);
			int count = 0;
			while (it.hasNext()) {
				it.next();
				count++;
			}
			System.out.println(System.currentTimeMillis()-start + "," +count);
		}	
		if (true){
			long start = System.currentTimeMillis();
			String goodid = "dd-a27d-835565dfb080";
			String attr = "a_b_3503";
			System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
			System.out.println(os.sumOrdersByGood(goodid, attr));
			System.out.println(System.currentTimeMillis() -start);
		}	
		if (true){
			String goodid = "good_d191eeeb-fed1-4334-9c77-3ee6d6d66aff";
			String attr = "app_order_33_0";
			System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
			System.out.println(os.sumOrdersByGood(goodid, attr));
			
			attr = "done";
			System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
			KeyValue sum = os.sumOrdersByGood(goodid, attr);
			if (sum == null) {
				System.out.println("由于该字段是布尔类型，返回值是null");
			}
			
			attr = "foo";
			System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
			sum = os.sumOrdersByGood(goodid, attr);
			if (sum == null) {
				System.out.println("由于该字段不存在，返回值是null");
			}
		}
	}
}
