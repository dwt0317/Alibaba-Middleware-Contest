package com.alibaba.middleware.race.construct;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.ExtendBufferedWriter;
import com.alibaba.middleware.race.utils.IOUtils;

public class ConstructHelper {
	
	public static void constructHashIndex() {
		// 5个线程各自完成之后 该函数才能返回
		CountDownLatch latch = new CountDownLatch(5);
		new Thread(new OrderIndexCreator("orderid", Globals.query1IndexWriters, Globals.query1LineRecords,Globals.orderFiles, CommonConstants.QUERY1_ORDER_SPLIT_SIZE,
				CommonConstants.ORDERFILE_BLOCK_SIZE, latch,new String[]{"orderid"})).start();
		new Thread(new OrderIndexCreator("buyerid", Globals.query2IndexWriters, Globals.query2LineRecords, Globals.orderFiles, CommonConstants.QUERY2_ORDER_SPLIT_SIZE,
				CommonConstants.ORDERFILE_BLOCK_SIZE, latch,new String[]{"buyerid","createtime"})).start();
		new Thread(new OrderIndexCreator("goodid", Globals.query3IndexWriters, Globals.query3LineRecords ,Globals.orderFiles, CommonConstants.QUERY3_ORDER_SPLIT_SIZE,
				CommonConstants.ORDERFILE_BLOCK_SIZE, latch, new String[]{"goodid"})).start();

		new Thread(new BuyerGoodIndexCreator("buyerid" ,Globals.buyerFiles, CommonConstants.OTHERFILE_BLOCK_SIZE, latch)).start();
		new Thread(new BuyerGoodIndexCreator("goodid", Globals.goodFiles, CommonConstants.OTHERFILE_BLOCK_SIZE, latch)).start();
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	

	/**
	 * 创建构造索引的writer
	 */
	public static void constructWriterForIndexFile() {
		// 创建4种查询的4中索引文件和买家 商品信息的writer
		Globals.query1IndexWriters = new ExtendBufferedWriter[CommonConstants.QUERY1_ORDER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.QUERY1_ORDER_SPLIT_SIZE; i++) {
			try {
				String file = Globals.query1Path + File.separator + i + CommonConstants.INDEX_SUFFIX;
				RandomAccessFile ran = new RandomAccessFile(file, "rw");
				ran.setLength(1024 * 1024 * 80);
				ran.close();
				Globals.query1IndexWriters[i] = IOUtils.createWriter(file, CommonConstants.INDEX_BLOCK_SIZE);
			} catch (IOException e) {

			}
		}

		Globals.query2IndexWriters = new ExtendBufferedWriter[CommonConstants.QUERY2_ORDER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.QUERY2_ORDER_SPLIT_SIZE; i++) {
			try {
				String file = Globals.query2Path + File.separator + i + CommonConstants.INDEX_SUFFIX;
				RandomAccessFile ran = new RandomAccessFile(file, "rw");
				ran.setLength(1024 * 1024 * 80);
				ran.close();
				Globals.query2IndexWriters[i] = IOUtils.createWriter(file, CommonConstants.INDEX_BLOCK_SIZE);
			} catch (IOException e) {

			}
		}
		
		Globals.query3IndexWriters = new ExtendBufferedWriter[CommonConstants.QUERY3_ORDER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.QUERY3_ORDER_SPLIT_SIZE; i++) {
			try {
				String file = Globals.query3Path + File.separator + i + CommonConstants.INDEX_SUFFIX;
				RandomAccessFile ran = new RandomAccessFile(file, "rw");
				ran.setLength(1024 * 1024 * 80);
				ran.close();
				Globals.query3IndexWriters[i] = IOUtils.createWriter(file, CommonConstants.INDEX_BLOCK_SIZE);
			} catch (IOException e) {

			}
		}
	}
	
	/**
	 * 索引创建目录
	 * @param storeFolders
	 */
	public static void constructDir(Collection<String> storeFolders) {
		List<String> storeFoldersList = new ArrayList<>(storeFolders);
		
		// 4种查询的3种索引文件和买家、商品信息平均分到不同的路径上
		int len = storeFoldersList.size();
		int storeIndex = 0;

		Globals.query1Path = storeFoldersList.get(storeIndex) + File.separator + CommonConstants.QUERY1_PREFIX;
		File query1File = new File(Globals.query1Path);
		if (!query1File.exists()) {
			query1File.mkdirs();
		}
		storeIndex++;
		storeIndex %= len;

		Globals.query2Path = storeFoldersList.get(storeIndex) + File.separator + CommonConstants.QUERY2_PREFIX;
		File query2File = new File(Globals.query2Path);
		if (!query2File.exists()) {
			query2File.mkdirs();
		}
		storeIndex++;
		storeIndex %= len;

		Globals.query3Path = storeFoldersList.get(storeIndex) + File.separator + CommonConstants.QUERY3_PREFIX;
		File query3File = new File(Globals.query3Path);
		if (!query3File.exists()) {
			query3File.mkdirs();
		}
		storeIndex++;
		storeIndex %= len;
	}
	
	public static void closeWriter() {
		try {
			for (ExtendBufferedWriter bw : Globals.query1IndexWriters) {
				bw.close();
			}

			for (ExtendBufferedWriter bw : Globals.query2IndexWriters) {
				bw.close();
			}

			for (ExtendBufferedWriter bw : Globals.query3IndexWriters) {
				bw.close();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
