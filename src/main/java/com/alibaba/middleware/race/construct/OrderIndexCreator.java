package com.alibaba.middleware.race.construct;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.alibaba.middleware.race.entity.KV;
import com.alibaba.middleware.race.entity.Row;
import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.ExtendBufferedReader;
import com.alibaba.middleware.race.utils.ExtendBufferedWriter;
import com.alibaba.middleware.race.utils.HashUtil;
import com.alibaba.middleware.race.utils.IOUtils;
import com.alibaba.middleware.race.utils.IndexBuffer;
import com.alibaba.middleware.race.utils.StringUtils;

/**
 * 构建order信息索引
 */
public class OrderIndexCreator implements Runnable{
	private String hashId;
	private ExtendBufferedWriter[] offSetwriters;
	private int[] indexLineRecords;
	private Collection<String> files;
	private CountDownLatch latch;
	private final int bucketSize;
	private final int blockSize;
	private String[] identities;
	private IndexBuffer[] bufferArray;		//读写index的缓冲区，索引构建性能提高约15%
	
	public OrderIndexCreator(String hashId, ExtendBufferedWriter[] offsetWriters,
			int[] indexLineRecords, Collection<String> files, int bucketSize, int blockSize, CountDownLatch latch, String[] identities) {
		super();
		this.latch = latch;
		this.hashId = hashId;
		this.offSetwriters = offsetWriters;
		this.files = files;
		this.indexLineRecords = indexLineRecords;
		this.bucketSize = bucketSize;
		this.blockSize = blockSize;
		this.identities = identities;
		this.bufferArray = new IndexBuffer[CommonConstants.INDEX_BUFFER_SIZE];
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		int fileIndex = 0;
		for(String orderFile : this.files) {
			Row kvMap = null;
			KV orderKV = null;
			int index;
			ExtendBufferedWriter offsetBw;
			// 记录当前行的偏移
			long offset = 0L;
			// 记录当前行的总长度
			int length = 0;
			int readLines;
			try (ExtendBufferedReader reader = IOUtils.createReader(orderFile, blockSize)) {
				String line;
				while (true) {
					for (readLines = 0; readLines < CommonConstants.INDEX_BUFFER_SIZE; readLines++) {
						line = reader.readLine();
						if (line == null) {
							break;
						}
						StringBuilder offSetMsg = new StringBuilder();
						kvMap = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
						length = line.getBytes().length;
						
						// orderId一定存在且为long
						orderKV = kvMap.getKV(hashId);					
						index = HashUtil.indexFor(hashId.equals("orderid") ? orderKV.valueAsLong() : orderKV.valueAsString(),
								bucketSize);
						
						for(String e : identities) {
							offSetMsg.append(kvMap.getKV(e).valueAsString());	//time是定长，直接append
						}
						offSetMsg.append(':');
						// 写入文件的index
						offSetMsg.append(fileIndex);
						offSetMsg.append(' ');
						offSetMsg.append(offset);
						offSetMsg.append(' ');
						offSetMsg.append(length);
						
						// 将对应index文件的行记录数++ 如果超过阈值则换行并清空
						this.indexLineRecords[index]++;
						if ( this.indexLineRecords[index] == CommonConstants.INDEX_LINE_RECORDS) {
							offSetMsg.append('\n');
							this.indexLineRecords[index] = 0;
						}
						this.bufferArray[readLines] = new IndexBuffer(index, offSetMsg.toString());										
						offset += (length + 1);
					}
					
					if (readLines == 0) break;
					int i = 0;
					//将缓冲区的index写入文件
					while (i < readLines) {			
						offsetBw = offSetwriters[(int)(bufferArray[i].getIndex())];
						offsetBw.write(bufferArray[i].getLine());
						i++;
					}		
				}	
				fileIndex++;				
			} catch (IOException | TypeException e) {
				System.out.println("Caught long type error!");
			}
		}
		this.latch.countDown();
	}
}
