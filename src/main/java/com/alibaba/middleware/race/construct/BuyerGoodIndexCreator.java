package com.alibaba.middleware.race.construct;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.entity.KV;
import com.alibaba.middleware.race.entity.Row;
import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.ExtendBufferedReader;
import com.alibaba.middleware.race.utils.IOUtils;
import com.alibaba.middleware.race.utils.IndexBuffer;
import com.alibaba.middleware.race.utils.StringUtils;


/**
 * buyer和good索引构建类
 * buyer和good放在内存中
 */
public class BuyerGoodIndexCreator implements Runnable {
	private String hashId;
	private Collection<String> files;
	private CountDownLatch latch;
	private final int blockSize;
	private IndexBuffer[] bufferArray;		//读写index的缓冲区，索引构建性能提高约15%
	private HashMap<String,String> goodMemoryIndexMap; 
	private HashMap<String,String> buyerMemoryIndexMap;
	
	public BuyerGoodIndexCreator(String hashId, Collection<String> files, int blockSize, CountDownLatch latch) {
		super();
		this.latch = latch;
		this.hashId = hashId;
		this.files = files;
		this.blockSize = blockSize;
		this.bufferArray = new IndexBuffer[CommonConstants.INDEX_BUFFER_SIZE];
		this.goodMemoryIndexMap = IndexVariables.goodMemoryIndexMap;
		this.buyerMemoryIndexMap = IndexVariables.buyerMemoryIndexMap;
	}

	@Override
	public void run() {
		int fileIndex = 0;
		for(String orderFile : this.files) {
			Row kvMap = null;
			KV orderKV = null;
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
						if (line == null) break;
						
						StringBuilder offSetMsg = new StringBuilder();
						kvMap = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
						length = line.getBytes().length;
						
						// orderId一定存在且为long
						orderKV = kvMap.getKV(hashId);
						offSetMsg.append(fileIndex);
						offSetMsg.append(' ');
						offSetMsg.append(offset);
						offSetMsg.append(' ');
						offSetMsg.append(length);
						this.bufferArray[readLines] = new IndexBuffer(orderKV.valueAsString(), offSetMsg.toString());											
						offset += (length + 1);
					}	
					if (readLines == 0) break;
					int i = 0;
					//将缓冲区的index写入文件
					while (i < readLines) {
						if (this.hashId.equals("goodid")) 								
							goodMemoryIndexMap.put((String)bufferArray[i].getIndex(), bufferArray[i].getLine());
					    else 								
							buyerMemoryIndexMap.put((String)bufferArray[i].getIndex(), bufferArray[i].getLine());
						i++;
					}						
				}	
				fileIndex++;				
			} catch (IOException e) {
				System.out.println("Caught long type error!");
			}
		}
		this.latch.countDown();
	}
}
