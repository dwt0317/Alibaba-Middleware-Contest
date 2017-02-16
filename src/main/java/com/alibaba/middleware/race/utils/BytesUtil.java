package com.alibaba.middleware.race.utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class BytesUtil {
	
	public static String[] getIndexInfo(long orderId,byte[] block){
		try {
			System.out.println(new String(block,"utf-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ByteBuffer bb = ByteBuffer.wrap(block);
		String[] indexArray;
		long tmpId = bb.getLong(); //id
		
		if(tmpId==orderId){
			indexArray = new String[3];
			bb.get();  //:
			indexArray[0] = String.valueOf(bb.getInt());  //file
			bb.get(); // s
			indexArray[1] = String.valueOf(bb.getLong()); //offset
			bb.get(); // s
			indexArray[2] = String.valueOf(bb.getInt()); //length
			return indexArray;
		}else return null;

	}
	
	/** converts an array of four bytes to an integer. */
	private static int bytesToInt(byte[] bytes){
		return (bytes[0]<<24) 
		| ((bytes[1]&0xff)<<16) 
		| ((bytes[2]&0xff)<<8) 
		| (bytes[3]&0xff);
	}
	
	public static void longToBytes(long v,byte[] buf){
		buf[0] = (byte)(v >>> 56);
		buf[1] = (byte)(v >>> 48);
		buf[2] = (byte)(v >>> 40);
		buf[3] = (byte)(v >>> 32);
		buf[4] = (byte)(v >>> 24);
		buf[5] = (byte)(v >>> 16);
		buf[6] = (byte)(v >>>  8);
		buf[7] = (byte)(v >>>  0);
	}
	
	public static long bytesToLong(byte[] b){
        return ( ( (long) b[7]) & 0xFF) +
            ( ( ( (long) b[6]) & 0xFF) << 8) +
            ( ( ( (long) b[5]) & 0xFF) << 16) +
            ( ( ( (long) b[4]) & 0xFF) << 24) +
            ( ( ( (long) b[3]) & 0xFF) << 32) +
            ( ( ( (long) b[2]) & 0xFF) << 40) +
            ( ( ( (long) b[1]) & 0xFF) << 48) +
            ( ( ( (long) b[0]) & 0xFF) << 56);
    }
	
	public static byte[] intToBytes(int number){
		byte[] ans = new byte[4];
		ans[0] = (byte) (number>>>24);
		ans[1] = (byte) (number>>>16 & 0xff);
		ans[2] = (byte) (number>>>8 & 0xff);
		ans[3] = (byte) (number & 0xff);
		return ans;
	}
}
