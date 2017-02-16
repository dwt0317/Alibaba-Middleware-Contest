package com.alibaba.middleware.race.utils;

public class HashUtil {
	public static int hashWithDistrub(Object k) {
        int h = 0;
        h ^= k.hashCode();
        // This function ensures that hashCodes that differ only by
        // constant multiples at each bit position have a bounded
        // number of collisions (approximately 8 at default load factor).
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
//		return k.hashCode();    
	}
	
	
	public static int indexFor(int hashCode,int length) {
		return hashCode & (length - 1);
	}
}
