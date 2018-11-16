package com.wangdi.util;

import java.math.BigDecimal;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 11:09 AM 2018/11/14
 */
public class NumberUtils {

    /**
     * 格式化小数
     */
    public static double formatDouble(double num, int scale) {
        BigDecimal bd = new BigDecimal(num);
        return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
