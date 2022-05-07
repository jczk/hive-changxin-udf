package org.changxin.utils;

import java.math.BigDecimal;

/**
 * 金额转换，阿拉伯数字的金额转换成中国传统大写汉字的形式
 */
public class DecodeWordToNumberString {

    private static final int DFT_SCALE = 2;

    /** 大写数字 */
    private static final String[] NUMBERS = { "零", "壹", "贰", "叁", "肆", "伍","陆", "柒", "捌", "玖" };
    /** 整数部分的单位 */
    private static final String[] IUNIT = { "元", "拾", "佰", "仟", "万", "拾", "佰","仟", "亿", "拾", "佰", "仟", "万", "拾", "佰", "仟" };
    /** 小数部分的单位 */
    private static final String[] DUNIT = { "角", "分", "厘" };

    /**
     * 得到大写金额。
     */
    public static String toChinese(String str) {
        str = str.replaceAll(",", "");// 去掉","
        String integerStr;// 整数部分数字
        String decimalStr;// 小数部分数字

        // 初始化：分离整数部分和小数部分
        if (str.indexOf(".") > 0) {
            integerStr = str.substring(0, str.indexOf("."));
            decimalStr = str.substring(str.indexOf(".") + 1);
        } else if (str.indexOf(".") == 0) {
            integerStr = "";
            decimalStr = str.substring(1);
        } else {
            integerStr = str;
            decimalStr = "";
        }
        // integerStr去掉首0，不必去掉decimalStr的尾0(超出部分舍去)
        if (!integerStr.equals("")) {
            integerStr = Long.toString(Long.parseLong(integerStr));
            if (integerStr.equals("0")) {
                integerStr = "";
            }
        }
        // overflow超出处理能力，直接返回
        if (integerStr.length() > IUNIT.length) {
            System.out.println(str + ":超出处理能力");
            return str;
        }

        int[] integers = toArray(integerStr);// 整数部分数字
        boolean isMust5 = isMust5(integerStr);// 设置万单位
        int[] decimals = toArray(decimalStr);// 小数部分数字
        return getChineseInteger(integers, isMust5)
                + getChineseDecimal(decimals);
    }

    /**
     * 整数部分和小数部分转换为数组，从高位至低位
     */
    private static int[] toArray(String number) {
        int[] array = new int[number.length()];
        for (int i = 0; i < number.length(); i++) {
            array[i] = Integer.parseInt(number.substring(i, i + 1));
        }
        return array;
    }

    /**
     * 得到中文金额的整数部分。
     */
    private static String getChineseInteger(int[] integers, boolean isMust5) {
        StringBuffer chineseInteger = new StringBuffer("");
        int length = integers.length;
        for (int i = 0; i < length; i++) {
            // 0出现在关键位置：1234(万)5678(亿)9012(万)3456(元)
            // 特殊情况：10(拾元、壹拾元、壹拾万元、拾万元)
            String key = "";
            if (integers[i] == 0) {
                if ((length - i) == 13)// 万(亿)(必填)
                    key = IUNIT[4];
                else if ((length - i) == 9)// 亿(必填)
                    key = IUNIT[8];
                else if ((length - i) == 5 && isMust5)// 万(不必填)
                    key = IUNIT[4];
                else if ((length - i) == 1)// 元(必填)
                    key = IUNIT[0];
                // 0遇非0时补零，不包含最后一位
                if ((length - i) > 1 && integers[i + 1] != 0)
                    key += NUMBERS[0];
            }
            chineseInteger.append(integers[i] == 0 ? key
                    : (NUMBERS[integers[i]] + IUNIT[length - i - 1]));
        }
        return chineseInteger.toString();
    }

    /**
     * 得到中文金额的小数部分。
     */
    private static String getChineseDecimal(int[] decimals) {
        StringBuffer chineseDecimal = new StringBuffer("");
        for (int i = 0; i < decimals.length; i++) {
            // 舍去3位小数之后的
            if (i == 3)
                break;
            chineseDecimal.append(decimals[i] == 0 ? ""
                    : (NUMBERS[decimals[i]] + DUNIT[i]));
        }
        return chineseDecimal.toString();
    }

    /**
     * 判断第5位数字的单位"万"是否应加。
     */
    private static boolean isMust5(String integerStr) {
        int length = integerStr.length();
        if (length > 4) {
            String subInteger = "";
            if (length > 8) { // TODO 12-9-17
                // 取得从低位数，第5到第8位的字串
                subInteger = integerStr.substring(length - 8, length - 4);
            } else {
                subInteger = integerStr.substring(0, length - 4);
            }
            return Integer.parseInt(subInteger) > 0;
        } else {
            return false;
        }
    }

    /**
     * BigDecimal 相乘,四舍五入保留0位
     *
     * @param a
     * @param b
     * @return a*b
     */
    public static BigDecimal mutiply(String a, String b, int roundingMode) {
        BigDecimal bd = new BigDecimal(a);
        return bd.multiply(new BigDecimal(b)).setScale(DFT_SCALE, roundingMode);
    }

    /**
     * BigDecimal 相除,四舍五入保留两位
     *
     * @param a
     * @param b
     * @return a/b
     */
    public static BigDecimal div(String a, String b, int roundingMode) {
        BigDecimal decimal1 = new BigDecimal(a);
        BigDecimal decimal2 = new BigDecimal(b);
        return decimal1.divide(decimal2, DFT_SCALE, roundingMode);
    }

    /**
     * BigDecimal 相加,四舍五入保留两位
     *
     * @param a
     * @param b
     * @return a+b
     */
    public static BigDecimal sum(String a, String b, int roundingMode) {
        BigDecimal decimal1 = new BigDecimal(a);
        BigDecimal decimal2 = new BigDecimal(b);
        // DecimalFormat format = new DecimalFormat("#0.00");
        return decimal1.add(decimal2).setScale(DFT_SCALE, roundingMode);
    }

    /**
     * BigDecimal 相减,四舍五入保留两位
     *
     * @param a
     * @param b
     * @return a+b
     */
    public static BigDecimal sub(String a, String b, int roundingMode) {
        BigDecimal decimal1 = new BigDecimal(a);
        BigDecimal decimal2 = new BigDecimal(b);
        // DecimalFormat format = new DecimalFormat("#0.00");
        return decimal1.subtract(decimal2).setScale(DFT_SCALE, roundingMode);
    }

    /**
     * 100.00 为10000
     *
     * @param a
     * @return
     */
    public static BigDecimal format(String a, int roundingMode) {
        return new BigDecimal(a).multiply(new BigDecimal(100)).setScale(0,
                roundingMode);
    }

    public static void main(String[] args) {
        String number = "9876543210.6";
        System.out.println(number + " " + DecodeWordToNumberString.toChinese(number));
        number = "32,323,230,200,000,1";
        System.out.println(number + " " + DecodeWordToNumberString.toChinese(number));
        number = "30000.05";
        System.out.println(number + " " + DecodeWordToNumberString.toChinese(number));
        number = "30000.00";
        System.out.println(number + " " + DecodeWordToNumberString.toChinese(number));
    }

}
