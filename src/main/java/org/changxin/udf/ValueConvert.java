package org.changxin.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.HashMap;
import java.util.Map;


@Description(
        name = "value_convert",
        value = "input a value is one element,result is number",
        extended = "select value_convert(); result is number"
)
public class ValueConvert extends GenericUDF {

    private static Map<String, String> cnNumToArabicNumMap;

    static {
        cnNumToArabicNumMap = new HashMap<>();
        cnNumToArabicNumMap.put("零", "0");
        cnNumToArabicNumMap.put("壹", "1");
        cnNumToArabicNumMap.put("一", "1");
        cnNumToArabicNumMap.put("贰", "2");
        cnNumToArabicNumMap.put("貳", "2");
        cnNumToArabicNumMap.put("两", "2");
        cnNumToArabicNumMap.put("二","2");
        cnNumToArabicNumMap.put("叁", "3");
        cnNumToArabicNumMap.put("三", "3");
        cnNumToArabicNumMap.put("肆", "4");
        cnNumToArabicNumMap.put("四", "4");
        cnNumToArabicNumMap.put("伍", "5");
        cnNumToArabicNumMap.put("五", "5");
        cnNumToArabicNumMap.put("陆", "6");
        cnNumToArabicNumMap.put("六", "6");
        cnNumToArabicNumMap.put("柒", "7");
        cnNumToArabicNumMap.put("七", "7");
        cnNumToArabicNumMap.put("捌", "8");
        cnNumToArabicNumMap.put("八", "8");
        cnNumToArabicNumMap.put("玖", "9");
        cnNumToArabicNumMap.put("九", "9");
    }

    /**
     * 将大写金额转换为字符串形式的阿拉伯数字。
     * 支持小于一万亿的数字，可精确到小数点后两位
     * 如果大写金额的格式错误，本函数不保证能范围正确的结果（废话..）
     */
    public static String formatAmount(String cnTraditionalNum) {
        //规范错误字符
        cnTraditionalNum = cnTraditionalNum
                .replaceAll("億", "亿")
                .replaceAll("萬", "万")
                .replaceAll("千", "仟")
                .replaceAll("百", "佰")
                .replaceAll("元", "圆")
                .replaceAll("圓", "圆")
                .replaceAll("角", "角")
                .replaceAll("分", "分")
        ;

        long textToNum = 0;
        String result = "";

        if (StringUtils.isNotBlank(cnTraditionalNum)){
            if (cnTraditionalNum.contains("亿")) {
                String yi = cnTraditionalNum.substring(0, cnTraditionalNum.indexOf("亿"));
                textToNum += cnThousandNumberToArab(yi) * 100000000L;
                cnTraditionalNum = cnTraditionalNum.substring(cnTraditionalNum.indexOf("亿"));
            }

            if (cnTraditionalNum.contains("万")) {
                String wan = cnTraditionalNum.substring(0, cnTraditionalNum.indexOf("万"));
                textToNum += cnThousandNumberToArab(wan) * 10000L;
                cnTraditionalNum = cnTraditionalNum.substring(cnTraditionalNum.indexOf("万"));
            }

            if (cnTraditionalNum.contains("圆")) {
                String wan = cnTraditionalNum.substring(0, cnTraditionalNum.indexOf("圆"));
                textToNum += cnThousandNumberToArab(wan);
                cnTraditionalNum = cnTraditionalNum.substring(cnTraditionalNum.indexOf("圆"));
            }

            if (cnTraditionalNum.contains("仟")) {
                String wan = cnTraditionalNum.substring(0, cnTraditionalNum.indexOf("仟"));
                textToNum += cnThousandNumberToArab(wan) * 1000L;
                cnTraditionalNum = cnTraditionalNum.substring(cnTraditionalNum.indexOf("仟"));
            }

            if (cnTraditionalNum.contains("佰")) {
                String wan = cnTraditionalNum.substring(0, cnTraditionalNum.indexOf("佰"));
                textToNum += cnThousandNumberToArab(wan) * 100L;
                cnTraditionalNum = cnTraditionalNum.substring(cnTraditionalNum.indexOf("佰"));
            }

            if (cnTraditionalNum.contains("拾")) {
                String wan = cnTraditionalNum.substring(0, cnTraditionalNum.indexOf("拾"));
                textToNum += cnThousandNumberToArab(wan) * 10L;
                cnTraditionalNum = cnTraditionalNum.substring(cnTraditionalNum.indexOf("拾"));
            }

            //小数点后
            String jiao = "0";
            String fen = "0";
            if (cnTraditionalNum.contains("角")) {
                jiao = cnNumToArabicNumMap.get(cnTraditionalNum.substring(cnTraditionalNum.indexOf("角") - 1, cnTraditionalNum.indexOf("角"))).toString();
            }
            if (cnTraditionalNum.contains("分")) {
                fen = cnNumToArabicNumMap.get(cnTraditionalNum.substring(cnTraditionalNum.indexOf("分") - 1, cnTraditionalNum.indexOf("分"))).toString();
            }

            result = String.valueOf(textToNum);
            result += "." + jiao + fen;
        }
        if (result.equalsIgnoreCase("0.00")){
            result=cnTraditionalNum;
        }

        System.out.println("formatAmount-------------"+ textToNum);
        return result;
    }


    /**
     * 将大写数字转换为阿拉伯数字（数字小于一万）
     */
    private static long cnThousandNumberToArab(String number) {
        int result = 0;

        if (number.contains("仟")) {
            result += Integer.parseInt(getArabicValue(String.valueOf(number.charAt(number.indexOf("仟") - 1)))) * 1000;
        }
        if (number.contains("佰")) {
            result += Integer.valueOf(getArabicValue(String.valueOf(number.charAt(number.indexOf("佰") - 1)))) * 100;
        }
        if (number.contains("拾")) {
            result += Integer.valueOf(getArabicValue(String.valueOf(number.charAt(number.indexOf("拾") - 1)))) * 10;
        }
        if (cnNumToArabicNumMap.containsKey(number.substring(number.length() - 1))) {
            result += Integer.valueOf(cnNumToArabicNumMap.get(number.substring(number.length() - 1)));
        }
        return result;
    }

    private static String getArabicValue(String num) {
        String result = cnNumToArabicNumMap.get(num.toString());
        if (result == null){
            return num.toString();
        }else {
            return result;
        }

    }



    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 0) {
            throw new UDFArgumentLengthException("args cannot be empty");
        }
        // 定义函数的返回类型为java的String
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        String value = arguments[0].get().toString();
        String amount = formatAmount(value);
        return amount;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Usage: value_convert(String str)";
    }

    public static void main(String[] args) {

//        System.out.println(formatAmount("三万叁仟零肆拾伍亿零贰佰萬柒仟陆佰零伍万"));
        String t = "三万一仟九佰零肆拾伍亿零贰佰萬柒仟陆佰零伍万";

        System.out.println(t.indexOf("佰")-1);

    }
}
