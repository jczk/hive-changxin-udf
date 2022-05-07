package org.changxin.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.math.BigDecimal;
import java.util.Objects;

import static org.apache.commons.lang.StringUtils.isBlank;

@Description(
        name = "word_number",
        value = "input a value is one element,result is number",
        extended = "select word_number(); result is number"
)
public class WordToNumber extends GenericUDF {


    /**
     * 中文数字
     */
    private static char[] cnArr_a = new char[]{'零', '一', '二', '三', '四', '五', '六', '七', '八', '九'};
    private static char[] cnArr_A = new char[]{'零', '壹', '贰', '叁', '肆', '伍', '陆', '柒', '捌', '玖'};
    private static final String allChineseNum = "零一二三四五六七八九壹贰叁肆伍陆柒捌玖十拾百佰千仟万萬亿";

    /**
     * 中文单位
     */
    private static char[] unit_a = new char[]{'亿', '万', '千', '百', '十'};
    private static char[] unit_A = new char[]{'亿', '萬', '仟', '佰', '拾'};
    private static final String allChineseUnit = "十拾百佰千仟万萬亿";

    /**
     * 将汉字中的数字转换为阿拉伯数字
     * (例如：三万叁仟零肆拾伍亿零贰佰萬柒仟陆佰零伍)
     *
     * @param chineseNum;
     * @return long
     */
    public static BigDecimal chineseNumToArabicNum(String chineseNum) {

        BigDecimal result = new BigDecimal(0);// 最终返回的结果

        if (chineseNum == null || chineseNum.trim().length() == 0) {
            return result;
        }

        char lastUnit = chineseNum.charAt(chineseNum.length() - 1);
        // 默认:1
        long lastUnitNum = 1;
        if (isCN_Unit(lastUnit)) {
            chineseNum = chineseNum.substring(0, chineseNum.length() - 1);
            lastUnitNum = chnNameValue[ChnUnitToValue(String.valueOf(lastUnit))].value;
        }

        // 将小写数字转为大写数字
        for (int i = 0; i < cnArr_a.length; i++) {
            chineseNum = chineseNum.replaceAll(String.valueOf(cnArr_a[i]), String.valueOf(cnArr_A[i]));
        }
        // 将小写单位转为大写单位
        for (int i = 0; i < unit_a.length; i++) {
            chineseNum = chineseNum.replaceAll(String.valueOf(unit_a[i]), String.valueOf(unit_A[i]));
        }
        // System.out.println(">>> 大写数字：" + chineseNum);

        for (int i = 0; i < unit_A.length; i++) {
            if (chineseNum.trim().length() == 0) {
                break;
            }
            String unitUpperCase = String.valueOf(unit_A[i]);
            String str = null;
            if (chineseNum.contains(unitUpperCase)) {
                str = chineseNum.substring(0, chineseNum.lastIndexOf(unitUpperCase) + 1);
            }
            if (str != null && str.trim().length() > 0) {
                // 下次循环截取的基础字符串
                chineseNum = chineseNum.replaceAll(str, "");
                // System.out.println(">>> 本次循环待处理中文数字: " + str);
                // 单位基础值
                long unitNum = chnNameValue[ChnUnitToValue(unitUpperCase)].value;
                String temp = str.substring(0, str.length() - 1);
                long number = ChnStringToNumber(temp);
                result = result.add(BigDecimal.valueOf(number).multiply(BigDecimal.valueOf(unitNum)));
            }
            // 最后一次循环，被传入的数字没有处理完并且没有单位的个位数处理
            if (i + 1 == unit_a.length && !chineseNum.equals("")) {
                // System.out.println(">>> 个位数：" + chineseNum);
                long number = ChnStringToNumber(chineseNum);
                result = result.add(BigDecimal.valueOf(number));
            }
        }
        // 加上单位
        if (lastUnitNum > 1) {
            result = result.multiply(BigDecimal.valueOf(lastUnitNum));
        }

        return result;
    }

    /**
     * 返回中文数字汉字所对应的阿拉伯数字，若str不为中文数字，则返回-1
     *
     * @param string;
     * @return int
     */
    private static int strToNum(String string) {
        for (int i = 0; i < cnArr_a.length; i++) {
            if (Objects.equals(string, String.valueOf(cnArr_a[i])) || Objects.equals(string, String.valueOf(cnArr_A[i]))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 判断传入的字符串是否全是汉字数字和单位
     *
     * @param chineseStr;
     * @return boolean
     */
    public static boolean isCN_Num_All(String chineseStr) {
        if (isBlank(chineseStr)) {
            return true;
        }
        char[] charArray = chineseStr.toCharArray();
        for (char c : charArray) {
            if (!allChineseNum.contains(String.valueOf(c))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 判断传入的字符是否是汉字数字和单位
     *
     * @param chineseChar;
     * @return boolean
     */
    public static boolean isCN_Num(char chineseChar) {
        if (!allChineseNum.contains(String.valueOf(chineseChar))) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * 判断是否是中文单位
     *
     * @param unitStr;
     * @return boolean
     */
    public static boolean isCN_Unit(char unitStr) {
        if (!allChineseUnit.contains(String.valueOf(unitStr))) {
            return false;
        } else {
            return true;
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
    public String evaluate(DeferredObject[] arguments) throws HiveException {

        String text = arguments[0].get().toString();
        String number = "";

        boolean flag = true;
        for (int i = 0; i < text.length(); i++) {
            boolean cn_num = isCN_Num(text.toCharArray()[i]);

            flag = flag && cn_num;

            if (i == (text.length() - 1) && flag) {
                number = chineseNumToArabicNum(text).toString();
            } else if (!flag) {
                number = text;
                break;
            }
        }

        return number;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Usage: word_number(String str)";
    }

    /**
     * 中文转换成阿拉伯数字，中文字符串除了包括0-9的中文汉字，还包括十，百，千，万等权位。
     * 此处是完成对这些权位的类型定义。
     * name是指这些权位的汉字字符串。
     * value是指权位多对应的数值的大小。诸如：十对应的值的大小为10，百对应为100等
     * secUnit若为true，代表该权位为节权位，即万，亿，万亿等
     */
    public static class Chn_Name_value {
        String name;
        long value;
        Boolean secUnit;

        Chn_Name_value(String name, long value, Boolean secUnit) {
            this.name = name;
            this.value = value;
            this.secUnit = secUnit;
        }
    }

    static Chn_Name_value chnNameValue[] = {
            new Chn_Name_value("十", 10, false),
            new Chn_Name_value("拾", 10, false),
            new Chn_Name_value("百", 100, false),
            new Chn_Name_value("佰", 100, false),
            new Chn_Name_value("千", 1000, false),
            new Chn_Name_value("仟", 1000, false),
            new Chn_Name_value("万", 10000, true),
            new Chn_Name_value("萬", 10000, true),
            new Chn_Name_value("亿", 100000000, true)
    };

    /**
     * 返回中文汉字权位在chnNameValue数组中所对应的索引号，若不为中文汉字权位，则返回-1
     *
     * @param str;
     * @return int
     */
    private static int ChnUnitToValue(String str) {
        for (int i = 0; i < chnNameValue.length; i++) {
            if (str.equals(chnNameValue[i].name)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 返回中文数字字符串所对应的int类型的阿拉伯数字
     * (千亿/12位数)
     *
     * @param str;
     * @return long
     */
    private static long ChnStringToNumber(String str) {
        long returnNumber = 0;
        long section = 0;
        int index = 0;
        long number = 0;
        while (index < str.length()) {
            // 从左向右依次获取对应中文数字，取不到返回-1
            int num = strToNum(str.substring(index, index + 1));
            //若num>=0，代表该位置（pos），所对应的是数字不是权位。若小于0，则表示为权位
            if (num >= 0) {
                number = num;
                index++;
                //pos是最后一位，直接将number加入到section中。
                if (index >= str.length()) {
                    section += number;
                    returnNumber += section;
                    break;
                }
            } else {
                int chnNameValueIndex = ChnUnitToValue(str.substring(index, index + 1));
                //chnNameValue[chnNameValueIndex].secUnit==true，表示该位置所对应的权位是节权位，
                if (chnNameValue[chnNameValueIndex].secUnit) {
                    section = (section + number) * chnNameValue[chnNameValueIndex].value;
                    returnNumber += section;
                    section = 0;
                } else {
                    section += number * chnNameValue[chnNameValueIndex].value;
                }
                index++;
                number = 0;
                if (index >= str.length()) {
                    returnNumber += section;
                    break;
                }
            }
        }
        return returnNumber;
    }

    public static void main(String[] args) {
        String str2 = "七兆叁亿五万7千六百零五";

        String str = "三万叁仟零肆拾伍亿零贰佰萬柒仟陆佰零伍万";
        System.out.println(">>> " + str + " : " + chineseNumToArabicNum(str));

        boolean t = true;
        for (int i = 0; i < str2.length(); i++) {
            boolean cn_num = isCN_Num(str2.toCharArray()[i]);

            t = t && cn_num;

            if (i == (str2.length() - 1) && t) {
                System.out.println(">>> " + str2 + " : " + chineseNumToArabicNum(str2));
            } else if (!t) {
                System.out.println(str2);
                break;

            }

        }


    }

}
