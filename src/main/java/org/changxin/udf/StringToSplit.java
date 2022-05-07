package org.changxin.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * create temporary function mysplit AS 'org.changxin.udf.Mysplit2' USING JAR 'hdfs://changxin-160:8020/algorithm/hive-changxin-udf-1.0.jar';
 *
 */

@Description(name = "to_split",
        value = "_FUNC_(<data_string>) - Returns a list by split string use character ',' ",
        extended = "Example:\n >SELECT _FUNC_('Hello,world,tang') FROM src LIMIT 1;\n  [\"hello\",\"world\",\"tang\"]")
public class StringToSplit extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 定义函数的返回类型为java的List
        ObjectInspector returnOI = PrimitiveObjectInspectorFactory
                .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
        return ObjectInspectorFactory.getStandardListObjectInspector(returnOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException(" arguments length must be equal to 1");
        }

        String str = arguments[0].get().toString();
        String[] s = str.split(",", -1);

        return new ArrayList<String>(Arrays.asList(s));
    }

    @Override
    public String getDisplayString(String[] children) {
        return "Usage: mysplit(String str)";
    }
}
