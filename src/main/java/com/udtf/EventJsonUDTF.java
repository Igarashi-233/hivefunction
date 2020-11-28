package com.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    @Deprecated
    public StructObjectInspector initialize(ObjectInspector[] argOIs) {
        List<ObjectInspector> fieldsType = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        fieldNames.add("event_Name");
        fieldsType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("event_Json");
        fieldsType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldsType);
    }

    @Override
    public void process(Object[] args) throws HiveException {

        String input = args[0].toString();

        if (StringUtils.isBlank(input)) {
            return;
        } else {
            try {
                JSONArray jsonArray = new JSONArray(input);

                if (jsonArray == null)
                    return;

                for (int i = 0; i < jsonArray.length(); i++) {

                    String[] results = new String[2];

                    try {
                        results[0] = jsonArray.getJSONObject(i).getString("en");
                        results[1] = jsonArray.getString(i);
                    } catch (JSONException e) {
                        e.printStackTrace();
                        continue;
                    }
                    forward(results);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
