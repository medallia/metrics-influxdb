package metrics_influxdb;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by ssurendran on 6/22/15.
 */
public class JsonBuilderV9Test {

    @Test
    public void testConvertTags(){
        JsonBuilderV9 jsonBuilderV9 = new JsonBuilderV9("test","test");
        assertEquals(null,jsonBuilderV9.convertTags(null));
        assertEquals(null, jsonBuilderV9.convertTags(new HashMap<String, String>()));
        Map<String,String> tags = new HashMap<>();
        tags.put("region","uswest");
        tags.put("host","server01");
        assertEquals(replaceWhitespace("\"tags\": {\"host\" : \"server01\",\"region\" : \"uswest\"}"),
                replaceWhitespace(jsonBuilderV9.convertTags(tags)));
    }

    private String replaceWhitespace(String str){
        return str.replaceAll("\\s+","");
    }
}
