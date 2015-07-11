package metrics_influxdb;

/**
 * Created by ssurendran on 6/15/15.
 */


import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import io.dropwizard.metrics.MetricName;
import org.joda.time.DateTime;

class JsonBuilderV9 extends JsonBuilderDefault {

    private String metadata;


    public JsonBuilderV9(String database, String precision) {
        metadata = "{\"database\":\"" + database + /*"\",\"precision\":\"" + precision +*/ "\",\"points\":";
    }

    @Override
    public String toJsonString() {
        json.append(']');
        String str = metadata + json.toString() + "}";
        json.setLength(json.length() - 1);
        return str;
    }

    @Override
    public void appendSeries(String namePrefix, MetricName name, String nameSuffix, String[] columns, Object[][] points) {
        if (hasSeriesData)
            json.append(',');
        hasSeriesData = true;
        final DateTime timestamp = new DateTime();
        json.append("{\"measurement\":\"").append(namePrefix).append(name.getKey())
                .append(nameSuffix).append('"');
        String tagString = convertTags(name.getTags());
        if(tagString != null)
            json.append(","+tagString );
        json.append(",\"fields\":{");
        Object[] row = points[0];
        for (int j = 1; j < row.length; j++) {
            if (j > 1)
                json.append(',');
            Object value = row[j];
            json.append("\"" + columns[j] + "\":");
            if (value instanceof String) {
                json.append('"').append(value).append('"');
            } else if ((value instanceof Collection) && ((Collection<?>) value).size() < 1) {
                json.append("null");
            } else if (value instanceof Double && Double.isInfinite((double) value)) {
                json.append("null");
            } else if (value instanceof Float && Float.isInfinite((float) value)) {
                json.append("null");
            } else {
                json.append(value);
            }
        }
        json.append("},");

        json.append("\"time\": \"" + timestamp + "\" }");
    }

    public String convertTags(Map<String, String> tags) {
        String tagString = new String();
        if(tags != null){
            for(Entry<String,String> entry:tags.entrySet())
                tagString += addAround(entry.getKey(),'"') + " : " + addAround(entry.getValue(),'"') +",";
        }
        return tagString.length() > 0?addAround(TAGS,'"') + ":" + addAround(tagString.substring(0,tagString.length()-1),'{','}'):null;
    }

    private String addAround(String tagString, char startChar, char endChar) {
        return startChar + tagString + endChar;
    }

    private String addAround(String str,char c) {
        return c + str + c;
    }

}

