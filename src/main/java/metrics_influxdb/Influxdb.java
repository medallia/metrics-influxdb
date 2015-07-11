package metrics_influxdb;

import io.dropwizard.metrics.MetricName;

public interface Influxdb {
	public void resetRequest();
	public boolean hasSeriesData();
	public long convertTimestamp(long timestamp);
	public void appendSeries(String namePrefix, MetricName name, String nameSuffix, String[] columns, Object[][] points);
	public int sendRequest(boolean throwExc, boolean printJson) throws Exception;
	public void setDebug(boolean debug);
	public void setVersion(INFLUXDB_VERSION version);
	public void setDatabase(String database);
	public void setPrecision(String precision);

	enum INFLUXDB_VERSION{
		ZERO_POINT_EIGHT,ZERO_POINT_NINE;
	}
}
