//	metrics-influxdb
//
//	Written in 2014 by David Bernard <dbernard@novaquark.com>
//
//	[other author/contributor lines as appropriate]
//
//	To the extent possible under law, the author(s) have dedicated all copyright and
//	related and neighboring rights to this software to the public domain worldwide.
//	This software is distributed without any warranty.
//
//	You should have received a copy of the CC0 Public Domain Dedication along with
//	this software. If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
package metrics_influxdb;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.dropwizard.metrics.MetricName;
import metrics_influxdb.Influxdb.INFLUXDB_VERSION;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.metrics.Clock;
import io.dropwizard.metrics.Counter;
import io.dropwizard.metrics.Counting;
import io.dropwizard.metrics.Gauge;
import io.dropwizard.metrics.Histogram;
import io.dropwizard.metrics.Meter;
import io.dropwizard.metrics.Metered;
import io.dropwizard.metrics.MetricFilter;
import io.dropwizard.metrics.MetricRegistry;
import io.dropwizard.metrics.ScheduledReporter;
import io.dropwizard.metrics.Snapshot;
import io.dropwizard.metrics.Timer;


/**
 * A reporter which publishes metric values to a InfluxDB server.
 *
 * @see <a href="http://influxdb.org/">InfluxDB - An open-source distributed
 *      time series database with no external dependencies.</a>
 */
public class InfluxdbReporter extends ScheduledReporter {
	private static String[] COLUMNS_TIMER = {
		"time", "count"
		, "min", "max", "mean", "std-dev"
		, "50-percentile", "75-percentile", "95-percentile", "99-percentile", "999-percentile"
		, "one-minute", "five-minute", "fifteen-minute", "mean-rate"
		, "run-count"
	};
	private static String[] COLUMNS_HISTOGRAM = {
		"time", "count"
		, "min", "max", "mean", "std-dev"
		, "50-percentile", "75-percentile", "95-percentile", "99-percentile", "999-percentile"
		, "run-count"
	};
	private static String[] COLUMNS_COUNT = {
		"time", "count"
	};
	private static String[] COLUMNS_GAUGE = {
		"time", "value"
	};
	private static String[] COLUMNS_METER = {
		"time", "count"
		, "one-minute", "five-minute", "fifteen-minute", "mean-rate"
	};
	private volatile boolean stop;

	private CountDownLatch latch = new CountDownLatch(1);

	private INFLUXDB_VERSION version = Influxdb.INFLUXDB_VERSION.ZERO_POINT_EIGHT;

	/**
	 * Returns a new {@link Builder} for {@link InfluxdbReporter}.
	 *
	 * @param registry
	 *          the registry to report
	 * @return a {@link Builder} instance for a {@link InfluxdbReporter}
	 */
	public static Builder forRegistry(MetricRegistry registry) {
		return new Builder(registry);
	}



	/**
	 * A builder for {@link InfluxdbReporter} instances. Defaults to not using a
	 * prefix, using the default clock, converting rates to events/second,
	 * converting durations to milliseconds, and not filtering metrics.
	 */
	public static class Builder {
		private final MetricRegistry registry;
		private Clock clock;
		private String prefix;
		private TimeUnit rateUnit;
		private TimeUnit durationUnit;
		private MetricFilter filter;
		private boolean skipIdleMetrics;

		private Builder(MetricRegistry registry) {
			this.registry = registry;
			this.clock = Clock.defaultClock();
			this.prefix = null;
			this.rateUnit = TimeUnit.SECONDS;
			this.durationUnit = TimeUnit.MILLISECONDS;
			this.filter = MetricFilter.ALL;
		}

		/**
		 * Use the given {@link Clock} instance for the time.
		 *
		 * @param clock a {@link Clock} instance
		 * @return {@code this}
		 */
		public Builder withClock(Clock clock) {
			this.clock = clock;
			return this;
		}

		/**
		 * Prefix all metric names with the given string.
		 *
		 * @param prefix the prefix for all metric names
		 * @return {@code this}
		 */
		public Builder prefixedWith(String prefix) {
			this.prefix = prefix;
			return this;
		}

		/**
		 * Convert rates to the given time unit.
		 *
		 * @param rateUnit a unit of time
		 * @return {@code this}
		 */
		public Builder convertRatesTo(TimeUnit rateUnit) {
			this.rateUnit = rateUnit;
			return this;
		}

		/**
		 * Convert durations to the given time unit.
		 *
		 * @param durationUnit a unit of time
		 * @return {@code this}
		 */
		public Builder convertDurationsTo(TimeUnit durationUnit) {
			this.durationUnit = durationUnit;
			return this;
		}

		/**
		 * Only report metrics which match the given filter.
		 *
		 * @param filter a {@link MetricFilter}
		 * @return {@code this}
		 */
		public Builder filter(MetricFilter filter) {
			this.filter = filter;
			return this;
		}

		/**
		 * Only report metrics that have changed.
		 *
		 * @param skipIdleMetrics
		 * @return {@code this}
		 */
		public Builder skipIdleMetrics(boolean skipIdleMetrics) {
			this.skipIdleMetrics = skipIdleMetrics;
			return this;
		}

		/**
		 * Builds a {@link InfluxdbReporter} with the given properties, sending
		 * metrics using the given {@link Influxdb} client.
		 *
		 * @param influxdb a {@link Influxdb} client
		 * @return a {@link InfluxdbReporter}
		 */
		public InfluxdbReporter build(Influxdb influxdb) {
			return new InfluxdbReporter(registry,
					influxdb,
					clock,
					prefix,
					rateUnit,
					durationUnit,
					filter,
					skipIdleMetrics);
		}
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(InfluxdbReporter.class);

	private final Influxdb influxdb;
	private final Clock clock;
	private final String prefix;
	private final boolean skipIdleMetrics;
	private final Map<MetricName, Long> previousValues;

	// Optimization : use pointsXxx to reduce object creation, by reuse as arg of
	// Influxdb.appendSeries(...)
	private final Object[][] pointsTimer = { {
		0l,
		0,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0l
	} };
	private final Object[][] pointsHistogram = { {
		0l,
		0,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0.0d,
		0l
	} };
	private final Object[][] pointsCounter = { {
		0l,
		0l
	} };
	private final Object[][] pointsGauge = { {
		0l,
		null
	} };
	private final Object[][] pointsMeter = { {
		0l,
		0,
		0.0d,
		0.0d,
		0.0d,
		0.0d
	} };

	private InfluxdbReporter(MetricRegistry registry,
			Influxdb influxdb,
			Clock clock,
			String prefix,
			TimeUnit rateUnit,
			TimeUnit durationUnit,
			MetricFilter filter,
			boolean skipIdleMetrics) {
		super(registry, "influxdb-reporter", filter, rateUnit, durationUnit);
		this.influxdb = influxdb;
		this.clock = clock;
		this.prefix = (prefix == null) ? "" : (prefix.trim() + ".");
		this.skipIdleMetrics = skipIdleMetrics;
		this.previousValues = new TreeMap<MetricName, Long>();
	}

	@Override
	@SuppressWarnings("rawtypes")
	public void report(SortedMap<MetricName, Gauge> gauges,
			SortedMap<MetricName, Counter> counters,
			SortedMap<MetricName, Histogram> histograms,
			SortedMap<MetricName, Meter> meters,
			SortedMap<MetricName, Timer> timers) {
		final long timestamp = clock.getTime();

		// oh it'd be lovely to use Java 7 here
		try {
			influxdb.resetRequest();

			for (Map.Entry<MetricName, Gauge> entry : gauges.entrySet()) {
				reportGauge(entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<MetricName, Counter> entry : counters.entrySet()) {
				reportCounter(entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<MetricName, Histogram> entry : histograms.entrySet()) {
				reportHistogram(entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<MetricName, Meter> entry : meters.entrySet()) {
				reportMeter(entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<MetricName, Timer> entry : timers.entrySet()) {
				reportTimer(entry.getKey(), entry.getValue(), timestamp);
			}

			if (influxdb.hasSeriesData()) {
				influxdb.sendRequest(true, false);
			}

			if(stop) {
				stop();
				latch.countDown();
			}
		} catch (Exception e) {
			LOGGER.warn("Unable to report to InfluxDB. Discarding data.", e);
		}
	}

	private void reportTimer(MetricName name, Timer timer, long timestamp) {
		if (canSkipMetric(name, timer)) {
			return;
		}
		final Snapshot snapshot = timer.getSnapshot();
		Object[] p = pointsTimer[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = snapshot.size();
		p[2] = convertDuration(snapshot.getMin());
		p[3] = convertDuration(snapshot.getMax());
		p[4] = convertDuration(snapshot.getMean());
		p[5] = convertDuration(snapshot.getStdDev());
		p[6] = convertDuration(snapshot.getMedian());
		p[7] = convertDuration(snapshot.get75thPercentile());
		p[8] = convertDuration(snapshot.get95thPercentile());
		p[9] = convertDuration(snapshot.get99thPercentile());
		p[10] = convertDuration(snapshot.get999thPercentile());
		p[11] = convertRate(timer.getOneMinuteRate());
		p[12] = convertRate(timer.getFiveMinuteRate());
		p[13] = convertRate(timer.getFifteenMinuteRate());
		p[14] = convertRate(timer.getMeanRate());
		p[15] = timer.getCount();
		assert (p.length == COLUMNS_TIMER.length);
		influxdb.appendSeries(prefix, name, ".timer", COLUMNS_TIMER, pointsTimer);
	}

	private void reportHistogram(MetricName name, Histogram histogram, long timestamp) {
		if (canSkipMetric(name, histogram)) {
			return;
		}
		final Snapshot snapshot = histogram.getSnapshot();
		Object[] p = pointsHistogram[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = snapshot.size();
		p[2] = snapshot.getMin();
		p[3] = snapshot.getMax();
		p[4] = snapshot.getMean();
		p[5] = snapshot.getStdDev();
		p[6] = snapshot.getMedian();
		p[7] = snapshot.get75thPercentile();
		p[8] = snapshot.get95thPercentile();
		p[9] = snapshot.get99thPercentile();
		p[10] = snapshot.get999thPercentile();
		p[11] = histogram.getCount();
		assert (p.length == COLUMNS_HISTOGRAM.length);
		influxdb.appendSeries(prefix, name, ".histogram", COLUMNS_HISTOGRAM, pointsHistogram);
	}

	private void reportCounter(MetricName name, Counter counter, long timestamp) {
		Object[] p = pointsCounter[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = counter.getCount();
		assert (p.length == COLUMNS_COUNT.length);
		influxdb.appendSeries(prefix, name, ".count", COLUMNS_COUNT, pointsCounter);
	}

	private void reportGauge(MetricName name, Gauge<?> gauge, long timestamp) {
		Object[] p = pointsGauge[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = gauge.getValue();
		assert (p.length == COLUMNS_GAUGE.length);
		influxdb.appendSeries(prefix, name, ".value", COLUMNS_GAUGE, pointsGauge);
	}

	private void reportMeter(MetricName name, Metered meter, long timestamp) {
		if (canSkipMetric(name, meter)) {
			return;
		}
		Object[] p = pointsMeter[0];
		p[0] = influxdb.convertTimestamp(timestamp);
		p[1] = meter.getCount();
		p[2] = convertRate(meter.getOneMinuteRate());
		p[3] = convertRate(meter.getFiveMinuteRate());
		p[4] = convertRate(meter.getFifteenMinuteRate());
		p[5] = convertRate(meter.getMeanRate());
		assert (p.length == COLUMNS_METER.length);
		influxdb.appendSeries(prefix, name, ".meter", COLUMNS_METER, pointsMeter);
	}

	// private String format(Object o) {
	// if (o instanceof Float) {
	// return format(((Float) o).doubleValue());
	// } else if (o instanceof Double) {
	// return format(((Double) o).doubleValue());
	// } else if (o instanceof Byte) {
	// return format(((Byte) o).longValue());
	// } else if (o instanceof Short) {
	// return format(((Short) o).longValue());
	// } else if (o instanceof Integer) {
	// return format(((Integer) o).longValue());
	// } else if (o instanceof Long) {
	// return format(((Long) o).longValue());
	// }
	// return null;
	// }
	// private String format(long n) {
	// return Long.toString(n);
	// }
	//
	// private String format(double v) {
	// // the Carbon plaintext format is pretty underspecified, but it seems like
	// it just wants
	// // US-formatted digits
	// return String.format(Locale.US, "%2.2f", v);
	// }

	/**
	 * Returns true if this metric is idle and should be skipped.
	 *
	 * @param name
	 * @param counting
	 * @return true if the metric should be skipped
	 */
	private boolean canSkipMetric(MetricName name, Counting counting) {
		boolean isIdle = calculateDelta(name, counting.getCount()) == 0L;
		if (skipIdleMetrics && !isIdle) {
			previousValues.put(name, counting.getCount());
		}
		return skipIdleMetrics && isIdle;
	}

	/**
	 * Calculate the delta from the current value to the previous reported value.
	 */
	private long calculateDelta(MetricName name, long count) {
		Long previous = previousValues.get(name);
		if (previous == null) {
			// unknown metric, force non-zero delta to report
			return -1L;
		}
		if (count < previous) {
			LOGGER.warn("Saw a non-monotonically increasing value for metric '{}'", name);
			return 0L;
		}
		return count - previous;
	}

	public void stopWhenDone() throws InterruptedException {
		stop = true;
		latch.await();
	}

	public void setVersion(INFLUXDB_VERSION version){
		this.version = version;
	}
}
