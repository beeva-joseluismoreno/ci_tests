package com.bbva.kbdp.flume.sink.hbase;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bbva.kbdp.domain.KeyByDate;
import com.bbva.kbdp.utils.DateUtils;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

public class RepCRMRegexpHBaseFeedbackEventSerializer2 implements HbaseEventSerializer {

	// Config vars
	/** Regular expression used to parse groups from event data. */
	public static final String REGEX_CONFIG = "regex";
	public static final String REGEX_DEFAULT = "(.*)";

	/** Whether to ignore case when performing regex matches. */
	public static final String IGNORE_CASE_CONFIG = "regexIgnoreCase";
	public static final boolean INGORE_CASE_DEFAULT = false;

	/** Comma separated list of column names to place matching groups in. */
	public static final String COL_NAME_CONFIG = "colNames";
	public static final String COLUMN_NAME_DEFAULT = "payload";

	/** Index of the row key in matched regex groups */
	public static final String ROW_KEY_INDEX_CONFIG = "rowKeyIndex";

	/** Placeholder in colNames for row key */
	public static final String ROW_KEY_NAME = "ROW_KEY";

	/** Whether to deposit event headers into corresponding column qualifiers */
	public static final String DEPOSIT_HEADERS_CONFIG = "depositHeaders";
	public static final boolean DEPOSIT_HEADERS_DEFAULT = false;

	/** What charset to use when serializing into HBase's byte arrays */
	public static final String CHARSET_CONFIG = "charset";
	public static final String CHARSET_DEFAULT = "UTF-8";

	public static final String TABLE_NAME = "tableName";
	/*
	 * This is a nonce used in HBase row-keys, such that the same row-key never
	 * gets written more than once from within this JVM.
	 */
	protected static final AtomicInteger nonce = new AtomicInteger(0);
	protected static String randomKey = RandomStringUtils.randomAlphanumeric(10);

	protected byte[] cf;
	private byte[] payload;
	private List<byte[]> colNames = Lists.newArrayList();
	private Map<String, String> headers;
	private boolean regexIgnoreCase;
	private boolean depositHeaders;
	private Pattern inputPattern;
	private Charset charset;
	private int rowKeyIndex;
	private String tableName;
	private final String SEND = "S";
	private final String OPEN = "O";
	private final String CLICK = "C";
	private final String ONE_STR = "1";
	private final int ACTION_INDEX = 6;
	private final int OPEN_COLUMN_INDEX = 6;
	private final int FEC_OPEN_COLUMN_INDEX = 7;
	private final int CLICK_COLUMN_INDEX = 8;
	private final int FEC_CLICK_COLUMN_INDEX = 9;

	Table table = null;

	private static final Logger logger = LoggerFactory.getLogger(RepCRMRegexpHBaseFeedbackEventSerializer2.class);

	@Override
	public void configure(Context context) {

		System.out.println(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure");
		String regex = context.getString(REGEX_CONFIG, REGEX_DEFAULT);
		System.out.println(
				":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure:: regex "
						+ regex);
		regexIgnoreCase = context.getBoolean(IGNORE_CASE_CONFIG, INGORE_CASE_DEFAULT);
		depositHeaders = context.getBoolean(DEPOSIT_HEADERS_CONFIG, DEPOSIT_HEADERS_DEFAULT);
		inputPattern = Pattern.compile(regex, Pattern.DOTALL + (regexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
		// logger.info("CHARSET ::::::::::::::: " +
		// context.getString(CHARSET_CONFIG, CHARSET_DEFAULT));

		charset = Charset.forName(context.getString(CHARSET_CONFIG, CHARSET_DEFAULT));

		// logger.info("CHARSET Class :: " + charset.toString());

		// logger.info("CHARSET Class :: " + charset);

		tableName = context.getString(TABLE_NAME);

		String colNameStr = context.getString(COL_NAME_CONFIG, COLUMN_NAME_DEFAULT);
		// System.out.println(
		// ":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure::
		// colNameStr " + colNameStr);
		String[] columnNames = colNameStr.split(",");
		for (String s : columnNames) {
			// System.out
			// .println(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure::
			// column:: " + s);
			colNames.add(s.getBytes(charset));
		}
		// System.out.println(
		// ":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure::
		// colNames " + colNames);
		// Rowkey is optional, default is -1
		rowKeyIndex = context.getInteger(ROW_KEY_INDEX_CONFIG, -1);
		System.out.println(
				":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure:: rowKeyIndex "
						+ rowKeyIndex);
		// if row key is being used, make sure it is specified correct
		if (rowKeyIndex >= 0) {
			if (rowKeyIndex >= columnNames.length) {
				// System.out.println(
				// ":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure::
				// rowKeyIndex >= columnNames.length");
				throw new IllegalArgumentException(
						ROW_KEY_INDEX_CONFIG + " must be " + "less than num columns " + columnNames.length);
			}
			if (!ROW_KEY_NAME.equalsIgnoreCase(columnNames[rowKeyIndex])) {
				// System.out.println(
				// ":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure::
				// !ROW_KEY_NAME.equalsIgnoreCase(columnNames[rowKeyIndex])");
				// System.out.println("columnNames[rowKeyIndex]" +
				// columnNames[rowKeyIndex]);
				// System.out.println("ROW_KEY_NAME" + ROW_KEY_NAME);
				throw new IllegalArgumentException("Column at " + rowKeyIndex + " must be " + ROW_KEY_NAME + " and is "
						+ columnNames[rowKeyIndex]);
			}
		}

		logger.info(
				":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: HBaseConfiguration.create() INI");
		Configuration config = HBaseConfiguration.create();
		Connection connection = null;
		logger.info(
				":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: HBaseConfiguration.create() FIN");

		try {
			logger.info(
					":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: HBaseConfiguration.createConnection() ini");
			connection = ConnectionFactory.createConnection(config);
			logger.info(
					":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: HBaseConfiguration.createConnection() fin");
			table = connection.getTable(TableName.valueOf(tableName));
			logger.info(
					":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: HBaseConfiguration.createConnection() getTable fin");
			
//			IndexedTableAdmin admin = null;
//	        admin = new IndexedTableAdmin(conf);
//
//	        admin.addIndex(Bytes.toBytes("test_table"),
//	            new IndexSpecification("column2",
//	            Bytes.toBytes("columnfamily1:column2")));
	        
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(
					":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: ERROR AL OBTENER LA TABLA");
			e.printStackTrace();
		}

	}

	@Override
	public void configure(ComponentConfiguration arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void initialize(Event event, byte[] columnFamily) {
		// System.out.println(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.initialize");
		this.headers = event.getHeaders();
		this.payload = event.getBody();
		this.cf = columnFamily;

	}

	/**
	 * Returns a row-key with the following format: [time in millis]-[random
	 * key]-[nonce]
	 */
	protected byte[] getRowKey(Calendar cal) {
		/*
		 * NOTE: This key generation strategy has the following properties:
		 * 
		 * 1) Within a single JVM, the same row key will never be duplicated. 2)
		 * Amongst any two JVM's operating at different time periods (according
		 * to their respective clocks), the same row key will never be
		 * duplicated. 3) Amongst any two JVM's operating concurrently
		 * (according to their respective clocks), the odds of duplicating a
		 * row-key are non-zero but infinitesimal. This would require
		 * simultaneous collision in (a) the timestamp (b) the respective nonce
		 * and (c) the random string. The string is necessary since (a) and (b)
		 * could collide if a fleet of Flume agents are restarted in tandem.
		 * 
		 * Row-key uniqueness is important because conflicting row-keys will
		 * cause data loss.
		 */
		String rowKey = String.format("%s-%s-%s", cal.getTimeInMillis(), randomKey, nonce.getAndIncrement());
		return rowKey.getBytes(charset);
	}

	protected byte[] getRowKey() {
		return getRowKey(Calendar.getInstance());
	}

	@Override
	public List<Row> getActions() {
		KeyByDate existingRowKey = null;
		// TODO event length control

		// System.out.println(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.getActions");
		List<Row> actions = Lists.newArrayList();
		// System.out.println(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.getActions
		// :: new String(payload, charset)" + new String(payload, charset));

		String record = new String(payload, charset);
		// byte[] utf8String = UTF8.getBytes(record);
		// String utf8Record = new String(utf8String);
		String[] values = record.split("\\|", -1);

		// System.out.println("VALUES length :: " + values.length);
		// System.out.println("RECORD :: " + record);

		if (null != record && !record.contains("COD_PERSONA_TOKEN") && values.length == 41) {// TODO
																								// change
																								// for
																								// a
																								// sort
																								// of
																								// skipHeaders
																								// method
			// System.out.println("values.length :: " + values.length);
			// for (int i = 0; i < values.length; i++) {
			// System.out.println("values[" + i + "] :: " + values[i]);colNames
			// }

			byte[] rowKey;

			Matcher m = inputPattern.matcher(new String(payload, charset));

			// try{
			// if (!m.matches()) {
			// System.out.println("4444");
			// System.out.println("No machea");
			// return Lists.newArrayList();
			// }else{
			// System.out.println("5555");
			// System.out.println("SI machea");
			// }
			// }catch(Exception e){
			// System.out.println("ttttttttttttttttttttttt :: " + e);
			// }

			// if (m.groupCount() != colNames.size()) {
			// System.out.println("m.groupCount() != colNames.size() :: " +
			// m.groupCount() + " :: " + colNames.size());
			// return Lists.newArrayList();
			// }else{
			// System.out.println("group count bien");
			// }

			try {
				if (rowKeyIndex < 0) {
					rowKey = getRowKey();
					// System.out.println(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.getActions::
					// rowKey < 0 : " + rowKey);
				} else {
					// rowKey = m.group(rowKeyIndex +
					// 1).getBytes(Charsets.UTF_8);
					// rowKey = (values[0] + values[3] +
					// values[7]).getBytes(Charsets.UTF_8);
					// if the action is different than send, an existent row key
					// should be found. Otherwise, the row key
					// will be campania, cliente_sucio and fecha_acción.

					existingRowKey = getSendKey(values, table);
					int random = 1 + (int) (Math.random() * ((100 - 1) + 1));
					//
					rowKey = !SEND.equals(values[6]) && existingRowKey != null ?
							existingRowKey.getRowKey() :
							(random + "_" + values[0] + values[7].split(" ")[0] + values[3]).getBytes(Charsets.UTF_8);

					//rowKey = (random + "_" + values[0] + values[7].split(" ")[0] + values[3]).getBytes(Charsets.UTF_8);

					// String module = ""+Math.abs(record.hashCode() % 256);
					// rowKey = module.getBytes();

				}

				Put put = new Put(rowKey);
				// put.setWriteToWAL(false);

				// values to be inserted are picked up from event
				String[] feedbackValues = fillValues(values);

				// System.out.println("RowKey :: " + rowKey);

				// System.out.println("HAce el Put");

				if (SEND.equals(values[ACTION_INDEX])
						|| (!SEND.equals(values[ACTION_INDEX]) && existingRowKey == null)) {
					// It's a Send. All values are inserted.
					for (int i = 0; i < colNames.size() - 1; i++) {
						// if (i != rowKeyIndex) {
						// System.out.println("añade los put ::" + new
						// String(colNames.get(i + 1)) + " :: i es " + i + " ::
						// valor :: " + feedbackValues[i]);
						// put.add(cf, colNames.get(i), m.group(i +
						// 1).getBytes(Charsets.UTF_8));
						put.addColumn(cf, colNames.get(i + 1), feedbackValues[i].getBytes(Charsets.UTF_8));

						// }
					}
				} else {
					insertOtherThanSend(put, colNames, values);
				}

				// System.out.println("Registro insertado :: rowKey :: " + new
				// String(rowKey));
				if (depositHeaders) {
					for (Map.Entry<String, String> entry : headers.entrySet()) {
						put.addColumn(cf, entry.getKey().getBytes(charset), entry.getValue().getBytes(charset));
					}
				}
				// put.setWriteToWAL(false);
				actions.add(put);
				// logger.info("Registro insertado :: rowKey :: " + new
				// String(rowKey));
				// System.out.println("Añade el put al actions");
			} catch (Exception e) {
				System.out.println("Could not get row key! :: " + e);
				throw new FlumeException("Could not get row key!", e);
			}
		} else {
			System.out.println("LENGTH WRONG :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
		}
		// System.out.println("Acaba getActions");
		return actions;
	}

	private void insertOtherThanSend(Put put, List<byte[]> inputColNames, String[] eventValues) {
		if (OPEN.equals(eventValues[ACTION_INDEX])) {
			put.addColumn(cf, colNames.get(OPEN_COLUMN_INDEX), ONE_STR.getBytes(Charsets.UTF_8));
			put.addColumn(cf, colNames.get(FEC_OPEN_COLUMN_INDEX), eventValues[7].getBytes(Charsets.UTF_8));
		} else if (CLICK.equals(eventValues[ACTION_INDEX])) {
			put.addColumn(cf, colNames.get(CLICK_COLUMN_INDEX), ONE_STR.getBytes(Charsets.UTF_8));
			put.addColumn(cf, colNames.get(FEC_CLICK_COLUMN_INDEX), eventValues[7].getBytes(Charsets.UTF_8));
		} else {
			put.addColumn(cf, "DEFAULT".getBytes(Charsets.UTF_8), ONE_STR.getBytes(Charsets.UTF_8));
		}
	}

	private String[] fillValues(String[] inputValues) {
		String[] result = new String[colNames.size() - 1];
		// COD_CAMPANIA
		result[0] = inputValues[3];
		// CLI_TOKEN
		result[1] = inputValues[0];
		// FEC_LANZ
		result[2] = inputValues[7];
		// SEND
		if (null != inputValues[6] && SEND.equals(inputValues[6])) {
			result[3] = "1";
			result[4] = inputValues[7];
		} else {
			result[3] = "0";
			result[4] = "";
		}
		// OPEN
		if (null != inputValues[6] && OPEN.equals(inputValues[6])) {
			result[5] = "1";
			result[6] = inputValues[7];
		} else {
			result[5] = "0";
			result[6] = "";
		}
		// CLICK
		if (null != inputValues[6] && CLICK.equals(inputValues[6])) {
			result[7] = "1";
			result[8] = inputValues[7];
		} else {
			result[7] = "0";
			result[8] = "";
		}
		// SEGMENTO
		result[9] = inputValues[40];
		// LIFE_SUCCESS
		result[10] = inputValues[33];
		// FEC_CARGA
		if (null != inputValues[6] && SEND.equals(inputValues[6])) {
			result[11] = DateUtils.getStringFromDate(new Date());
		} else {
			result[11] = "";
		}
		// PRODUCTO
		result[12] = inputValues[19];

		return result;
	}

	private KeyByDate getSendKey(String[] values, Table table2) {
		// System.out.println("getSendKey init");
		// Configuration config = HBaseConfiguration.create();
		// Connection connection = null;
		// Table table = null;
		Scan scan = null;
		ResultScanner resultScanner = null;
		List<KeyByDate> lDates = new ArrayList<KeyByDate>();
		KeyByDate kbd = null;

		// Thread.currentThread().getStackTrace().

		try {
			// connection = ConnectionFactory.createConnection(config);
			// System.out.println("connection ::::::::::::");
			// table = connection.getTable(TableName.valueOf(tableName));
			// System.out.println("table ::::::::::::::");
			// Filter
			Filter filter = new PrefixFilter(Bytes.toBytes(values[3] + values[0]));
			logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 1111");
			FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			//logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 2222");
			SingleColumnValueFilter filterByCamp = null;
			SingleColumnValueFilter filterByToken = null;
			SingleColumnValueFilter filterByFLanz = null;
			SingleColumnValueFilter filterBySend = null;

//			String clave = values[0] + values[7].split(" ")[0] + values[3];
//			String pattern = "^(?:(?!0)\\d{1,2}|100)_" + values[0] + ".*" + values[3] + "$";
//			RegexStringComparator rr = new RegexStringComparator(pattern);// + "_" + clave);
//			Filter filter2 = new RowFilter(CompareOp.EQUAL, // co RowFilterExample-2-Filter2 Another filter, this time using a regular expression to match the row keys.
//				      rr);
			
			if (values[0] != null) {
				filterByCamp = new SingleColumnValueFilter(cf, Bytes.toBytes("TOKEN"), CompareOp.EQUAL,
						Bytes.toBytes(values[0]));
			} else
				logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: values[0] null");
			if (values[3] != null) {
				filterByToken = new SingleColumnValueFilter(cf, Bytes.toBytes("CAMP"), CompareOp.EQUAL,
						Bytes.toBytes(values[3]));
			} else
				logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: values[3] null");
			if (values[7] != null) {
				filterByFLanz = new SingleColumnValueFilter(cf, Bytes.toBytes("FLANZ"), CompareOp.LESS_OR_EQUAL,
						Bytes.toBytes(values[7]));
			} else
				logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: values[7] null");
			filterBySend = new SingleColumnValueFilter(cf, Bytes.toBytes("SEND"), CompareOp.EQUAL, Bytes.toBytes("1"));
			
			//logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 3333");
			if (filterByToken != null) list.addFilter(filterByToken);
			if (filterByCamp != null) list.addFilter(filterByCamp);
			if (filterByFLanz != null) list.addFilter(filterByFLanz);
			list.addFilter(filterBySend);
			// System.out.println("filter :::::::::::::::::::::");
			scan = new Scan();
			//logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 4444");
			scan.setMaxVersions(1);
			
			
			
			scan.setFilter(list);
//			scan.setFilter(filter2);
			
			
			
			//logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 5555");
			scan.addColumn(cf, "FLANZ".getBytes());
			//logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 6666");
			//scan.setCaching(500);
			//logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 6666-00000");
			if (null != table2) {
				try{
					resultScanner = table2.getScanner(scan);
				}catch(NullPointerException e){
					logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: ERROR EN SCANER");
					e.printStackTrace();
				}
				
			} else {
				logger.info(
						":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: table is null");
			}
			//logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 7777");
			// System.out.println("resultScanner ::::::::::::::::::::::::");
			//logger.info("::::::ITERATOR SIZE::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 7777" + ((Collection)resultScanner.iterator()).size());
			
			for (Iterator<Result> iterator = resultScanner.iterator(); iterator.hasNext();) {
				// printRow(iterator.next());
				// System.out.println("entra for ::::::::::::::::::::::::");
				// TODO add to a List and look for the closest in order to get
				// the timestamp and use it as part of the rowkey
				//logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 8888");
				Result result = iterator.next();
				String sDate = Bytes.toString(result.getValue(cf, Bytes.toBytes("FLANZ")));
				byte[] rowKey = result.getRow();
				//logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 9999");
				// System.out.println("sDate :::::::::::::::::::::::: " +
				// sDate);
				if (null != sDate) {
					// if (
					// (DateUtils.getDateFromString(sDate).compareTo(DateUtils.getDateFromString(values[7])))
					// >= 0){
					// fechaLanzamiento = sDate;
					// }
					lDates.add(new KeyByDate(DateUtils.getDateFromString(sDate), rowKey));

				}
			}
//			logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 10-3333");
			kbd = DateUtils.closestDate(lDates, values[7]);
//			logger.info(":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer :: 10-4444");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// Make sure you close your scanners when you are done!
			resultScanner.close();
		}

		return kbd != null ? kbd : null;
	}

	private Connection getConnection() {
		Configuration config = HBaseConfiguration.create();
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return connection;
	}

	public static void printRow(Result result) {
		String returnString = "";
		returnString += Bytes.toString(result.getValue(Bytes.toBytes("fdb"), Bytes.toBytes("COD_PERSONA_TOKEN")))
				+ ", ";
		returnString += Bytes.toString(result.getValue(Bytes.toBytes("fdb"), Bytes.toBytes("ID_JOURNEY"))) + ", ";
		returnString += Bytes.toString(result.getValue(Bytes.toBytes("fdb"), Bytes.toBytes("TIMESTAMP")));
		// System.out.println(returnString);
	}

	@Override
	public List<Increment> getIncrements() {
		return Lists.newArrayList();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	public void queryTable(String ia_table) {
		Configuration config = HBaseConfiguration.create();
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf(ia_table));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Scan scan = null;
		ResultScanner resultScanner = null;

		try {
			// Filter
			Filter filter = new PrefixFilter(Bytes.toBytes("144097551400001045764D94224283J2014305"));
			scan = new Scan();
			// byte[] prefix = Bytes.toBytes(values[0] + values[3]);
			// Scan scan = new Scan(prefix);
			// Filter prefixFilter = new PrefixFilter(prefix);
			scan.setFilter(filter);
			resultScanner = table.getScanner(scan);
			for (Iterator<Result> iterator = resultScanner.iterator(); iterator.hasNext();) {
				printRow(iterator.next());
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// Make sure you close your scanners when you are done!
			resultScanner.close();
		}
	}

	public static void main(String[] args) {

		
		String[] input1 = new String[41];
		input1[0] = "01045764D94224283J2014305";
		input1[3] = "76389EFD-C1B0-4846-96A2-9FAC72A870F7";
		input1[7] = "08/09/2015 0:58:34";

		String clave = input1[0] + input1[7].split(" ")[0] + input1[3];
		String pattern = input1[0] + "*" + input1[7];
		Pattern p = Pattern.compile("^(?:(?!0)\\d{1,2}|100)_" + input1[0] + ".*" + input1[3] + "$");
		
		 Matcher m = p.matcher("34_" + clave); 
		 boolean b = m.matches();
		 
		 
		RepCRMRegexpHBaseFeedbackEventSerializer2 me = new RepCRMRegexpHBaseFeedbackEventSerializer2();

		Configuration config = HBaseConfiguration.create();
		Connection connection = null;
		Table table = null;

		me.tableName = "kbdp:dfeedback_new";
		me.cf = "fdb".getBytes();

		try {
			connection = ConnectionFactory.createConnection(config);
			table = connection.getTable(TableName.valueOf("kbdp:dfeedback_new"));

			String[] input = new String[41];
			input[0] = "01045764D94224283J2014305";
			input[3] = "76389EFD-C1B0-4846-96A2-9FAC72A870F7";
			input[7] = "08/09/2015 0:58:34";
			//KeyByDate kbd = me.getSendKey(input, table);
			//String rowKey1 = new String(kbd.getRowKey());
	//		if (rowk)
			int random = 1 + (int) (Math.random() * ((100 - 1) + 1));
			byte[] rowKey = (random + "_" + input[0] + input[7].split(" ")[0] + input[3]).getBytes(Charsets.UTF_8);
			//byte[] rowKey = (""+random).getBytes(Charsets.UTF_8);
			Put put = new Put(rowKey);
			put.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes("FLANZ"), Bytes.toBytes(input[7]));
			put.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes("SEND"), Bytes.toBytes("1"));
			table.put(put);
			//
//			input[0] = "01045764D94224283J2014305";
//			input[3] = "76389EFD-C1B0-4846-96A2-9FAC72A870F7";
//			input[7] = "09/09/2015 0:58:34";
//			KeyByDate kbd2 = me.getSendKey(input, table);
//			String rowKey2 = new String(kbd2.getRowKey());
//			random = 1 + (int) (Math.random() * ((100 - 1) + 1));
//			if (rowKey2 != null){
//				//rowKey = (random + "_" + input[0] + rowKey2 + input[3]).getBytes(Charsets.UTF_8);
//				Put put2 = new Put(rowKey2.getBytes());
//				put2.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes("FLANZ"), Bytes.toBytes(input[7]));
//				put2.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes("OPEN"), Bytes.toBytes("1"));
//				table.put(put2);
//			}
//			//
//			input[0] = "01045764D94224283J2014305";
//			input[3] = "76389EFD-C1B0-4846-96A2-9FAC72A870F7";
//			input[7] = "10/09/2015 0:58:34";
//			KeyByDate kbd3 = me.getSendKey(input, table);
//			String rowKey3 = new String(kbd3.getRowKey());
//			random = 1 + (int) (Math.random() * ((100 - 1) + 1));
//			if (rowKey3 != null){
//				//rowKey = (random + "_" + input[0] + rowKey3 + input[3]).getBytes(Charsets.UTF_8);
//				Put put3 = new Put(rowKey3.getBytes());
//				put3.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes("FLANZ"), Bytes.toBytes(input[7]));
//				put3.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes("CLICK"), Bytes.toBytes("1"));
//				table.put(put3);
//
//			}
//			//
//			input[0] = "01046226A44963888G2014314";
//			input[3] = "76389EFD-C1B0-4846-96A2-9FAC72A870F7";
//			input[7] = "08/09/2015 3:46:02";
//			KeyByDate kbd4 = me.getSendKey(input, table);
//			String rowKey4 = null;
//			if (kbd4 != null){
//				rowKey4 = new String(kbd4.getRowKey());
//			}
//			
//			random = 1 + (int) (Math.random() * ((100 - 1) + 1));
//			if (rowKey4 != null){
//				//rowKey = (random + "_" + input[0] + rowKey4 + input[3]).getBytes(Charsets.UTF_8);
//				Put put4 = new Put(rowKey4.getBytes());
//				put4.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes("FLANZ"), Bytes.toBytes(input[7]));
//				put4.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes("SEND"), Bytes.toBytes("1"));
//				table.put(put4);
//			}else{
//				rowKey = (random + "_" + input[0] + input[7] + input[3]).getBytes(Charsets.UTF_8);
//				Put put5 = new Put(rowKey);
//				put5.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes("FLANZ"), Bytes.toBytes(input[7]));
//				put5.addColumn(Bytes.toBytes("fdb"), Bytes.toBytes("SEND"), Bytes.toBytes("1"));
//				table.put(put5);
//			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		me.queryTable(me.tableName);
	}

}
