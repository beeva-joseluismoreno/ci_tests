package com.bbva.kbdp.flume.sink.hbase;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HBaseSinkConfigurationConstants;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
//import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bbva.kbdp.domain.KeyByDate;
import com.bbva.kbdp.utils.DateUtils;
import com.bbva.kbdp.utils.FileUtils;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

public class RepCRMRegexpHBaseApxEventSerializer implements HbaseEventSerializer {

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
	public static final String QUALIFIER_CONTRATA = "CONTRATA";
	public static final String QUALIFIER_FEC_CONTRATA = "FCONTRATA";
	public static final String QUALIFIER_AMOUNT = "AMOUNT";
	
	public static final String DATE_CONTRATA_FORMAT = "yyyy-MM-dd";
	
    public static final String CONFIG_KEYTAB = "kerberosKeytab";
    public static final String CONFIG_PRINCIPAL = "kerberosPrincipal";

	
	private String kerberosPrincipal;
	private String kerberosKeytab;
	private PrivilegedExecutor privilegedExecutor;


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

	private String[] productsInScopeList = null;
	//private static final String hdfsPropertiesPath = "/tmp/CLOUDERA/product.properties";
	private static final String hdfsPropertiesPath = "/data_raw/KBDP/product.properties";
	//private static final String hdfsPropertiesPath = "/home/joseluismoreno/workspace/rt-kbdp-feedbackIngest/src/main/resources/product.properties";
	private static final String productsInScope = "productsInScope";
	private static final int factsColumnsLength = 4;
	private static final int productIndex = 1;
	private static final int participantsIndex = 0;
	private static final int uuidIndex = 0;
	private String tableName;
	private final String ONE_STR = "1";
	
	Table table = null;
	
	private static final Logger logger = LoggerFactory.getLogger(RepCRMRegexpHBaseApxEventSerializer.class);

	@Override
	public void configure(Context context) {

		logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.configure");
		String regex = context.getString(REGEX_CONFIG, REGEX_DEFAULT);
		//System.out.println(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.configure:: regex " + regex);
		regexIgnoreCase = context.getBoolean(IGNORE_CASE_CONFIG, INGORE_CASE_DEFAULT);
		depositHeaders = context.getBoolean(DEPOSIT_HEADERS_CONFIG, DEPOSIT_HEADERS_DEFAULT);
		inputPattern = Pattern.compile(regex, Pattern.DOTALL + (regexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
		//System.out.println(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.configure:: inputPattern " + inputPattern);
		charset = Charset.forName(context.getString(CHARSET_CONFIG, CHARSET_DEFAULT));

		tableName = context.getString(TABLE_NAME);
		
		String colNameStr = context.getString(COL_NAME_CONFIG, COLUMN_NAME_DEFAULT);
		//System.out.println(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.configure:: colNameStr " + colNameStr);
		String[] columnNames = colNameStr.split(",");
		for (String s : columnNames) {
//			System.out
//					.println(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.configure:: column:: " + s);
			colNames.add(s.getBytes(charset));
		}
		//System.out.println(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.configure:: colNames " + colNames);
		// Rowkey is optional, default is -1
		rowKeyIndex = context.getInteger(ROW_KEY_INDEX_CONFIG, -1);
		//System.out.println(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.configure:: rowKeyIndex " + rowKeyIndex);
		// if row key is being used, make sure it is specified correct
		if (rowKeyIndex >= 0) {
			if (rowKeyIndex >= columnNames.length) {
				//System.out.println(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.configure:: rowKeyIndex >= columnNames.length");
				throw new IllegalArgumentException(
						ROW_KEY_INDEX_CONFIG + " must be " + "less than num columns " + columnNames.length);
			}
			if (!ROW_KEY_NAME.equalsIgnoreCase(columnNames[rowKeyIndex])) {
				//System.out.println(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.configure:: !ROW_KEY_NAME.equalsIgnoreCase(columnNames[rowKeyIndex])");
				//System.out.println("columnNames[rowKeyIndex]" + columnNames[rowKeyIndex]);
				//System.out.println("ROW_KEY_NAME" + ROW_KEY_NAME);
				throw new IllegalArgumentException("Column at " + rowKeyIndex + " must be " + ROW_KEY_NAME + " and is "
						+ columnNames[rowKeyIndex]);
			}
		}
		
	    kerberosKeytab = context.getString(CONFIG_KEYTAB);
	    kerberosPrincipal = context.getString(CONFIG_PRINCIPAL);

	    try {
	        privilegedExecutor = FlumeAuthenticationUtil.getAuthenticator(kerberosPrincipal, kerberosKeytab);
	      } catch (Exception ex) {
	    	  logger.info(
	  				":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: Falla el PrivilegedExecutor");
//	        sinkCounter.incrementConnectionFailedCount();
	        throw new FlumeException("Failed to login to HBase using "
	          + "provided credentials.", ex);
	      }

	    
		logger.info(
				":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: HBaseConfiguration.create() INI");
		final Configuration config = HBaseConfiguration.create();
		Connection connection = null;
		logger.info(
				":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: HBaseConfiguration.create() FIN");

	    try {
	        table = privilegedExecutor.execute(new PrivilegedExceptionAction<HTable>() {
	          @Override
	          public HTable run() throws Exception {
	            HTable table = new HTable(config, tableName);
	            table.setAutoFlush(false);
	            // Flush is controlled by us. This ensures that HBase changing
	            // their criteria for flushing does not change how we flush.
	            return table;
	          }
	        });
			logger.info(
					":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: HBaseConfiguration.createConnection() getTable fin");

	      } catch (Exception e) {
//	        sinkCounter.incrementConnectionFailedCount();
	        logger.error("*** Could not load table, " + tableName +
	            " from HBase ***", e);
	        throw new FlumeException("Could not load table, " + tableName +
	            " from HBase", e);
	      }
		
		
		
//		try {
//			logger.info(
//					":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: HBaseConfiguration.createConnection() ini");
//			connection = ConnectionFactory.createConnection(config);
//			logger.info(
//					":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: HBaseConfiguration.createConnection() fin");
//			table = connection.getTable(TableName.valueOf(tableName));
//			logger.info(
//					":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: HBaseConfiguration.createConnection() getTable fin");
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			logger.error(
//					":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: ERROR AL OBTENER LA TABLA");
//			e.printStackTrace();
//		}
		
	}

	@Override
	public void configure(ComponentConfiguration conf) {
	}

	@Override
	public void initialize(Event event, byte[] columnFamily) {
		logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.initialize");
		this.headers = event.getHeaders();
		this.payload = event.getBody();
		this.cf = columnFamily;
		
		logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.initialize :: obteniendo productos en scope :: productsInScope :: " + productsInScope + " :: hdfsPropertiesPath :: " + hdfsPropertiesPath);
		
		productsInScopeList = FileUtils.getProperty(productsInScope, hdfsPropertiesPath);

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
	public List<Row> getActions() throws FlumeException {
		
		
		logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions");
		List<Row> actions = Lists.newArrayList();
		//System.out.println(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: new String(payload, charset)" + new String(payload, charset));
		
//		Matcher m = inputPattern.matcher(new String(payload, charset));
//		if (!m.matches()) {
//			System.out.println("No machea");
//			return Lists.newArrayList();
//		}

		// //System.out.println("m.groupCount() = colNames.size() :: " +
		// m.groupCount() + " :: " + colNames.size());

//		if (m.groupCount() != colNames.size()) {
//			System.out.println("m.groupCount() != colNames.size() :: " + m.groupCount() + " :: " + colNames.size());
//			return Lists.newArrayList();
//		}

		try {
			// Before populating the puts, it's necessary to filter out the
			// product that are no of interest.
			// Besides, if a product belongs to more that one campaign,
			// it's necessary to insert in hbase as many records as campaigns
			// has the event
			String strPayload = new String(this.payload);
			String[] values = null;
			String inputProduct;
			String[] participants;
			String participant;
			String product = null;
			

			if (null != strPayload) {
				values = strPayload.split(",");
				logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: strPayload :: " + strPayload);
				logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: values.length :: " + values.length);
				if (null != values && values.length == factsColumnsLength) {
					inputProduct = values[productIndex];
					participants = values[participantsIndex].split("\\|");
					//inputCodPersona = values[codPersonaIndex];
					logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: inputProduct :: " + inputProduct);
					// product in scope checking
					if (productValidation(inputProduct)) {
						logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: producto valido");
						// campaign is obtained from products-campaigns mapping
						product = productsMapping(inputProduct);
						logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: producto es :: " + product);
						//there can be more than one campaign associated to one product
						try{
							if (null != product){
								//for (int i = 0; i < participants.length; i++) {
									//addPuts(participants[i], values, product, actions);
								//}
								//TODO first participant, eventually
								addPuts(participants[0], values, product, actions);
							}else{
								logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: producto null");
							}
						}catch(Exception e){
							//System.out.println("::::::::::::RegexHbaseEventSerializer.getActions :: casca ");
							e.printStackTrace();
						}
					}else{
						logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: producto NO valido");
					}
				}

			}else{
				logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: payload null");
			}

			//Put put = new Put(rowKey);

		} catch (Exception e) {
			throw new FlumeException("Could not get row key!", e);
		}
		System.out.println("Acaba getActions");
		return actions;
	}

	private void addPuts(String participant, String[] values, String product, List<Row> actions) {
		KeyByDate feedbackRowKey = null;
		byte[] rowKey = null;
		Put put = null;
		//We need to set the rowKey first.
		if (rowKeyIndex < 0) {
			rowKey = getRowKey();
			System.out.println(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions:: rowKey < 0 : " + rowKey);
		} else {
			//rowKey = m.group(rowKeyIndex + 1).getBytes(Charsets.UTF_8);
			System.out.println(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions:: rowKey > 0 : " + rowKey);
			//we need to find the closest fec_lanz to the event's fec_contrata in order join the event to that fec_lanz 
			//before the put. fec_contrata and product from event will be used for that. 
			logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: va a recuperar la rowKey");
			feedbackRowKey = getFeedbackKey(values, product, participant);
			if (null != feedbackRowKey){
				logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: recupera la rowKey");
				rowKey = feedbackRowKey.getRowKey();
				logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: rowKey :: " + new String(rowKey));
			}else{
				logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: NO recupera la rowKey");
				rowKey = getRowKey();
				logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: insertando con rowKey :: " + new String(rowKey));
			}
			put = new Put(rowKey);
			put.addColumn(cf, QUALIFIER_CONTRATA.getBytes(), ONE_STR.getBytes(Charsets.UTF_8));
			put.addColumn(cf, QUALIFIER_FEC_CONTRATA.getBytes(), values[2].getBytes(Charsets.UTF_8));
			put.addColumn(cf, QUALIFIER_AMOUNT.getBytes(), values[3].getBytes(Charsets.UTF_8));
			logger.info(":::::::::::::::::::::::::::::::::::RegexHbaseEventSerializer.getActions :: hace el put ");
			
		}

		
//		for (int ii = 0; ii < colNames.size(); ii++) {
//			if (ii != rowKeyIndex) {
//				put.add(cf, colNames.get(ii), m.group(ii + 1).getBytes(Charsets.UTF_8));
//			}
//		}
		//System.out.println("::::::::::::RegexHbaseEventSerializer.getActions :: depositHeaders :: " + depositHeaders);
		if (depositHeaders) {
			for (Map.Entry<String, String> entry : headers.entrySet()) {
				put.addColumn(cf, entry.getKey().getBytes(charset), entry.getValue().getBytes(charset));
			}
		}
		//System.out.println("Añade el put al actions ini");
		actions.add(put);
		System.out.println("Se añade el put al actions");
		//System.out.println("Añade el put al actions fin");

	}

	private KeyByDate getFeedbackKey(String[] values, String product, String participant) {
		System.out.println("getFeedbackKey init");
		//String[] result = new String[2]; 
		Scan scan = null;
		ResultScanner resultScanner = null;
		List<KeyByDate> lDates = new ArrayList<KeyByDate>();
		//String fechaLanzamiento = null;
		String sCodCampania = null;
		KeyByDate kbd = null;

		try {
			
			SingleColumnValueFilter scvfCliente = new SingleColumnValueFilter(Bytes.toBytes("fdb"), Bytes.toBytes("TOKEN"), CompareOp.EQUAL, Bytes.toBytes(participant));
			SingleColumnValueFilter scvfCampania = new SingleColumnValueFilter(Bytes.toBytes("fdb"), Bytes.toBytes("PROD"), CompareOp.EQUAL, Bytes.toBytes(product));
			FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			scvfCampania.setFilterIfMissing(true);
			scvfCliente.setFilterIfMissing(true);
			fl.addFilter(scvfCampania);
			fl.addFilter(scvfCliente);
			
			
			//List<Filter> filters = new ArrayList<Filter>();
			//filters.add(scvfCliente);
			//filters.add(scvfCampania);
			//FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
			scan = new Scan();
			scan.setMaxVersions(1);
			scan.setFilter(fl);
			//scan.addColumn(cf, "TOKEN".getBytes());
			//scan.addColumn(cf, "PROD".getBytes());
			//scan.addColumn(cf, "FLANZ".getBytes());
			resultScanner = table.getScanner(scan);
		    for (Iterator<Result> iterator = resultScanner.iterator(); iterator.hasNext();) {
		        //printRow(iterator.next());
		    	logger.info("entra for ::::::::::::::::::::::::");
		    	Result row = iterator.next();
		        //TODO add to a List and look for the closest in order to get the timestamp and use it as part of the rowkey
		    	String sDate = Bytes.toString(row.getValue(Bytes.toBytes("fdb"), Bytes.toBytes("FLANZ")));
		    	byte[] rowKey = row.getRow();
		    	//sCodCampania = Bytes.toString(row.getValue(Bytes.toBytes("fdb"), Bytes.toBytes("COD_CAMPANIA")));
		    	logger.info("sDate :::::::::::::::::::::::: " + sDate);
		    	logger.info("sCodCampania :::::::::::::::::::::::: " + sCodCampania);
		        if (null != sDate){
		        	lDates.add(new KeyByDate(DateUtils.getDateFromString(sDate), rowKey));
		        }
		    }

		    //TODO change to KeyByDate
		    kbd = DateUtils.closestDate(lDates, values[2], DATE_CONTRATA_FORMAT);
		    
		    //result[0] = sCodCampania;
		    //result[1] = fechaLanzamiento;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			// Make sure you close your scanners when you are done!
			resultScanner.close();
		}
		
		return kbd != null ? kbd : null;
	}

	@Override
	public List<Increment> getIncrements() {
		return Lists.newArrayList();
	}

	@Override
	public void close() {
	}

	private boolean productValidation(String inputProduct) {
		boolean interest = false;
		for (int i = 0; i < productsInScopeList.length && !interest; i++) {
            interest = (inputProduct.equals(productsInScopeList[i]));
        }
		return interest;
	}

	private String productsMapping(String iaProduct) {
		return FileUtils.getProperty(iaProduct, hdfsPropertiesPath)[0];
	}

	public static void main(String[] args) {
		
		RepCRMRegexpHBaseApxEventSerializer me = new RepCRMRegexpHBaseApxEventSerializer();
		
		Configuration config = HBaseConfiguration.create();
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(config);
			me.table = connection.getTable(TableName.valueOf("KBDP:TKBDPFDB"));
			me.cf = "ev".getBytes();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(
					":::::::::::::::::::::::::::::::::::RepCRMRegexpHBaseFeedbackEventSerializer.configure :: ERROR AL OBTENER LA TABLA");
			e.printStackTrace();
		}
		
		me.productsInScopeList = FileUtils.getProperty(productsInScope, hdfsPropertiesPath);
		me.charset = Charset.forName("UTF-8");
		
//		String[] input = new String[3];
//		input[0] = "01045764D94224283J2014305";
//		input[1] = "009000090100902";
//		input[2] = "09/09/2015 10:58:34";
//		
//		KeyByDate kbd = me.getFeedbackKey(input, "1 - 464 - Consumo");

		String[] input1 = new String[4];
		input1[0] = "01318819J90949511A2014305|93171792B83772484B2015244|93171792B59241757J2015244";
		input1[1] = "009000090100902";
		input1[2] = "2015-09-08";
		input1[3] = "27606.56";

		me.payload = "01318819J90949511A2014305|93171792B83772484B2015244|93171792B59241757J2015244,009000090100902,2015-09-08,27606.56".getBytes();
		
		try {
			me.table.put((Put)me.getActions().get(0));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//KeyByDate kbd1 = me.getFeedbackKey(input1, "1 - 464 - Consumo", "93171792B45888069G2015244");
		
		System.out.println("sdfs");
	}

}
