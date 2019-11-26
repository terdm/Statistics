package terdm.example;

import com.workday.insights.matrix.Forecast;
import oracle.jdbc.internal.OracleTypes;
import oracle.sql.ARRAY;
import oracle.sql.Datum;
import oracle.sql.StructDescriptor;
import ru.tinkoff.invest.openapi.SimpleStopLossStrategy;
import ru.tinkoff.invest.openapi.StrategyExecutor;
import ru.tinkoff.invest.openapi.data.*;
import ru.tinkoff.invest.openapi.wrapper.Connection;
import ru.tinkoff.invest.openapi.wrapper.Context;
import ru.tinkoff.invest.openapi.wrapper.SandboxContext;
import ru.tinkoff.invest.openapi.wrapper.impl.ConnectionFactory;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import static java.lang.Thread.sleep;
import java.util.Properties;

//import java.sql.PreparedStatement;
//import java.sql.SQLException;

public class App {
    private static String ssoToken;
    private static String ticker;
    private static CandleInterval candleInterval;
    private static BigDecimal maxVolume;
    private static boolean useSandbox;

    static BigDecimal[] nums;
    static String[] tickers;
    static FileInputStream fis;
    static Properties property = new Properties();
    static String cUSER,cPWD;

    public static void main(String[] args) {
        final Logger logger;

        try {
            fis = new FileInputStream("C:\\TKS\\invest\\example\\src\\main\\resources\\app_props.properties ");
            property.load(fis);
            cUSER = property.getProperty("USER");
            cPWD = property.getProperty("PWD");
        } catch (IOException e) {
            System.err.println("Properties file not found!");
        }


        try {
            logger = initLogger();
            extractParams(args, logger);
        } catch (IllegalArgumentException ex) {
            return;
        } catch (IOException ex) {
            System.err.println("При инициализации логгера произошла ошибка: " + ex.getLocalizedMessage());
            return;
        }

        try {
            final Connection connection;
            final Context context;
            if (useSandbox) {
                logger.fine("Создаём подключение в режиме \"песочницы\"... ");
                connection = ConnectionFactory.connectSandbox(ssoToken, logger).join();
            } else {
                logger.fine("Создаём подключение в биржевом режиме... ");
                connection = ConnectionFactory.connect(ssoToken, logger).join();
            }

            initCleanupProcedure(connection, logger);

            context = connection.context();

            if (useSandbox) {
                // ОБЯЗАТЕЛЬНО нужно выполнить регистрацию в "песочнице"
                ((SandboxContext)context).performRegistration();
            }
            logger.severe("Before test_conn");
            test_conn(logger);


            final InstrumentsList actualResponse = context.getMarketStocks().get();
            logger.severe("Before save");
            save_stock_candles(context, actualResponse, logger);
            logger.severe("After save");

            OffsetDateTime from = OffsetDateTime.of(2017, 01, 01, 7, 0, 0, 0, ZoneOffset.UTC);
            OffsetDateTime to = OffsetDateTime.of(2019, 10, 14, 7, 0, 0, 0, ZoneOffset.UTC);

            int forecastSize = 400;

            //logger.severe("to.minus " +to.minus(OffsetDateTime.of(2018, 05, 08, 7, 0, 0, 0, ZoneOffset.UTC)));
            forecastSize = (int) ChronoUnit.DAYS.between(OffsetDateTime.of(2017, 8, 01, 7, 0, 0, 0, ZoneOffset.UTC),to);
            logger.severe(" compareTo forecastSize " + forecastSize);
            to = to.minusDays(forecastSize);
            get_tickers(logger);
            String sTicker_forecast;
            for (int j=0;j<4;j++) {
                sTicker_forecast = tickers[j];
                logger.severe("Before get_rates_by_ticker sTicker_forecast " + sTicker_forecast + " j " + j);
                get_rates_by_ticker(sTicker_forecast,
                        from,
                        to,
                        logger);
                logger.severe("After get_rates_by_ticker");
                /////////////////////////////////////////////////
                logger.severe("Before Forecast");
                Forecast forecast = new Forecast();
                String[] arglist = new String[0];

                double[] ratesArr = new double[nums.length];

                for (int i = 0; i < nums.length; i++) {
                    ratesArr[i] = nums[i].doubleValue();
                }
                logger.severe("before forecastData ratesArr.length " + ratesArr.length);
                if (ratesArr.length != 0) {
                    forecast.setRatesArray(ratesArr);
                    forecast.setForecastSize(forecastSize);
                    double[] forecastData = forecast.main(arglist);

                    BigDecimal[] bdForecast = new BigDecimal[forecastData.length];
                    for (int i = 0; i < forecastData.length; i++) {
                        bdForecast[i] = BigDecimal.valueOf(forecastData[i]);
                        bdForecast[i] = bdForecast[i].setScale(2, BigDecimal.ROUND_HALF_UP);
                    }

                    save_forecast_candles(bdForecast, sTicker_forecast, from, to, logger);
                /*logger.severe(" forecastData.length " + forecastData.length);
                for (int i = 0; i < forecastData.length; i++) {
                    logger.severe("i " + i + " forecastData " + forecastData[i]);
                }
                */
                }
                logger.severe("after Forecast");
            }
            ///////////////////////////////////////////////


            logger.fine("Ищём по тикеру " + ticker + "... ");
            final var instrumentsList = context.searchMarketInstrumentsByTicker(ticker).join();

            final var instrumentOpt = instrumentsList.getInstruments()
                    .stream()
                    .findFirst();

            final Instrument instrument;
            if (instrumentOpt.isEmpty()) {
                logger.severe("Не нашлось инструмента с нужным тикером.");
                return;
            } else {
                instrument = instrumentOpt.get();
            }

           /* final var from = OffsetDateTime.of(2019, 11, 7, 0, 0, 0, 0, ZoneOffset.UTC);
            final var to = OffsetDateTime.of(2019, 11, 7, 23, 59, 59, 0, ZoneOffset.UTC);
            HistoricalCandles lHistoricalCandles;
            String  sFigi;
            sFigi = instrument.getFigi();
            lHistoricalCandles = context.getMarketCandles(sFigi, from, to, CandleInterval.ONE_MIN).get();
            List<Candle> lCandles;
            lCandles = lHistoricalCandles.getCandles();
*/
            //save_candles(lCandles,instrument.getTicker(),logger);



            if (useSandbox) {
                initPrepareSandbox((SandboxContext)context, instrument, logger);
            }

            logger.fine("Получаем валютные балансы... ");
            final var portfolioCurrencies = context.getPortfolioCurrencies().join();

            final var portfolioCurrencyOpt = portfolioCurrencies.getCurrencies().stream()
                    .filter(pc -> pc.getCurrency() == instrument.getCurrency())
                    .findFirst();

            final PortfolioCurrencies.PortfolioCurrency portfolioCurrency;
            if (portfolioCurrencyOpt.isEmpty()) {
                logger.severe("Не нашлось нужной валютной позиции.");
                return;
            } else {
                portfolioCurrency = portfolioCurrencyOpt.get();
            }

            logger.fine("Запускаем робота... ");
            final CompletableFuture<Void> result = new CompletableFuture<>();
            final var strategy = new SimpleStopLossStrategy(
                    portfolioCurrency,
                    instrument,
                    maxVolume,
                    5,
                    candleInterval,
                    BigDecimal.valueOf(0.1),
                    BigDecimal.valueOf(0.1),
                    BigDecimal.valueOf(0.5),
                    BigDecimal.valueOf(0.5),
                    logger
            );
            final var strategyExecutor = new StrategyExecutor(context, strategy, logger);
            strategyExecutor.run();
            result.join();
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Что-то пошло не так.", ex);
        }
    }



    private static Logger initLogger() throws IOException {
        LogManager logManager = LogManager.getLogManager();
        final var classLoader = App.class.getClassLoader();

        try (InputStream input = classLoader.getResourceAsStream("logging.properties")) {

            if (input == null) {
                throw new FileNotFoundException();
            }

            Files.createDirectories(Paths.get("./logs"));
            logManager.readConfiguration(input);
        }

        return Logger.getLogger(App.class.getName());
    }

    private static void extractParams(final String[] args, final Logger logger) throws IllegalArgumentException {
        if (args.length == 0) {
            logger.severe("Не передан авторизационный токен!");
            throw new IllegalArgumentException();
        } else if (args.length == 1) {
            logger.severe("Не передан исследуемый тикер!");
            throw new IllegalArgumentException();
        } else if (args.length == 2) {
            logger.severe("Не передан разрешающий интервал свечей!");
            throw new IllegalArgumentException();
        } else if (args.length == 3) {
            logger.severe("Не передан допустимый объём используемых средств!");
            throw new IllegalArgumentException();
        } else if (args.length == 4) {
            logger.severe("Не передан признак использования песочницы!");
            throw new IllegalArgumentException();
        } else {
            ssoToken = args[0];
            ticker = args[1];
            switch (args[2]) {
                case "1min":
                    candleInterval = CandleInterval.ONE_MIN;
                    break;
                case "2min":
                    candleInterval = CandleInterval.TWO_MIN;
                    break;
                case "3min":
                    candleInterval = CandleInterval.THREE_MIN;
                    break;
                case "5min":
                    candleInterval = CandleInterval.FIVE_MIN;
                    break;
                case "10min":
                    candleInterval = CandleInterval.TEN_MIN;
                    break;
                default:
                    logger.severe("Не распознан разрешающий интервал!");
                    throw new IllegalArgumentException();
            }
            maxVolume = new BigDecimal(args[3]);
            useSandbox = Boolean.parseBoolean(args[4]);
        }
    }

    private static void initPrepareSandbox(final SandboxContext context,
                                           final Instrument instrument,
                                           final Logger logger) {
        logger.fine("Очищаем всё позиции... ");
        context.clearAll().join();

        logger.fine("Ставим на баланс немного " + instrument.getCurrency() + "... ");
        context.setCurrencyBalance(instrument.getCurrency(), maxVolume).join();
    }

    private static void initCleanupProcedure(final Connection connection, final Logger logger) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("Закрываем соединение... ");
                connection.close();
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Что-то произошло при закрытии соединения!", e);
            }
        }));
    }

    public static void test_conn(Logger logger) throws ClassNotFoundException {
        System.setProperty("oracle.net.tns_admin", "C:/Oracle/oraclient18/NETWORK/ADMIN");
        String dbURL = property.getProperty("dbURL");
        Class.forName ("oracle.jdbc.OracleDriver");
        java.sql.Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(dbURL, cUSER, cPWD);
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT count(*) FROM DUAL");
            if (rs.next()) {
                logger.severe(rs.getString(1));
            }
        } catch (Exception e) {
            logger.severe(" test_conn exception " + e.toString());
            e.printStackTrace();
        }
        finally {
            if (stmt != null) try { stmt.close(); } catch (Exception e) {}
            if (conn != null) try { conn.close(); } catch (Exception e) {}
        }
    }

    public static java.sql.Connection get_conn(Logger logger) throws ClassNotFoundException {
        System.setProperty("oracle.net.tns_admin", "C:/Oracle/oraclient18/NETWORK/ADMIN");
        String dbURL = property.getProperty("dbURL");
        Class.forName ("oracle.jdbc.OracleDriver");
        java.sql.Connection conn = null;

        try {
            conn = DriverManager.getConnection(dbURL, cUSER, cPWD);

        } catch (Exception e) {
            logger.severe(" test_conn exception " + e.toString());
            e.printStackTrace();
        }
        finally {
            if (conn != null) try { conn.close(); } catch (Exception e) {}
        }
        return conn;
    }

    public static void save(List<Instrument> entities, Logger logger) throws SQLException,ClassNotFoundException {
        System.setProperty("oracle.net.tns_admin", "C:/Oracle/oraclient18/NETWORK/ADMIN");
        String sql = "INSERT INTO LLC_MS (TICKER) Values (?)";
        String dbURL = property.getProperty("dbURL");
        Class.forName ("oracle.jdbc.OracleDriver");
        java.sql.Connection conn = null;
        try (
                java.sql.Connection connection = conn = DriverManager.getConnection(dbURL, cUSER, cPWD);
                PreparedStatement statement = connection.prepareStatement(sql);
        ) {
            int i = 0;
            for (Instrument entity : entities) {
                statement.setString(1, entity.getTicker());
                // ...
                statement.addBatch();
                i++;
                if (i % 1000 == 0 || i == entities.size()) {
                    statement.executeBatch(); // Execute every 1000 items.
                }
            }
        }
        catch (Exception ex) {
            logger.severe(" save exception " + ex.toString());
        }
    }
    public static void save_candles(List<Candle> entities, String ticker , Long MSC_H_ID, Logger logger) throws SQLException,ClassNotFoundException {
        System.setProperty("oracle.net.tns_admin", "C:/Oracle/oraclient18/NETWORK/ADMIN");
        String sql = "INSERT INTO LLC_MSC_B (TICKER,FIGI,CANDLE_INTERVAL,PRICE_O,PRICE_C,PRICE_H,PRICE_L,DEALS_VOLUME,CANDLE_TIME,MSC_H_ID) Values (?,?,?,?,?,?,?,?,?,?)";


        //String dbURL = property.getProperty("dbURL_TEST");
        String dbURL = property.getProperty("dbURL");
        Class.forName ("oracle.jdbc.OracleDriver");
        java.sql.Connection conn = null;
        try (
                java.sql.Connection connection = conn = DriverManager.getConnection(dbURL, cUSER, cPWD);
                PreparedStatement statement = connection.prepareStatement(sql);
        ) {
            int i = 0;
            for (Candle entity : entities) {


                statement.setString(1, ticker);
                statement.setString(2, entity.getFigi());
                statement.setString(3, "" + entity.getInterval());
                statement.setBigDecimal(4, entity.getO());
                statement.setBigDecimal(5, entity.getC());
                statement.setBigDecimal(6, entity.getH());
                statement.setBigDecimal(7, entity.getL());
                statement.setBigDecimal(8, entity.getV());
                statement.setString(9, "" + entity.getTime());
                statement.setLong(10,  MSC_H_ID);

                // ...
                statement.addBatch();
                i++;
                if (i % 1000 == 0 || i == entities.size()) {
                    statement.executeBatch(); // Execute every 1000 items.
                }
            }
        }
        catch (Exception ex) {
            logger.severe(" save exception " + ex.toString());
        }
    }

    public static void save_stock_candles(Context context, InstrumentsList actualResponse, Logger logger)
    {
        Long nId;
        String sFigi, sTicker,sHCS;
        Instrument vInstrument;
        List<Candle> lCandles;
        final var from = OffsetDateTime.of(2017, 01, 01, 7, 0, 0, 0, ZoneOffset.UTC);
        //final var to = OffsetDateTime.of(2019, 11, 15, 23, 59, 59, 999, ZoneOffset.UTC);
        final var to = OffsetDateTime.now();
        HistoricalCandles lHistoricalCandles;
        OffsetDateTime cycl_from;
        OffsetDateTime cycl_to;

        for (cycl_from = from; cycl_from.compareTo(to) < 0 ; cycl_from = cycl_from.plusYears(1) ) {
            if (cycl_from.plusYears(1).minusSeconds(1).compareTo(to) < 0)
            {
                cycl_to=cycl_from.plusYears(1).minusSeconds(1);
            }
            else {
                cycl_to = to;
            }
            logger.severe("in cycle cycl_from  " + cycl_from.toString() + " cycl_to " + cycl_to.toString());
            Integer j = 0;
            try {
                for (int i = 0; i < actualResponse.getInstruments().size(); i++) {
                    try {
                        vInstrument = actualResponse.getInstruments().get(i);
                        sFigi = vInstrument.getFigi();
                        sTicker = vInstrument.getTicker();
                        logger.severe("Test sTicker  "+  sTicker);
                        //if (sTicker.equals("PGR")) {
                        if (1 == 1) {
                            logger.severe("sTicker  " + sTicker + " sFigi  " + sFigi + " i " + i);
                            if (j > 100) {
                                sleep(1000 * 61);
                                j = 0;
                            }
                            j++;
                            nId = save_candles_search_params(sFigi, sTicker, cycl_from, cycl_to, CandleInterval.DAY, logger);
                            lHistoricalCandles = context.getMarketCandles(sFigi, cycl_from, cycl_to, CandleInterval.DAY).get();
                            lCandles = lHistoricalCandles.getCandles();
                            save_candles(lCandles, sTicker, nId, logger);
                        }
                    } catch (Exception ex) {
                        logger.severe("Exception in candle value  " + ex.toString() + " i " + i);
                    }
                }
            } catch (Exception ex) {
                logger.severe("Exception in candles " + ex.toString());
            }
            ;
        }
    }

    public static Long save_candles_search_params(String figi,
                                                  String ticker,
                                                  OffsetDateTime from,
                                                  OffsetDateTime to,
                                                  CandleInterval interval,
                                                  Logger logger) throws SQLException,ClassNotFoundException {
        System.setProperty("oracle.net.tns_admin", "C:/Oracle/oraclient18/NETWORK/ADMIN");
        String sql = "BEGIN INSERT INTO LLC_MSC_H (TICKER,FIGI,CANDLE_INTERVAL,FROM_DATE,TILL_DATE) Values (?,?,?,?,?) returning REC_ID into ?; END;";

        String dbURL = property.getProperty("dbURL");

        Class.forName ("oracle.jdbc.OracleDriver");

        //java.sql.Connection conn = null;
        Long lRec_id;
        try (
                //java.sql.Connection connection = conn = DriverManager.getConnection(dbURL, cUSER, cPWD);
                java.sql.Connection connection = DriverManager.getConnection(dbURL, cUSER, cPWD);
                CallableStatement  statement = connection.prepareCall(sql);
        ) {
                statement.setString(1,ticker);

                statement.setString(2,figi);

                statement.setString(3, "" + interval);
                statement.setString(4, ""+ from);
                statement.setString(5, ""+ to);statement.registerOutParameter(6, OracleTypes.NUMBER);
              // ...
                logger.severe(" save_candles_search_params before execute ");
                statement.execute();
                lRec_id = statement.getLong(6);
            }
        catch (Exception ex) {
            logger.severe(" save_candles_search_params exception " + ex.toString());
            return null;
        }
     return lRec_id;
    }
//////////////////////////////////////////////////////////////////////////////
    public static void get_rates_by_ticker(String ticker,
                                           OffsetDateTime from,
                                           OffsetDateTime till,
                                           Logger logger) throws SQLException,ClassNotFoundException {
        System.setProperty("oracle.net.tns_admin", "C:/Oracle/oraclient18/NETWORK/ADMIN");
        String sql = "BEGIN LLC_MSC_UTILS.GET_RATES_BY_TICKET(?,?,?,?,?); END;";

        //String dbURL = "jdbc:oracle:thin:@RBR";
        String dbURL = property.getProperty("dbURL");
        logger.severe(" get_rates_by_ticker dbURL " + dbURL);
        Class.forName ("oracle.jdbc.OracleDriver");
        java.sql.Connection conn = null;

        String sErr_Msg;
        try (
                java.sql.Connection connection = conn = DriverManager.getConnection(dbURL, cUSER, cPWD);
                CallableStatement  statement = connection.prepareCall(sql);
        ) {
            statement.registerOutParameter(1, OracleTypes.VARCHAR);
            statement.setString(2,ticker);
            statement.setString(3,""+ from);
            statement.setString(4,""+ till);
            statement.registerOutParameter(5, Types.ARRAY, "LLC_NUMBER_NTT");
            // ...
            logger.severe(" get_rates_by_ticker before execute ");
            statement.execute();
            System.out.println("1");
            sErr_Msg = statement.getString(1);
            System.out.println("2");
            nums = (BigDecimal[]) (statement.getArray(5).getArray());
            System.out.println("3");
            System.out.println(Arrays.toString(nums));

            logger.severe(" sErr_Msg " + sErr_Msg + " nums "+Arrays.toString(nums));
        }
        catch (Exception ex) {
            logger.severe(" save_candles_search_params exception " + ex.toString());

        }

    }

    //////////////////////////////////////////////////////////////////////////////
    public static void get_tickers(Logger logger) throws SQLException,ClassNotFoundException {
        System.setProperty("oracle.net.tns_admin", "C:/Oracle/oraclient18/NETWORK/ADMIN");
        String sql = "SELECT TICKER FROM LLC_MS WHERE ROWNUM <5";

        //String dbURL = "jdbc:oracle:thin:@RBR";
        String dbURL = property.getProperty("dbURL");

        Class.forName ("oracle.jdbc.OracleDriver");
        //java.sql.Connection conn = null;

        try (
                java.sql.Connection connection = DriverManager.getConnection(dbURL, cUSER, cPWD);
                //CallableStatement  statement = connection.prepareCall(sql);
                PreparedStatement statement = connection.prepareStatement(sql);

        ) {
            ResultSet rs = statement.executeQuery(sql);

            tickers = new String[4];
            System.out.println(1);
            while (rs.next()) {
                System.out.println(2);
                String coffeeName = rs.getString("TICKER");
                System.out.println(3);
                System.out.println(coffeeName );
                tickers[0] = coffeeName;
                System.out.println(4 );
                //System.out.println(coffeeName );
            }
            /*while (rs.next()) {
                Array z = rs.getArray("TICKER");
                //String[] zips = (String[])z.getArray();
                tickers = (String[])z.getArray();
                for (int i = 0; i < tickers.length; i++) {
                    System.out.println(tickers[i]);
                }
            }*/
        }
        catch (Exception ex) {
            logger.severe(" get_tickers exception " + ex.toString());

        }
            System.out.println("10");

    }
//////////////////////////////////////////////////////////////////////////////////////////
    public static void save_forecast_candles(BigDecimal[] entities,
                                             String ticker,
                                             OffsetDateTime from,
                                             OffsetDateTime to,
                                             Logger logger) throws SQLException,ClassNotFoundException {
        System.setProperty("oracle.net.tns_admin", "C:/Oracle/oraclient18/NETWORK/ADMIN");


        String dbURL = property.getProperty("dbURL");
        Class.forName ("oracle.jdbc.OracleDriver");
        java.sql.Connection conn = null;
        long FC_H_ID;
        FC_H_ID = 0;
        logger.severe("save_forecast_candles starts from " + from + " to "  + to);
        String sql = "BEGIN INSERT INTO LLC_FC_H (TICKER,FROM_DATE,TILL_DATE) Values (?,?,?) returning REC_ID into ?; END;";
        try (
                java.sql.Connection connection = conn = DriverManager.getConnection(dbURL, cUSER, cPWD);
                CallableStatement  statement = connection.prepareCall(sql);
        ) {


                statement.setString(1, ticker);
                statement.setString(2, "" + from);
                statement.setString(3, "" + to);

            statement.registerOutParameter(4, OracleTypes.NUMBER);
            // ...
            logger.severe(" save_forecast_candles before execute ");
            statement.execute();
            FC_H_ID = statement.getLong(4);


        }
        catch (Exception ex) {
            logger.severe(" save exception " + ex.toString());
        }


         sql = "INSERT INTO LLC_FC_B (TICKER,PRICE_F,CANDLE_TIME,FC_H_ID) Values (?,?,?,?)";

        try (
                java.sql.Connection connection = conn = DriverManager.getConnection(dbURL, cUSER, cPWD);
                PreparedStatement statement = connection.prepareStatement(sql);
        ) {
            int i = 0;
            for (BigDecimal entity : entities) {

                statement.setString(1, ticker);
                statement.setBigDecimal(2, entity);
                statement.setString(3, "" + to.plusDays(i+1));
                statement.setLong(4,  FC_H_ID);

                statement.addBatch();
                i++;
                if (i % 1000 == 0 || i == entities.length) {
                    statement.executeBatch(); // Execute every 1000 items.
                }
            }
        }
        catch (Exception ex) {
            logger.severe(" save exception " + ex.toString());
        }
    }


}
