import config.DatabaseConfigurator;
import data.*;
import logic.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static util.Numbers.toPercentage;

public class ConnectionReuseDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionReuseDemo.class);

    private static final int FETCH_SIZE = 10;
    private static final long DELAY = 5000; // 11000;

    private static final long LOOPS = 20;
    private static final int TIMES = 5;

    private static final DatabaseConfigurator configurator;
    private static final SufficientDataDao connectionDao;
    private static final SufficientDataDao templateDao;

    static {
        try {
            configurator = new DatabaseConfigurator(FETCH_SIZE);
            connectionDao = new ConnectionReuseImpl(configurator);
            templateDao = new TemplateImpl(configurator.getTemplate());
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void logOverallResults(final Map<DaoType, List<QueryResultAggregate>> allResults) {
        if (LOOPS > 1 && TIMES > 1 && DELAY > 0) {
            logger.info(
                    "Ran {} main loops, within which we executed the query sequence {} times, with a {} millisecond sleep" +
                    " delay between runs",
                    LOOPS, TIMES, DELAY
            );
        } else if (LOOPS > 1 && TIMES > 1 && DELAY == 0) {
            logger.info(
                    "Ran {} main loops, within which we executed the query sequence {} times",
                    LOOPS, TIMES
            );
        } else if (LOOPS > 1) {
            logger.info(
                    "Ran {} main loops executing the query sequence",
                    LOOPS
            );
        }

        logResultsComparison(
                QueryResultAggregate.reduce(allResults.get(DaoType.CONNECTION)),
                QueryResultAggregate.reduce(allResults.get(DaoType.TEMPLATE))
        );
    }

    private static void logResultsComparison(final Map<DaoType, QueryResultAggregate> resultsMap) {
        logResultsComparison(resultsMap.get(DaoType.CONNECTION), resultsMap.get(DaoType.TEMPLATE));
    }

    private static void logResultsComparison(final QueryResultAggregate connectionResults,
                                             final QueryResultAggregate templateResults) {
        if (TIMES > 1 && DELAY > 0) {
            logger.info(
                    "Finished a main loop, which ran the query sequence {} times, with a {} millisecond sleep delay" +
                    " between runs",
                    TIMES, DELAY
            );
        } else if (TIMES > 1 && DELAY == 0) {
            logger.info(
                    "Finished a main loop, which ran the query sequence {} times",
                    TIMES
            );
        } else {
            logger.info(
                    "Finished a main loop, which ran the query sequence once"
            );
        }

        final QueryResultAggregate connectionMillis = connectionResults.toUnit(SecondsPartUnit.MILLIS);
        final QueryResultAggregate connectionSeconds = connectionMillis.toUnit(SecondsPartUnit.SECONDS);

        logger.info(
                "Connection re-use minimum time: {} ns, {} ms, {} s",
                connectionResults.getMin(), connectionMillis.getMin(), connectionSeconds.getMin()
        );
        logger.info(
                "Connection re-use maximum time: {} ns, {} ms, {} s",
                connectionResults.getMax(), connectionMillis.getMax(), connectionSeconds.getMax()
        );
        logger.info(
                "Connection re-use average time: {} ns, {} ms, {} s",
                connectionResults.getAverage(), connectionMillis.getAverage(), connectionSeconds.getAverage()
        );

        final QueryResultAggregate templateMillis = templateResults.toUnit(SecondsPartUnit.MILLIS);
        final QueryResultAggregate templateSeconds = templateMillis.toUnit(SecondsPartUnit.SECONDS);

        logger.info(
                "Template minimum time: {} ns, {} ms, {} s",
                templateResults.getMin(), templateMillis.getMin(), templateSeconds.getMin()
        );
        logger.info(
                "Template maximum time: {} ns, {} ms, {} s",
                templateResults.getMax(), templateMillis.getMax(), templateSeconds.getMax()
        );
        logger.info(
                "Template average time: {} ns, {} ms, {} s",
                templateResults.getAverage(), templateMillis.getAverage(), templateSeconds.getAverage()
        );

        logger.info(
                "Connection re-use as percentage of template, min: {}, max: {}, average: {}",
                toPercentage(connectionResults.getMin(), templateResults.getMin()),
                toPercentage(connectionResults.getMax(), templateResults.getMax()),
                toPercentage(connectionResults.getAverage(), templateResults.getAverage())
        );
    }

    private static Map<DaoType, QueryResultAggregate> run() throws InterruptedException {
        return Stream.of(connectionDao, templateDao)
                     .collect(
                             Collectors.toMap(
                                     SufficientDataDao::getDaoType,
                                     dao -> {
                                         try {
                                             return new QueryResultAggregate(new QueryRunner(dao).getQueryRuntimes(TIMES, DELAY));
                                         } catch (InterruptedException e) {
                                             throw new RuntimeException(e);
                                         }
                                     }
                             )
                     );
    }

    private static void addResults(final Map<DaoType, List<QueryResultAggregate>> total,
                                   final Map<DaoType, QueryResultAggregate> current) {
        for (final Map.Entry<DaoType, QueryResultAggregate> entry : current.entrySet()) {
            total.putIfAbsent(entry.getKey(), new ArrayList<>());
            total.get(entry.getKey()).add(entry.getValue());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, SQLException {
        final Map<DaoType, List<QueryResultAggregate>> allResults = new HashMap<>();

        for (int i = 0; i < LOOPS; i++) {
            final Map<DaoType, QueryResultAggregate> currentResult = run();
            logResultsComparison(currentResult);
            addResults(allResults, currentResult);
        }

        logOverallResults(allResults);
    }
}