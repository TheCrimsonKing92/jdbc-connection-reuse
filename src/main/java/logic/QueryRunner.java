package logic;

import data.DaoType;
import data.SufficientDataDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class QueryRunner {
    private static final Logger logger = LoggerFactory.getLogger(QueryRunner.class);
    public static DaoType getNextDaoType(final int connectionTimes, final int templateTimes, final int times,
                                         final Random random) {
        if (connectionTimes == times && templateTimes == times) {
            return null;
        }

        if (templateTimes == times && connectionTimes < times) {
            return DaoType.CONNECTION;
        }

        if (connectionTimes == times && templateTimes < times) {
            return DaoType.TEMPLATE;
        }

        return random.nextBoolean() ? DaoType.CONNECTION : DaoType.TEMPLATE;
    }
    private final SufficientDataDao dao;

    public QueryRunner(final SufficientDataDao dao) {
        this.dao = dao;
    }
    public BigDecimal getQueryRuntime() {
        long start = System.nanoTime();
        List<Long> ids = dao.getIds();
        final String[] record = dao.getRecordById(ids.get(0));
        logger.info("Record: {}", Arrays.toString(record));
        final String[] meta = dao.getRecordMetaById(ids.get(0));
        if (meta == null) {
            logger.info("No meta record found");
        } else {
            logger.info("Meta record: {}", Arrays.toString(meta));
        }
        final Map<Long, String[]> recordsById = dao.getRecordsByIds(ids.subList(0, 1000));
        logger.info("Retrieved {} recordsById", recordsById.size());
        final Map<Long, String[]> recordMetasById = dao.getRecordMetasByIds(ids.subList(0, 1000));
        logger.info("Retrieved {} recordMetasById", recordMetasById.size());
        /*
        final List<String[]> templateRecordsWithCreated = dao.getRecordsWithCreated(
                Timestamp.valueOf(
                        templateRecordById[1]
                )
        );
        */
        final List<String[]> recordsWithGenerated = dao.getRecordsWithGenerated(false);
        logger.info("{} records with generated", recordsWithGenerated.size());
        long end = System.nanoTime();

        return new BigDecimal(end - start);
    }

    public List<BigDecimal> getQueryRuntimes(final int times, final long delay) throws InterruptedException {
        final List<BigDecimal> runtimes = new ArrayList<>();

        for (int i = 0; i < times; i++) {
            runtimes.add(getQueryRuntime());

            if (delay > 0 && i < times - 1) {
                logger.info("Sleeping for arbitrary delay between runs");
                Thread.sleep(delay);
            }
        }

        return runtimes;
    }

    public static Map<DaoType, List<BigDecimal>> getQueryRuntimes(final SufficientDataDao connectionDao,
                                                                  final SufficientDataDao templateDao,
                                                                  final int times,
                                                                  final long delay) throws InterruptedException {
        final Map<DaoType, List<BigDecimal>> results = new HashMap<>();
        final QueryRunner connectionRunner = new QueryRunner(connectionDao);
        final QueryRunner templateRunner = new QueryRunner(templateDao);
        results.put(DaoType.CONNECTION, new ArrayList<>());
        results.put(DaoType.TEMPLATE, new ArrayList<>());

        int connectionTimes = 0;
        int templateTimes = 0;

        final Random randomSource = new Random();

        logger.info("Beginning runs");
        while (connectionTimes < times || templateTimes < times) {
            final DaoType type = getNextDaoType(connectionTimes, templateTimes, times, randomSource);

            if (type == null) {
                break;
            }

            if (DaoType.CONNECTION == type) {
                logger.info("Connection dao run {}", connectionTimes);
                results.get(type).add(connectionRunner.getQueryRuntime());
                connectionTimes++;
            } else {
                logger.info("Template dao run {}", templateTimes);
                results.get(type).add(templateRunner.getQueryRuntime());
                templateTimes++;
            }

            if (delay > 0 && (connectionTimes < times || templateTimes < times)) {
                logger.info("Not all runs are done, sleeping to introduce arbitrary separation");
                Thread.sleep(delay);
            }
        }

        logger.info("Done with runs");
        return results;
    }
}
