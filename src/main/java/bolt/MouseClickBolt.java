package bolt;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import common.MongoLookupMapper;
import org.apache.commons.lang.Validate;
import org.apache.storm.mongodb.bolt.AbstractMongoBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;

/**
 * Basic bolt for writing to MongoDB.
 *
 * Note: Each MongoInsertBolt defined in a topology is tied to a specific collection.
 *
 */
public class MouseClickBolt extends AbstractMongoBolt {

    private static final int DEFAULT_FLUSH_INTERVAL_SECS = 1;

    private MongoLookupMapper mapper;

    private boolean ordered = true;  //default is ordered.

    private int batchSize;

    private BatchHelper batchHelper;

    private int flushIntervalSecs = DEFAULT_FLUSH_INTERVAL_SECS;

    public MouseClickBolt(String url, String collectionName, MongoLookupMapper mapper) {
        super(url, collectionName);

        Validate.notNull(mapper, "MongoMapper can not be null");

        this.mapper = mapper;
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            if(batchHelper.shouldHandle(tuple)){
                batchHelper.addBatch(tuple);
            }

            if(batchHelper.shouldFlush()) {
                flushTuples();
                batchHelper.ack();
            }
        } catch (Exception e) {
            batchHelper.fail(e);
        }
    }

    private void flushTuples(){
        List<Document> docs = new LinkedList<>();
        for (Tuple t : batchHelper.getBatchTuples()) {
            Document doc = mapper.toDocument(t);
            docs.add(doc);
        }
        mongoClient.insert(docs, ordered);
        updateUser(docs);
    }

    private void updateUser(List<Document> docs) {

    }

    public MouseClickBolt withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public MouseClickBolt withOrdered(boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    public MouseClickBolt withFlushIntervalSecs(int flushIntervalSecs) {
        this.flushIntervalSecs = flushIntervalSecs;
        return this;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), flushIntervalSecs);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.batchHelper = new BatchHelper(batchSize, collector);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}