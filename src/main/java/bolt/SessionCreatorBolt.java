package bolt;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import common.MongoLookupMapper;
import org.apache.storm.mongodb.bolt.AbstractMongoBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;


public class SessionCreatorBolt extends AbstractMongoBolt {

    private static final int DEFAULT_FLUSH_INTERVAL_SECS = 1;

    private MongoLookupMapper mapper;
    private MongoLookupMapper updateMapper;

    private boolean ordered = true;  //default is ordered.

    private int batchSize = 15000;

    private BatchHelper batchHelper;

    private int flushIntervalSecs = DEFAULT_FLUSH_INTERVAL_SECS;

    public SessionCreatorBolt(String url, String collectionName, MongoLookupMapper mapper, MongoLookupMapper updateMapper) {
        super(url, collectionName);

//        Validate.notNull(mapper, "MongoMapper can not be null");

        this.mapper = mapper;
        this.updateMapper = updateMapper;
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
        List<Tuple> tuples = new LinkedList<>();
        List<Document> docs = new LinkedList<>();
        for (Tuple t : batchHelper.getBatchTuples()) {
            Document doc = mapper.toDocument(t);
            docs.add(doc);
            tuples.add(t);
        }
        mongoClient.insert(docs, ordered);
        updateUser(docs, tuples);
    }

    private void updateUser(List<Document> docs, List<Tuple> tuples) {
        int i = 0;
        for (Document doc : docs) {
            Tuple tuple = tuples.get(i);
            List<Values> valuesList = updateMapper.toTuple(tuple, doc, null, null);
            for (Values values : valuesList) {
                this.collector.emit(tuple, values);
            }
            i++;
        }
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
        updateMapper.declareOutputFields(declarer);
    }

}