package bolt;

/**
 * Created by Chao on 4/7/2017 AD.
 */
import com.mongodb.client.model.Filters;
import common.AthenaMongoClient;
import common.AthenaQueryFilterCreator;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ActionPusherBolt extends BaseRichBolt {

    private static final int DEFAULT_FLUSH_INTERVAL_SECS = 1;

    private AthenaQueryFilterCreator queryCreator;
    private String url, collectionName;

    private AthenaMongoClient mongoClient;
    private OutputCollector collector;
    private String[] actionFields;

    private BatchHelper batchHelper;
    private int batchSize = 15000;
    private int flushIntervalSecs = DEFAULT_FLUSH_INTERVAL_SECS;

    private boolean upsert;  //the default is false.
    private boolean many;  //the default is false.

    public ActionPusherBolt(String url, String collectionName, AthenaQueryFilterCreator queryCreator, String[] actionsFields) {
        this.url = url;
        this.collectionName = collectionName;
        this.actionFields = actionsFields;

//        Validate.notNull(queryCreator, "QueryFilterCreator can not be null");

        this.queryCreator = queryCreator;
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

    private Document toDocument(ITuple tuple) {
        Document document = new Document();
        for(String field : actionFields) {
            if(tuple.contains(field)) {
                document.append(field, tuple.getValueByField(field));
            } else {
                document.append(field, "To be added");
            }
        }
        List<Document> documentList = new ArrayList<>();
        documentList.add(document);
        Document pushObject = new Document();
        pushObject.append("$each", documentList);
        pushObject.append("$sort", new Document("issueTime", 1));
        return new Document("$push", new Document("actions", pushObject));
    }

    private void flushTuples(){
        for (Tuple t : batchHelper.getBatchTuples()) {
            Bson idFilter;
            Document updateDoc = toDocument(t);
            if (t.contains("_id")) {
                idFilter = Filters.eq("_id", t.getValueByField("_id"));
                mongoClient.update(idFilter, updateDoc, upsert, many);
                this.collector.ack(t);
            } else {
                Bson filter = queryCreator.customCreateSession(t);
                Bson timeFilter = Filters.eq("issueTime", -1);
                Document targetSession = mongoClient.findLatest(filter, timeFilter).first();
                idFilter = Filters.eq("_id", targetSession.get("_id"));
                mongoClient.update(idFilter, updateDoc, upsert, many);
            }
        }
    }

    public ActionPusherBolt withUpsert(boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    public ActionPusherBolt withMany(boolean many) {
        this.many = many;
        return this;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), flushIntervalSecs);
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.mongoClient = new AthenaMongoClient(url, collectionName);
        this.batchHelper = new BatchHelper(batchSize, collector);
    }

    public void cleanup() {
        this.mongoClient.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}