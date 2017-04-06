package bolt;

/**
 * Created by Chao on 4/4/2017 AD.
 */
import common.AthenaMongoClient;
import common.AthenaQueryFilterCreator;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Map;

public class ResultUpdateBolt extends BaseRichBolt {

    private AthenaQueryFilterCreator queryCreator;
    private String url, collectionName, type;

    private AthenaMongoClient mongoClient;
    private OutputCollector collector;

    private boolean upsert;  //the default is false.
    private boolean many;  //the default is false.

    public ResultUpdateBolt(String url, String collectionName, AthenaQueryFilterCreator queryCreator, String type) {
        this.url = url;
        this.collectionName = collectionName;
        this.type = type;

//        Validate.notNull(queryCreator, "QueryFilterCreator can not be null");

        this.queryCreator = queryCreator;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            return;
        }

        try{
            //get document
            Document doc = toDocument(tuple);
            //get query filter
            Bson filter = queryCreator.createFilter(tuple);
            mongoClient.update(filter, doc, upsert, many);
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    private Document toDocument(ITuple tuple) {
        Document document = new Document();
        document.append("actionId", tuple.getValueByField("actionId"));
        document.append("type", this.type);
        //$set operator: Sets the value of a field in a document.
        return new Document("$push", new Document("actionList", document));
    }

    public ResultUpdateBolt withUpsert(boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    public ResultUpdateBolt withMany(boolean many) {
        this.many = many;
        return this;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.mongoClient = new AthenaMongoClient(url, collectionName);
    }

    public void cleanup() {
        this.mongoClient.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}