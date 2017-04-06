package bolt;

import common.AthenaLookupMapper;
import common.AthenaMongoClient;
import common.AthenaQueryFilterCreator;
import common.MongoLookupMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Map;

/**
 * Created by Chao on 4/6/2017 AD.
 */
public class SessionTypeCreatorBolt extends BaseRichBolt {

    private AthenaQueryFilterCreator queryCreator;
    private MongoLookupMapper mapper;
    private String url, collectionName;

    private OutputCollector collector;
    private AthenaMongoClient mongoClient;

    public SessionTypeCreatorBolt(String url, String collectionName, AthenaQueryFilterCreator queryCreator, MongoLookupMapper mapper) {
        this.url = url;
        this.mapper = mapper;
        this.collectionName = collectionName;
        this.queryCreator = queryCreator;
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            return;
        }

        try{
            //get query filter
            Bson filter = queryCreator.createFilter(tuple, "type", "startType");
            //find document from mongodb
            AthenaLookupMapper createMapper = new AthenaLookupMapper().withFields(queryCreator.getFields());
            Document updateDocument = createMapper.toDocument(tuple, "type", "startType");
            System.out.println(updateDocument.toString());
            Document doc = mongoClient.findAndInsert(filter, updateDocument);
            //get storm values and emit
            List<Values> valuesList = mapper.toTuple(tuple, doc, "sessionTypeId", "_id");
            for (Values values : valuesList) {
                this.collector.emit(tuple, values);
            }
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.mongoClient = new AthenaMongoClient(url, collectionName);
    }

    public void cleanup() {
        this.mongoClient.close();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        mapper.declareOutputFields(declarer);
    }

}