package common;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Chao on 4/4/2017 AD.
 */
public class AthenaUpdateMapper implements MongoLookupMapper{

    private String[] fields;

    public AthenaUpdateMapper(String... fields) {
        this.fields = fields;
    }

    public Document toDocument(ITuple tuple) {
        Document document = new Document();
        for(String field : fields){
            document.append(field, tuple.getValueByField(field));
        }
        //$set operator: Sets the value of a field in a document.
        return new Document("$push", document);
    }

    @Override
    public List<Values> toTuple(ITuple input, Document doc, String from, String to) {
        Values values = new Values();

        for(String field : fields) {
            if (from != null && to != null) {
                if (field.equals(from)) {
                    field = to;
                }
            }
            if(input.contains(field)) {
                values.add(input.getValueByField(field));
            } else {
                values.add(doc.get(field));
            }
        }
        List<Values> result = new ArrayList<Values>();
        result.add(values);
        return result;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(fields));
    }

    public AthenaUpdateMapper withFields(String... fields) {
        this.fields = fields;
        return this;
    }
}