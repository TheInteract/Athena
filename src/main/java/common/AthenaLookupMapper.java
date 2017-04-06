package common;

/**
 * Created by Chao on 4/4/2017 AD.
 */
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class AthenaLookupMapper implements MongoLookupMapper {

    private String[] fields;

    public AthenaLookupMapper(String... fields) {
        this.fields = fields;
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
//                values.add("To be added");
            }
        }
        List<Values> result = new ArrayList<Values>();
        result.add(values);
        return result;
    }

    @Override
    public Document toDocument(ITuple input) {
        Document document = new Document();
        for(String field : fields){
            if(input.contains(field)) {
                document.append(field, input.getValueByField(field));
            } else {
//                values.add(doc.get(field));
                document.append(field, "To be added");
            }
        }
        //$set operator: Sets the value of a field in a document.
        return document;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(fields));
    }

    public AthenaLookupMapper withFields(String... fields) {
        this.fields = fields;
        return this;
    }

    public String[] getFields() {
        return this.fields;
    }
}