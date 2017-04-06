package common;

import org.apache.storm.tuple.ITuple;
import org.bson.Document;

/**
 * Created by Chao on 4/4/2017 AD.
 */
public class AthenaUpdateMapper {

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

    public AthenaUpdateMapper withFields(String... fields) {
        this.fields = fields;
        return this;
    }
}