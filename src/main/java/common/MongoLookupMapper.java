package common;

/**
 * Created by Chao on 4/4/2017 AD.
 */
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;
import org.bson.Document;

import java.io.Serializable;
import java.util.List;

public interface MongoLookupMapper extends Serializable {

    /**
     * Converts a Mongo document to a list of storm values that can be emitted. This is done to allow a single
     * storm input tuple and a single Mongo document to result in multiple output values.
     * @param input the input tuple.
     * @param doc the mongo document
     * @return a List of storm values that can be emitted. Each item in list is emitted as an output tuple.
     */
    List<Values> toTuple(ITuple input, Document doc, String from, String to);

    Document toDocument(ITuple input);

    /**
     * declare what are the fields that this code will output.
     * @param declarer
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);
}