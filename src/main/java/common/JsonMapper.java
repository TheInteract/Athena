package common;

import com.google.gson.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import schema.AthenaObject;

/**
 * Created by Chao on 4/3/2017 AD.
 */
public class JsonMapper {
    private Gson gson;

    public JsonMapper() {
        this.gson = new GsonBuilder().serializeNulls().create();
    }

    public Values toValues(String input, Class<? extends AthenaObject> inputClass) {
        AthenaObject tempObject = this.gson.fromJson(input, inputClass);
        return tempObject.getValueList();
    }
}
