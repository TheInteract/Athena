package common;

import com.google.gson.*;
import org.apache.storm.tuple.Values;
import schema.AthenaInterface;

import java.util.Date;

/**
 * Created by Chao on 4/3/2017 AD.
 */
public class JsonMapper {
    private Gson gson;

    public JsonMapper()
    {
        this.gson = new GsonBuilder().registerTypeAdapter(Date.class, new JsonDateDeserializer()).serializeNulls().create();
    }

    public Values toValues(String input, Class<? extends AthenaInterface> inputClass) {
        AthenaInterface tempObject = this.gson.fromJson(input, inputClass);
        return tempObject.getValueList();
    }
}
