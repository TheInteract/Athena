package schema;

import org.apache.storm.tuple.Values;

import java.util.Arrays;

/**
 * Created by Chao on 4/14/2017 AD.
 */
public class Unload extends AthenaObject implements AthenaInterface {

    public Values getValueList() {
        Values values = new Values();
        values.add(this.getIssueTime());
        values.add(this.getType().replace("on", ""));
        values.add(this.getAPI_KEY_PUBLIC());
        values.add(Arrays.asList(this.getVersions()));
        values.add(this.getDeviceCode());
        values.add(this.getUserCode());
        values.add(this.getSessionCode());
        return values;
    }
}