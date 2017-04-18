package schema;

import org.apache.storm.tuple.Values;
import schema.actions.APICallAction;

import java.util.Arrays;
import java.util.Date;

/**
 * Created by Chao on 4/10/2017 AD.
 */
public class APICall extends AthenaObject implements AthenaInterface {
    private APICallAction action;

    public Values getValueList() {
        Values values = new Values();
        values.add(this.getIssueTime());
        values.add(this.getType().replace("on", ""));
        values.add(this.getAPI_KEY_PUBLIC());
        values.add(Arrays.asList(this.getVersions()));
        values.add(this.getDeviceCode());
        values.add(this.getUserCode());
        values.add(this.getSessionCode());
        values.add(this.action.getUrl());
        values.add(this.action.getMethod());
        return values;
    }
}
