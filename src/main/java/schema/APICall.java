package schema;

import org.apache.storm.tuple.Values;
import schema.actions.APICallAction;

import java.util.Date;

/**
 * Created by Chao on 4/10/2017 AD.
 */
public class APICall implements AthenaObject {
    private Date issueTime;
    private String type;
    private String API_KEY_PUBLIC;
    private String deviceCode;
    private String userCode;
    private APICallAction action;

    public Values getValueList() {
        Values values = new Values();
        values.add(this.issueTime);
        values.add(this.type.replace("on", ""));
        values.add(this.API_KEY_PUBLIC);
        values.add(this.deviceCode);
        values.add(this.userCode);
        values.add(this.action.getUrl());
        values.add(this.action.getMethod());
        return values;
    }
}
