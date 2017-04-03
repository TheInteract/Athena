package schema;

import org.apache.storm.tuple.Values;
import schema.actions.MouseClickAction;

/**
 * Created by Chao on 4/3/2017 AD.
 */
public class MouseClick implements AthenaObject {
    private String issueTime;
    private String type;
    private String API_KEY_PUBLIC;
    private String deviceCode;
    private String userCode;
    private MouseClickAction action;

    public Values getValueList() {
        Values values = new Values();
        values.add(this.issueTime);
        values.add(this.type);
        values.add(this.API_KEY_PUBLIC);
        values.add(this.deviceCode);
        values.add(this.userCode);
        values.add(this.action.getTarget());
        values.add(this.action.getTimeStamp());
        return values;
    }
}
