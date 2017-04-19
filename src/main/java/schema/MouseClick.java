package schema;

import org.apache.storm.tuple.Values;
import schema.actions.MouseClickAction;

import java.util.Arrays;
import java.util.Date;

/**
 * Created by Chao on 4/3/2017 AD.
 */
public class MouseClick extends AthenaObject implements AthenaInterface {
    private MouseClickAction action;

    public Values getValueList() {
        Values values = new Values();
        values.add(this.getIssueTime());
        values.add(this.getType().replace("on", ""));
        values.add(this.getAPI_KEY_PUBLIC());
        values.add(this.getVersions());
        values.add(this.getDeviceCode());
        values.add(this.getUserCode());
        values.add(this.getSessionCode());
        values.add(this.action.getTimeStamp());
        values.add(this.action.getTarget());
        return values;
    }
}
