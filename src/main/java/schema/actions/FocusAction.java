package schema.actions;

import java.util.Date;

/**
 * Created by Chao on 4/11/2017 AD.
 */
public class FocusAction {
    private String href;
    private Date timeStamp;
    private int innerHeight;
    private int innerWidth;

    public String getHref() {
        return href;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public int getInnerHeight() {
        return innerHeight;
    }

    public int getInnerWidth() {
        return innerWidth;
    }
}
