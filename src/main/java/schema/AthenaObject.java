package schema;

import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Chao on 4/11/2017 AD.
 */
public class AthenaObject {
    private Date issueTime;
    private String type;
    private String API_KEY_PUBLIC;
    private String deviceCode;
    private String userCode;
    private String[] versions;
    private String sessionCode;

    public Date getIssueTime() {
        return issueTime;
    }

    public String getType() {
        return type;
    }

    public String getAPI_KEY_PUBLIC() {
        return API_KEY_PUBLIC;
    }

    public String getDeviceCode() {
        return deviceCode;
    }

    public String getUserCode() {
        return userCode;
    }

    public List<ObjectId> getVersions() {
        List<ObjectId> versionList = new ArrayList<>();
        for (String s : versions) {
            versionList.add(new ObjectId(s));
        }
        return versionList;
    }

    public String getSessionCode() {
        return sessionCode;
    }
}
