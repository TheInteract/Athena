package common;

/**
 * Created by Chao on 4/4/2017 AD.
 */

import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.tuple.ITuple;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Filters;

import java.io.Serializable;
import java.util.Arrays;

public class AthenaQueryFilterCreator implements QueryFilterCreator {

    private String[] fields;

    @Override
    public Bson createFilter(ITuple input) {
        Bson baseFilter = null;
        for(String field: fields) {
            if(input.contains(field)) {
                if (baseFilter == null) {
                    baseFilter = Filters.eq(field, input.getValueByField(field));
                } else {
                    baseFilter = Filters.and(baseFilter, Filters.eq(field, input.getValueByField(field)));
                }
            }
        }
        return baseFilter;
    }

    public Bson createFilterOr(ITuple input) {
        Bson baseFilter = null;
        for(String field: fields) {
            if(input.contains(field)) {
                if (baseFilter == null) {
                    baseFilter = Filters.eq("userIdentity", input.getValueByField(field));
                } else {
                    baseFilter = Filters.or(baseFilter, Filters.eq("userIdentity", input.getValueByField(field)));
                }
            }
        }
        return baseFilter;
    }

    public Bson customCreateSession(ITuple input) {
        Bson baseFilter = null;
        for(String field: fields) {
            if(input.contains(field)) {
                if (baseFilter == null) {
                    baseFilter = Filters.eq(field, input.getValueByField(field));
                } else {
                    baseFilter = Filters.and(baseFilter, Filters.eq(field, input.getValueByField(field)));
                }
            }
        }
        Bson timeFilter = Filters.lt("issueTime", input.getValueByField("issueTime"));
        baseFilter = Filters.and(baseFilter, timeFilter);
        return baseFilter;
    }

    public Bson createFilterActionType(ITuple input, String[] dataFields) {
        Bson baseFilter = null;
        for(String field: fields) {
            if(input.contains(field)) {
                if (Arrays.asList(dataFields).contains(field)) {
                    if (baseFilter == null) {
                        baseFilter = Filters.eq("data", new Document(field, input.getValueByField(field)));
                    } else {
                        baseFilter = Filters.and(baseFilter, Filters.eq("data", new Document(field, input.getValueByField(field))));
                    }
                } else {
                    if (baseFilter == null) {
                        baseFilter = Filters.eq(field, input.getValueByField(field));
                    } else {
                        baseFilter = Filters.and(baseFilter, Filters.eq(field, input.getValueByField(field)));
                    }
                }
            }
        }
        return baseFilter;
    }

    public AthenaQueryFilterCreator withField(String... field) {
        this.fields = field;
        return this;
    }

    public String[] getFields() {
        return this.fields;
    }

}
