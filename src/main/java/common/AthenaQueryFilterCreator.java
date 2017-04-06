package common;

/**
 * Created by Chao on 4/4/2017 AD.
 */

import org.apache.storm.mongodb.common.QueryFilterCreator;
import org.apache.storm.tuple.ITuple;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Filters;

import java.io.Serializable;

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
            } else {
//                values.add(doc.get(field));
                if (baseFilter == null) {
                    baseFilter = Filters.eq(field, "To be added");
                } else {
                    baseFilter = Filters.and(baseFilter, Filters.eq(field, "To be added"));
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
                    baseFilter = Filters.eq(field, input.getValueByField(field));
                } else {
                    baseFilter = Filters.or(baseFilter, Filters.eq(field, input.getValueByField(field)));
                }
            } else {
//                values.add(doc.get(field));
                if (baseFilter == null) {
                    baseFilter = Filters.eq(field, "To be added");
                } else {
                    baseFilter = Filters.or(baseFilter, Filters.eq(field, "To be added"));
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
