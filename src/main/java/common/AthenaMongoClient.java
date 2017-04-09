package common;

/**
 * Created by Chao on 4/4/2017 AD.
 */

import java.util.List;

import com.mongodb.client.FindIterable;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOptions;

public class AthenaMongoClient {

    private MongoClient client;
    private MongoCollection<Document> collection;

    public AthenaMongoClient(String url, String collectionName) {
        //Creates a MongoURI from the given string.
        MongoClientURI uri = new MongoClientURI(url);
        //Creates a MongoClient described by a URI.
        this.client = new MongoClient(uri);
        //Gets a Database.
        MongoDatabase db = client.getDatabase(uri.getDatabase());
        //Gets a collection.
        this.collection = db.getCollection(collectionName);
    }

    /**
     * Inserts one or more documents.
     * This method is equivalent to a call to the bulkWrite method.
     * The documents will be inserted in the order provided,
     * stopping on the first failed insertion.
     *
     * @param documents
     */
    public void insert(List<Document> documents, boolean ordered) {
        InsertManyOptions options = new InsertManyOptions();
        if (!ordered) {
            options.ordered(false);
        }
        collection.insertMany(documents, options);
    }

    public void insertOne(Document document) {
        collection.insertOne(document);
    }

    /**
     * Update a single or all documents in the collection according to the specified arguments.
     * When upsert set to true, the new document will be inserted if there are no matches to the query filter.
     *
     * @param filter
     * @param document
     * @param upsert a new document should be inserted if there are no matches to the query filter
     * @param many whether find all documents according to the query filter
     */
    public void update(Bson filter, Bson document, boolean upsert, boolean many) {
        //TODO batch updating
        UpdateOptions options = new UpdateOptions();
        if (upsert) {
            options.upsert(true);
        }
        if (many) {
            collection.updateMany(filter, document, options);
        }else {
            collection.updateOne(filter, document, options);
        }
    }

    /**
     * Finds a single document in the collection according to the specified arguments.
     *
     * @param filter
     */
    public Document find(Bson filter) {
        //TODO batch finding
        return collection.find(filter).first();
    }

    public FindIterable<Document> findLatest(Bson filter, Bson field) {
        return collection.find(filter).sort(field).limit(1);
    }

    /**
     * Closes all resources associated with this instance.
     */
    public void close(){
        client.close();
    }

    public Document findAndInsert(Bson filter, Document updateDocument) {
        Document update = new Document("$setOnInsert", updateDocument);

        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions();

        options.returnDocument(ReturnDocument.AFTER);
        options.upsert(true);

        return collection.findOneAndUpdate(filter, update, options);
    }
}