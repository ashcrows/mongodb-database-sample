import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoException;

public class MongoDBConnectionCheck {
    public static void main(String[] args) {
        // Replace with your actual MongoDB URI
        String uri = "mongodb+srv://g24ai1067:WHS9UBshA2HO0tT8@clusterassignment.l3zpqum.mongodb.net/?retryWrites=true&w=majority&appName=Clusterassignment";

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            // Attempt to connect and get database names
            MongoDatabase database = mongoClient.getDatabase("admin");

            // Perform a ping command
            org.bson.Document ping = new org.bson.Document("ping", 1);
            org.bson.Document commandResult = database.runCommand(ping);

            System.out.println("MongoDB connection successful: " + commandResult.toJson());
        } catch (MongoException e) {
            System.err.println("MongoDB connection failed: " + e.getMessage());
        }
    }
}
