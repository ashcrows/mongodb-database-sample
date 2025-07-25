import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.excludeId;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoDBAssignment07 {
    public static final String DATABASE_NAME = "bigdata_tpch";
    public MongoClient mongoClient;
    public MongoDatabase db;

    public static void main(String[] args) throws Exception {
        MongoDBAssignment07 qmongo = new MongoDBAssignment07();
        Scanner scanner = new Scanner(System.in);

        qmongo.connect();

        System.out.print("Run load()? (yes/no): ");
        if (scanner.nextLine().trim().equalsIgnoreCase("yes")) {
            qmongo.load();
        }

        System.out.print("Run loadNest()? (yes/no): ");
        if (scanner.nextLine().trim().equalsIgnoreCase("yes")) {
            qmongo.loadNest();
        }

        System.out.print("Run query1 (get customer name by custkey)? (yes/no): ");
        if (scanner.nextLine().trim().equalsIgnoreCase("yes")) {
            System.out.print("Enter custkey: ");
            int custkey = Integer.parseInt(scanner.nextLine());
            System.out.println(qmongo.query1(custkey));
        }

        System.out.print("Run query2 (get order date by order ID)? (yes/no): ");
        if (scanner.nextLine().trim().equalsIgnoreCase("yes")) {
            System.out.print("Enter order ID: ");
            int orderId = Integer.parseInt(scanner.nextLine());
            System.out.println(qmongo.query2(orderId));
        }

        System.out.print("Run query2Nest (get order date by order ID in nested)? (yes/no): ");
        if (scanner.nextLine().trim().equalsIgnoreCase("yes")) {
            System.out.print("Enter order ID: ");
            int orderId = Integer.parseInt(scanner.nextLine());
            System.out.println(qmongo.query2Nest(orderId));
        }

        System.out.print("Run query3 (total orders from orders collection)? (yes/no): ");
        if (scanner.nextLine().trim().equalsIgnoreCase("yes")) {
            System.out.println(qmongo.query3());
        }

        System.out.print("Run query3Nest (total orders from custorders)? (yes/no): ");
        if (scanner.nextLine().trim().equalsIgnoreCase("yes")) {
            System.out.println(qmongo.query3Nest());
        }

        System.out.print("Run query4 (top 5 customers by total order amount)? (yes/no): ");
        if (scanner.nextLine().trim().equalsIgnoreCase("yes")) {
            System.out.println(toString(qmongo.query4()));
        }

        System.out.print("Run query4Nest (top 5 customers by total order amount - nested)? (yes/no): ");
        if (scanner.nextLine().trim().equalsIgnoreCase("yes")) {
            System.out.println(toString(qmongo.query4Nest()));
        }

        scanner.close();
    }

    public MongoDatabase connect() {
        try {
            String uri = "mongodb+srv://<username>:<passw>@<cluster-name>.l3zpqum.mongodb.net/?retryWrites=true&w=majority&appName=<cluster-name>";
            mongoClient = MongoClients.create(uri);
            db = mongoClient.getDatabase(DATABASE_NAME);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return db;
    }

    public void load() throws Exception {
        MongoCollection<Document> custCol = db.getCollection("customer");
        MongoCollection<Document> orderCol = db.getCollection("orders");
        custCol.drop();
        orderCol.drop();

        List<Document> customers = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("data/customer.tbl"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split("\\|");
                Document doc = new Document("custkey", Integer.parseInt(f[0]))
                        .append("name", f[1])
                        .append("address", f[2])
                        .append("nationkey", Integer.parseInt(f[3]))
                        .append("phone", f[4])
                        .append("acctbal", Double.parseDouble(f[5]))
                        .append("mktsegment", f[6])
                        .append("comment", f[7]);
                customers.add(doc);
            }
        }
        custCol.insertMany(customers);

        List<Document> orders = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("data/order.tbl"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] f = line.split("\\|");
                Document doc = new Document("orderkey", Integer.parseInt(f[0]))
                        .append("custkey", Integer.parseInt(f[1]))
                        .append("orderstatus", f[2])
                        .append("totalprice", Double.parseDouble(f[3]))
                        .append("orderdate", f[4])
                        .append("orderpriority", f[5])
                        .append("clerk", f[6])
                        .append("shippriority", Integer.parseInt(f[7]))
                        .append("comment", f[8]);
                orders.add(doc);
            }
        }
        orderCol.insertMany(orders);
    }

    public void loadNest() throws Exception {
        MongoCollection<Document> nested = db.getCollection("custorders");
        nested.drop();

        MongoCollection<Document> custCol = db.getCollection("customer");
        MongoCollection<Document> orderCol = db.getCollection("orders");

        List<Document> bulkInsert = new ArrayList<>();
        for (Document customer : custCol.find()) {
            int custkey = customer.getInteger("custkey");
            List<Document> custOrders = orderCol.find(eq("custkey", custkey)).into(new ArrayList<>());
            customer.append("orders", custOrders);
            bulkInsert.add(customer);
        }
        nested.insertMany(bulkInsert);
    }

    public String query1(int custkey) {
        Document doc = db.getCollection("customer").find(eq("custkey", custkey))
                .projection(excludeId()).first();
        return doc != null ? doc.toJson() : "Customer not found.";
    }

    public String query2(int orderId) {
        Document doc = db.getCollection("orders").find(eq("orderkey", orderId))
                .projection(excludeId()).first();
        return doc != null ? doc.getString("orderdate") : "Order not found.";
    }

    public String query2Nest(int orderId) {
        Document match = db.getCollection("custorders").find(eq("orders.orderkey", orderId)).first();
        if (match != null) {
            List<Document> orders = (List<Document>) match.get("orders");
            for (Document o : orders) {
                if (o.getInteger("orderkey") == orderId) {
                    return o.getString("orderdate");
                }
            }
        }
        return "Order not found.";
    }

    public long query3() {
        return db.getCollection("orders").countDocuments();
    }

    public long query3Nest() {
        long total = 0;
        for (Document doc : db.getCollection("custorders").find()) {
            List<Document> orders = (List<Document>) doc.get("orders");
            total += orders.size();
        }
        return total;
    }

    public MongoCursor<Document> query4() {
        List<Bson> pipeline = Arrays.asList(
            new Document("$group", new Document("_id", "$custkey")
                    .append("totalAmount", new Document("$sum", "$totalprice"))),
            new Document("$sort", new Document("totalAmount", -1)),
            new Document("$limit", 5)
        );
        return db.getCollection("orders").aggregate(pipeline).iterator();
    }

    public MongoCursor<Document> query4Nest() {
        List<Bson> pipeline = Arrays.asList(
            new Document("$project", new Document("name", 1)
                    .append("totalAmount", new Document("$sum", "$orders.totalprice"))),
            new Document("$sort", new Document("totalAmount", -1)),
            new Document("$limit", 5)
        );
        return db.getCollection("custorders").aggregate(pipeline).iterator();
    }

    public static String toString(MongoCursor<Document> cursor) {
        StringBuilder buf = new StringBuilder("Rows:\n");
        int count = 0;
        if (cursor != null) {
            while (cursor.hasNext()) {
                buf.append(cursor.next().toJson()).append("\n");
                count++;
            }
            cursor.close();
        }
        buf.append("Number of rows: ").append(count);
        return buf.toString();
    }
}
