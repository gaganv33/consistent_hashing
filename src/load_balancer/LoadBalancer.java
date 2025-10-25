package load_balancer;

import data.DatabaseNodeData;
import data.Message;
import database.DatabaseNode;
import database.DatabaseNodeHelper;
import exception.NoDataFoundException;
import message_queue.MessageQueue;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class LoadBalancer {
    private int total_alive_database_nodes;
    private final HashMap<String, DatabaseNode> databases = new HashMap<>();
    private final HashMap<String, Integer> hashed_database_indices = new HashMap<>();
    private final MessageQueue messageQueue = new MessageQueue();
    private List<DatabaseNodeData> available_database_nodes;

    public LoadBalancer(int total_databases) throws NoSuchAlgorithmException {
        this.total_alive_database_nodes = total_databases;
        this.available_database_nodes = new ArrayList<>();
        for(int i = 0; i < total_databases; i++) {
            String database_name = "Database-" + (i + 1);
            databases.put(database_name, new DatabaseNode(database_name, messageQueue));
        }
        hashTheDatabase();
        createAvailableDatabaseNodes();
        startTheDaemonThreadToListenForAliveAndNotAliveDatabases();
    }

    public void addData(String key, String value) throws NoSuchAlgorithmException {
        DatabaseNode databaseNode = getDatabaseNode(key);
        DatabaseNodeHelper.addData(databaseNode, key, value);
    }

    public void removeData(String key) throws NoSuchAlgorithmException, NoDataFoundException {
        DatabaseNode databaseNode = getDatabaseNode(key);
        DatabaseNodeHelper.removeData(databaseNode, key);
    }

    public String getData(String key) throws NoSuchAlgorithmException, NoDataFoundException {
        DatabaseNode databaseNode = getDatabaseNode(key);
        return DatabaseNodeHelper.getData(databaseNode, key);
    }

    private DatabaseNode getDatabaseNode(String key) throws NoSuchAlgorithmException {
        int targetValue = hashing(key);
        int index = getTheNextAvailableNode(targetValue);
        if(index == -1) return databases.get(available_database_nodes.getFirst().name());
        return databases.get(available_database_nodes.get(index).name());
    }

    private void hashTheDatabase() throws NoSuchAlgorithmException {
        System.out.printf("[%s]: Hashing the databases\n", Thread.currentThread().getStackTrace()[1]);
        for(var name : databases.keySet()) {
            int index = hashing(name);
            hashed_database_indices.put(name, index);
        }
    }

    private int hashing(String name) throws NoSuchAlgorithmException {
        MessageDigest md5 = MessageDigest.getInstance("SHA3-256");
        byte[] digest = md5.digest(name.getBytes(StandardCharsets.UTF_8));

        BigInteger hashedBigInteger = new BigInteger(1, digest);
        return hashedBigInteger.mod(BigInteger.valueOf(total_alive_database_nodes)).intValue();
    }

    private void startTheDaemonThreadToListenForAliveAndNotAliveDatabases() {
        Thread daemonThread = new Thread(() -> {
            while(true) {
                try {
                    List<Message> messages = messageQueue.consume();
                    if(canPerformScalingDownVerification(messages)) {
                        System.out.println("Cannot perform simulated scaling down, since there is only 1 database available.");
                        continue;
                    }
                    System.out.printf("[%s]: Some databases status has changed\n", Thread.currentThread().getStackTrace()[1]);
                    System.out.println("Message size: " + messages.size());
                    createAvailableDatabaseNodes(messages);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        daemonThread.setDaemon(true);
        System.out.printf("[%s]: Starting the daemon thread to listen to databases when they are changing status from alive to not alive\n",
                Thread.currentThread().getStackTrace()[1]);
        daemonThread.start();
    }

    private boolean canPerformScalingDownVerification(List<Message> messages) {
        int scalingDownCount = 0;
        for(var message : messages) {
            if(!message.isAlive()) scalingDownCount += 1;
        }
        return scalingDownCount >= total_alive_database_nodes;
    }

    private void createAvailableDatabaseNodes() {
        printDatabaseDetails();
        available_database_nodes = new ArrayList<>(hashed_database_indices.entrySet().stream()
                .map(entry -> new DatabaseNodeData(entry.getKey(), entry.getValue())).toList());
        available_database_nodes.sort(Comparator.comparingInt(DatabaseNodeData::index));
        total_alive_database_nodes = available_database_nodes.size();
        printDatabaseDetails();
    }

    private void createAvailableDatabaseNodes(List<Message> messages) {
        printDatabaseDetails();
        for(var message : messages) {
            int index = getTheNextAvailableNode(hashed_database_indices.get(message.name()));
            System.out.printf("[%s]: index=%d, database_name=%s, is_alive=%s", Thread.currentThread().getStackTrace()[1],
                    index, message.name(), message.isAlive());
            processDatabaseNodes(index, message);
        }
        printDatabaseDetails();
    }

    private void printDatabaseDetails() {
        System.out.print("Details about the available databases: ");
        for(var database : available_database_nodes) System.out.printf("(%s, %d) ",
                database.name(), databases.get(database.name()).getContainer().size());
        System.out.println();
    }

    private int getTheNextAvailableNode(int targetValue) {
        int index = -1;
        int value = Integer.MAX_VALUE;
        int start = 0;
        int end = total_alive_database_nodes - 1;

        while(start <= end) {
            int mid = (end - start) / 2 + start;
            int currentIndex = available_database_nodes.get(mid).index();

            if(currentIndex < targetValue) {
                start = mid + 1;
            } else {
                if(currentIndex < value) {
                    value = currentIndex;
                    index = mid;
                }
                end = mid - 1;
            }
        }
        return index;
    }

    private void processDatabaseNodes(int index, Message message) {
        if(message.isAlive()) {
            processAliveDatabaseNodes(index, message.name());
        } else {
            processNotAliveDataNodes(index);
        }
        total_alive_database_nodes = available_database_nodes.size();
        System.out.println("After processing the consumer messages: " + total_alive_database_nodes);
    }

    private void processNotAliveDataNodes(int index) {
        if(index == (total_alive_database_nodes - 1)) {
            DatabaseNodeHelper.dataMovement(databases.get(available_database_nodes.get(index).name()),
                    databases.get(available_database_nodes.getFirst().name()));
        } else {
            DatabaseNodeHelper.dataMovement(databases.get(available_database_nodes.get(index).name()),
                    databases.get(available_database_nodes.get(index + 1).name()));
        }
        available_database_nodes.remove(index);
    }

    private void processAliveDatabaseNodes(int index, String name) {
        DatabaseNode databaseNode = databases.get(name);
        System.out.println("name: " + name + " index: " + index);
        if(index == 0) {
            DatabaseNodeHelper.dataMovement(
                    databases.get(available_database_nodes.get(total_alive_database_nodes - 1).name()),
                    databaseNode
            );
            available_database_nodes.add(index, new DatabaseNodeData(name, hashed_database_indices.get(name)));
        } else if(index == -1) {
            DatabaseNodeHelper.dataMovement(
                    databases.get(available_database_nodes.get(total_alive_database_nodes - 1).name()),
                    databaseNode
            );
            available_database_nodes.add(new DatabaseNodeData(name, hashed_database_indices.get(name)));
        } else {
            DatabaseNodeHelper.dataMovement(
                    databases.get(available_database_nodes.get(index).name()),
                    databaseNode
            );
            available_database_nodes.add(index, new DatabaseNodeData(name, hashed_database_indices.get(name)));
        }
    }
}
