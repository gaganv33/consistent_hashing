package database;

import data.Message;
import exception.NoDataFoundException;
import message_queue.MessageQueue;

import java.util.HashMap;
import java.util.Random;

public class DatabaseNode {
    private final String name;
    private boolean isAlive;
    private final HashMap<String, String> container;
    private final MessageQueue messageQueue;

    public DatabaseNode(String name, MessageQueue messageQueue) {
        this.name = name;
        this.isAlive = true;
        container = new HashMap<>();
        this.messageQueue = messageQueue;
        createDaemonThread();
    }

    public synchronized void addData(String key, String value) {
        container.put(key, value);
    }

    public synchronized void removeData(String key) throws NoDataFoundException {
        if(!container.containsKey(key)) throw new NoDataFoundException("Data for key: " + key + " not present in the database");
        container.remove(key);
    }

    public synchronized String getData(String key) throws NoDataFoundException {
        if(!container.containsKey(key)) throw new NoDataFoundException("Data for key: " + key + " not present in the database");
        return container.get(key);
    }

    public void scalingDown() {
        System.out.printf("[%s].%s: Scaling down database: %s\n", Thread.currentThread().getStackTrace()[1], name, true);
        this.isAlive = false;
        messageQueue.produce(new Message(name, false));
    }

    public void scalingUp() {
        System.out.printf("[%s] %s: Scaling up database: %s\n", Thread.currentThread().getStackTrace()[1], name, true);
        this.isAlive = true;
        messageQueue.produce(new Message(name, true));
    }

    public String getName() {
        return this.name;
    }

    public boolean getIsAlive() {
        return this.isAlive;
    }

    public HashMap<String, String> getContainer() {
        return this.container;
    }

    public void clearContainer() {
        this.container.clear();
    }

    public void createDaemonThread() {
        Thread thread = new Thread(() -> {
            while(true) {
                try {
                    Thread.sleep(1000 * (new Random().nextInt(30)));
                    if(isAlive) {
                        scalingDown();
                    } else {
                        scalingUp();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.setDaemon(true);
        System.out.printf("[%s]: Starting daemon thread to simulate scaling up and down of database\n", Thread.currentThread().getStackTrace()[1]);
        thread.start();
    }
}
