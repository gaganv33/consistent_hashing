package message_queue;

import data.Message;

import java.util.ArrayList;
import java.util.List;

public class MessageQueue {
    private static final Object lock = new Object();
    private final List<Message> messages;

    public MessageQueue() {
        messages = new ArrayList<>();
    }

    public void produce(Message message) {
        synchronized (lock) {
            messages.add(message);
            for(var x : messages) System.out.print(x.name() + " ");
            System.out.println();
            lock.notify();
        }
    }

    public List<Message> consume() throws InterruptedException {
        synchronized (lock) {
            while(messages.isEmpty()) {
                lock.wait();
            }
            List<Message> consumer_messages = new ArrayList<>(messages);
            messages.clear();
            lock.notifyAll();
            return consumer_messages;
        }
    }
}
