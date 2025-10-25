package user;

import exception.NoDataFoundException;
import load_balancer.LoadBalancer;

import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class User implements Runnable {
    private final String name;
    private final Random random = new Random();
    private final LoadBalancer loadBalancer;

    public User(String name, LoadBalancer loadBalancer) {
        this.name = name;
        this.loadBalancer = loadBalancer;
    }

    @Override
    public void run() {
        while(true) {
            int operation = random.nextInt(0, 3);
            String key = name + "-key-" + random.nextInt(0, 5);
            if(operation == 0) {
                System.out.println(name + " add data operation");
                String value = name + "-value-" + random.nextInt(0, 5);
                try {
                    loadBalancer.addData(key, value);
                } catch (NoSuchAlgorithmException e) {
                    System.out.println(e.getMessage());
                }
            } else if(operation == 1) {
                System.out.println(name + " remove data operation");
                try {
                    loadBalancer.removeData(key);
                } catch (NoSuchAlgorithmException | NoDataFoundException e) {
                    System.out.println(e.getMessage());
                }
            } else {
                System.out.println(name + " get data operation");
                try {
                    String value = loadBalancer.getData(key);
                    System.out.printf("%s: key: %s -> value: %s\n", name, key, value);
                } catch (NoSuchAlgorithmException | NoDataFoundException e) {
                    System.out.println(e.getMessage());
                }
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
