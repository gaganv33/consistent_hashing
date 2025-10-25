import load_balancer.LoadBalancer;
import user.User;

import java.security.NoSuchAlgorithmException;

public class Main {
    public static void main(String[] args) throws NoSuchAlgorithmException {
        LoadBalancer loadBalancer = new LoadBalancer(4);

        Thread user1 = new Thread(new User("user1", loadBalancer));
        Thread user2 = new Thread(new User("user2", loadBalancer));
        Thread user3 = new Thread(new User("user3", loadBalancer));

        user1.start();
        user2.start();
        user3.start();
    }
}
