package io.confluent.learning;

import java.util.Random;

public class User {
    public String id;
    public String name;
    public int age;
    public String email;

    private static final String[] names = {"Alice", "Bob", "Carol", "Dave", "Eve"};
    private static final Random rand = new Random();

    public static User random() {
        User u = new User();
        u.id = "user-" + rand.nextInt(10000);
        u.name = names[rand.nextInt(names.length)];
        u.age = 18 + rand.nextInt(50);
        u.email = u.name.toLowerCase() + "@example.com";
        return u;
    }
}
