package com.examples.test;

public class UserSingleton {
    public static int i;

    static {
        System.out.println("UserSingleton load.....");
        i = 5;
    }
    private  UserSingleton(){}


    private static class UserSingletonHolder{

        private static UserSingleton instance = new UserSingleton();

        static {
            System.out.println("UserSingletonHolder load.....");
        }
    }

    public static UserSingleton getInstance(){
            return UserSingletonHolder.instance;
    }

    public static void main(String[] args) {
        System.out.println(UserSingleton.i);
        UserSingleton.getInstance();
    }

}
