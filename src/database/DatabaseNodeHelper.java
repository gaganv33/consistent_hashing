package database;

import exception.NoDataFoundException;

import java.util.HashMap;

public class DatabaseNodeHelper {
    public static void dataMovement(DatabaseNode from, DatabaseNode to) {
        HashMap<String, String> fromContainer = from.getContainer();
        HashMap<String, String> toContainer = to.getContainer();

        toContainer.putAll(fromContainer);
        from.clearContainer();
    }

    public static void addData(DatabaseNode databaseNode, String key, String value) {
        databaseNode.addData(key, value);
    }

    public static void removeData(DatabaseNode databaseNode, String key) throws NoDataFoundException {
        databaseNode.removeData(key);
    }

    public static String getData(DatabaseNode databaseNode, String key) throws NoDataFoundException {
        return databaseNode.getData(key);
    }
}
