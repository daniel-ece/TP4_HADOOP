package tp4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseTest {

    private static Configuration conf = null;
    /**
     * Initialization
     */
    static {
        conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    }

    /**
     * Create a table
     */
    public static void createTable(String tableName, String[] familys)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Put a new record
     */
    public static void addNewEntry(String tableName) throws Exception {
        Scanner s = new Scanner(System.in);

        System.out.print("Name (ID): ");
        String id = s.nextLine();

        System.out.print("Gender: ");
        String gender = s.nextLine();

        System.out.print("Age: ");
        String age = s.nextLine();

        System.out.print("Address: ");
        String address = s.nextLine();

        System.out.print("Mail: ");
        String mail = s.nextLine();

        System.out.print("Tel: ");
        String tel = s.nextLine();

        System.out.print("Others information: ");
        String information = s.nextLine();


        System.out.print("Your best friend ? ");
        String bff = s.nextLine();

        System.out.println("Other friends ? (If you have more than one, write it separating them by ';') ");
        String otherFriends[] = s.nextLine().split(";");

        HBaseTest.addRecord(tableName, id, "info", "gender", gender);
        HBaseTest.addRecord(tableName, id, "info", "age", age);
        HBaseTest.addRecord(tableName, id, "info", "address", address);
        HBaseTest.addRecord(tableName, id, "info", "mail", mail);
        HBaseTest.addRecord(tableName, id, "info", "tel", tel);
        HBaseTest.addRecord(tableName, id, "info", "information", information);

        HBaseTest.addRecord(tableName, id, "friends", "bff", bff);
        for(int i=0; i<otherFriends.length; i++) {
            HBaseTest.addRecord(tableName, id, "friends", "others"+i, otherFriends[i]);
        }
    }
    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }

    /**
     * Get a row
     */
    public static void getOneRecord (String tableName) throws IOException{
        HTable table = new HTable(conf, tableName);
        String rowKey;
        Scanner s = new Scanner(System.in); //read in console
        System.out.print("ID : ");
        rowKey = s.nextLine();
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
    }
    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try{
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
                System.out.println();
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] agrs) {

        Scanner s = new Scanner(System.in); //read in console
        String choice = "0"; // choice of the user
        try {
            String tablename = "mle";
            String[] familys = { "info", "friends" };
            HBaseTest.createTable(tablename, familys);

            //REPL to fill up the database
            while(!choice.equals("4")) {
                System.out.println("--- Social network with HBase ---");
                System.out.println("1. Add record");
                System.out.println("2. Show one record");
                System.out.println("3. Show all records");
                System.out.println("4. Exit\n");

                System.out.print("Your choice : ");

                choice = s.nextLine();

                switch (choice) {
                    //1. Add record
                    case "1":
                        System.out.print(String.format("\033[H\033[2J")); //clear bash
                        System.out.println("--- Add record ---\n");
                        HBaseTest.addNewEntry(tablename);
                        System.out.println();
                        break;
                    //2. Show one record
                    case "2":
                        System.out.print(String.format("\033[H\033[2J")); //clear bash
                        System.out.println("--- Get one record ---\n");
                        HBaseTest.getOneRecord(tablename);
                        System.out.println();
                        break;
                    //3. Show all records
                    case "3":
                        System.out.print(String.format("\033[H\033[2J")); //clear bash
                        System.out.println("---  All records  ---\n");
                        HBaseTest.getAllRecord(tablename);
                        System.out.println();
                        break;
                    case "4":
                        break;
                    default:
                        System.out.print(String.format("\033[H\033[2J")); //clear bash
                        System.out.println("Invalid choice, try again !");
                        break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}