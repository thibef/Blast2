package DataStructures;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class DatasetReader {
    public DatasetReader(String d1, String d2, String grnd){
        read(d1,d2,grnd);
    }

    public static List<EntityProfile> read(String d1, String d2, String grnd){
        ArrayList<EntityProfile> read_data;
        ArrayList<EntityProfile> read_data2;
        HashSet<IdDuplicates> duplicates;
        Integer count = 4;
        Object tempOBJ = null;
        Object tempOBJ2 = null;
        Object grTruth = null;

        try {
            ObjectInputStream ois = new ObjectInputStream(
                    new FileInputStream(d1));
            tempOBJ = ois.readObject();

            ObjectInputStream ois2 = new ObjectInputStream(
                    new FileInputStream(d2));
            tempOBJ2 = ois2.readObject();

            ObjectInputStream ois3 = new ObjectInputStream(
                    new FileInputStream(grnd));
            grTruth = ois3.readObject();

            System.out.println("object read");
            read_data = (ArrayList<EntityProfile>) (tempOBJ);
            read_data2 = (ArrayList<EntityProfile>) (tempOBJ2);
            duplicates = (HashSet<IdDuplicates>) grTruth;

            System.out.println("dblp:"+read_data.size()+"\tacm:"+read_data2.size()+"\tDuplicates:"+duplicates.size());

            System.out.println("DBLP");
            for ( Attribute a:read_data.get(1).getAttributes()) {
                System.out.println(a.getName()+":"+a.getValue());
            }
            System.out.println("acm");
            for (Attribute a:read_data2.get(1).getAttributes()) {
                System.out.println(a.getName()+":"+a.getValue());
            }
            Iterator<IdDuplicates> iterate_Dup = duplicates.iterator();
            IdDuplicates example_duplicate = iterate_Dup.next();
            System.err.println("Ground Truth");
            System.out.println("ID1:"+example_duplicate.getEntityId1()+"\tID2:"+example_duplicate.getEntityId2()
                    +"\tHASH:"+example_duplicate.hashCode());


        } catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }

        return null;
    }
}