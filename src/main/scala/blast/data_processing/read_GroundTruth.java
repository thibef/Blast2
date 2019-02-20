package blast.data_processing;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import DataStructures.IdDuplicates;
import com.github.andrewoma.dexx.collection.Set;

public class read_GroundTruth {
    private static HashSet<IdDuplicates> hashSet;
    public static HashSet<Integer> listofHashValues = new HashSet<Integer>();


    public static void read_groundData(String path) {
        HashSet<IdDuplicates> duplicates=null ;
        try (ObjectInputStream ois_ground = new ObjectInputStream(new FileInputStream(path))) {
            duplicates = (HashSet<IdDuplicates>) (ois_ground.readObject());
        }
        catch (Exception e){
            e.printStackTrace();
        }

        hashSet = duplicates;

}

public static HashSet<Integer> get_the_hashValues(){
        for (IdDuplicates dup : hashSet){
            listofHashValues.add((Integer) (dup.hashCode()));
        }
        return listofHashValues;
}
}
