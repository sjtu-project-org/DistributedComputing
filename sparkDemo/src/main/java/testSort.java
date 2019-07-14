import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class testSort {
    public static void main(String []args) {
        List<String> l = new ArrayList<String>(Arrays.asList("B", "C", "A"));
        Collections.sort(l);

        for (String s : l) {
            System.out.println(s);
        }

    }
}
