package javacode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by lenovo on 2017/12/29.
 */
public class test {

    public static void main(String[] args) throws IOException
    {
        File f = new File("E:\\dir1\\test.txt");
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        StringBuilder strbuiler = new StringBuilder();
        strbuiler.append("test");
        bw.write(strbuiler.toString());
        bw.flush();
        bw.close();
    }


}
