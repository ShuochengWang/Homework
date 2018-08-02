package shuocheng;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class App 
{
    public static void main( String[] args ) throws Exception
    {
        byte[] data = "STRING FOR TEST. Hello World. STRING FOR TEST. Hello World.".getBytes();
        int num = 10000000;

        FileSystem fs = FileSystem.Factory.get();
        // fs.delete(new AlluxioURI("/myFile"));
        AlluxioURI path = new AlluxioURI("/myFile");

        FileOutStream out = fs.createFile(path);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < num; ++i)
        {
            out.write(data);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("alluxio write time：" + (endTime - startTime) + "ms");
        out.close();


        FileInStream in = fs.openFile(path);
        startTime = System.currentTimeMillis();
        int ch;
        while ((ch = in.read()) != -1);
        endTime = System.currentTimeMillis();
        System.out.println("alluxio read time：" + (endTime - startTime) + "ms");
        in.close();


        OutputStream fout = new FileOutputStream("myFile");
        startTime = System.currentTimeMillis();
        for (int i = 0; i < num; ++i)
        {
            fout.write(data);
        }
        endTime = System.currentTimeMillis();
        System.out.println("local fs write time：" + (endTime - startTime) + "ms");
        fout.close();

        InputStream fin = new FileInputStream("myFile");
        int size = fin.available();
        startTime = System.currentTimeMillis();
        for (int i = 0; i < size; i++)
        {
            fin.read();
        }
        endTime = System.currentTimeMillis();
        System.out.println("local fs read time：" + (endTime - startTime) + "ms");
        fin.close();
    }
}
