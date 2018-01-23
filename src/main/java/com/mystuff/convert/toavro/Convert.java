package com.mystuff.convert.toavro;

import org.apache.avro.*;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;


import java.io.File;
import java.io.IOException;

public class Convert {

    public void deserialize () {
        Schema schema = null;
        try {
            schema = new Schema.Parser().parse(new File("target/classes/user.avsc"));

        } catch (IOException e) {
            e.printStackTrace();
        }
        GenericDatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<GenericRecord>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(userDatumWriter);

        try {
            dataFileWriter.create(schema, new File("target/classes/users.avro"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

   public static void main(String [] args) {
     Convert convert = new Convert();
     convert.deserialize();
   }
 }
