package org.example.arrow.sample;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import okio.ByteString;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.commons.lang3.time.StopWatch;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FilterSingleColumnApp {


    private void doAnalytics() throws IOException {
        RootAllocator allocator = new RootAllocator();

        try (FileInputStream fd = new FileInputStream("books.arrow")) {
            // Setup file reader
            ArrowFileReader fileReader = new ArrowFileReader(new SeekableReadChannel(fd.getChannel()), allocator);
            fileReader.initialize();
            VectorSchemaRoot schemaRoot = fileReader.getVectorSchemaRoot();

            // Aggregate: Using ByteString as it is faster than creating a String from a byte[]
            Map<ByteString, Long> perFirstNameCount = new TreeMap<>();
            Map<ByteString, Long> perFirstNameSum = new TreeMap<>();
            processBatches(fileReader, schemaRoot, perFirstNameCount, perFirstNameSum);

            // Print results
            for (ByteString firstName : perFirstNameCount.keySet()) {
                double average = (double) perFirstNameSum.get(firstName) / perFirstNameCount.get(firstName);
                System.out.printf("First Name = %s; Average = %f\n", firstName, average);
            }
        }
    }

    /**
     * @param fileReader
     * @param schemaRoot
     * @param perCityCount
     * @param perCitySum
     * @throws IOException
     */
    private void processBatches(ArrowFileReader fileReader,
                                VectorSchemaRoot schemaRoot,
                                Map<ByteString, Long> perCityCount,
                                Map<ByteString, Long> perCitySum) throws IOException {
        // Reading the data, one batch at a time
        while (fileReader.loadNextBatch()) {
            int[] selectedIndexes = filterOnAuthorLastName(schemaRoot).elements();

            aggregate(schemaRoot, selectedIndexes, perCityCount, perCitySum);
        }
    }

    /**
     * @param schemaRoot
     * @param selectedIndexes
     * @param perFirstNameCount
     * @param perFirstNameSum
     */
    private void aggregate(VectorSchemaRoot schemaRoot,
                           int[] selectedIndexes,
                           Map<ByteString, Long> perFirstNameCount,
                           Map<ByteString, Long> perFirstNameSum
    ) {
        VarCharVector firstNameVector = (VarCharVector) ((StructVector) schemaRoot.getVector("author")).getChild("firstName");
        UInt4Vector ageDataVector = (UInt4Vector) ((StructVector) schemaRoot.getVector("author")).getChild("age");
        ;

        for (int selectedIndex : selectedIndexes) {
            ByteString firstName = ByteString.of(firstNameVector.get(selectedIndex));
            perFirstNameCount.put(firstName, perFirstNameCount.getOrDefault(firstName, 0L) + 1);
            perFirstNameSum.put(firstName, perFirstNameSum.getOrDefault(firstName, 0L) + ageDataVector.get(selectedIndex));
        }
    }

    //return a list of selected indexes if record author lastname ends with "ez"
    private IntArrayList filterOnAuthorLastName(VectorSchemaRoot root) {
        StructVector authorVector = (StructVector) root.getVector("author");
        VarCharVector lastNameVector = (VarCharVector) authorVector.getChild("lastName");

        IntArrayList selectedIndexes = new IntArrayList();
        byte[] suffixBytes = "ez".getBytes();
        for (int i = 0; i < root.getRowCount(); i++) {
            if (ByteString.of(lastNameVector.get(i)).endsWith(suffixBytes)) {
                selectedIndexes.add(i);
            }
        }

        selectedIndexes.trim();
        return selectedIndexes;
    }


    public static void main(String[] args) throws IOException {
        FilterSingleColumnApp app = new FilterSingleColumnApp();
        StopWatch stopWatch = StopWatch.createStarted();
        app.doAnalytics();
        stopWatch.stop();
        System.out.printf("Timing: %s", stopWatch);

    }
}
