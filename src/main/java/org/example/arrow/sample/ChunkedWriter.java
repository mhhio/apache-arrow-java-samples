package org.example.arrow.sample;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.example.arrow.sample.ArrowSchemas.bookSchema;

public class ChunkedWriter<T>{
    private final int chunkSize;
    private final Vectorizer<T> vectorizer;

    public ChunkedWriter(int chunkSize, Vectorizer<T> vectorizer) {
        this.chunkSize = chunkSize;
        this.vectorizer = vectorizer;
    }

    public void write(File file, T[] values) throws IOException {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();

        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(bookSchema(), allocator);
             FileOutputStream fd = new FileOutputStream(file);
             ArrowFileWriter fileWriter = new ArrowFileWriter(schemaRoot, dictProvider, fd.getChannel())) {

            System.out.println("Start writing");
            fileWriter.start();

            int index = 0;
            while (index < values.length) {
                schemaRoot.allocateNew();
                int chunkIndex = 0;
                while (chunkIndex < chunkSize && index + chunkIndex < values.length) {
                    vectorizer.vectorize(values[index + chunkIndex], chunkIndex, schemaRoot);
                    chunkIndex++;
                }
                schemaRoot.setRowCount(chunkIndex);
                System.out.println("Filled chunk with "+chunkIndex+" items; "+index + chunkIndex+" items written");
                fileWriter.writeBatch();
                System.out.println("Chunk written");

                index += chunkIndex;
                schemaRoot.clear();
            }

            System.out.println("Writing done");
            fileWriter.end();
        }
    }
}
