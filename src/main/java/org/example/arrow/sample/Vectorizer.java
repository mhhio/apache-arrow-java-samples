package org.example.arrow.sample;

import org.apache.arrow.vector.VectorSchemaRoot;

@FunctionalInterface
public interface Vectorizer<T> {
    void vectorize(T value, int index, VectorSchemaRoot batch);
}
