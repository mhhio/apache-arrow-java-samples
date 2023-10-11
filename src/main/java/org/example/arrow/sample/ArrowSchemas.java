package org.example.arrow.sample;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

public class ArrowSchemas {
    static Schema authorSchema() {
        return new Schema(authorFields());
    }

    static Schema bookSchema() {
        return new Schema(bookFields());
    }

    private static List<Field> authorFields() {
        return asList(
                new Field("firstName", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("lastName", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("age", FieldType.nullable(new ArrowType.Int(32, false)), null)
        );
    }

    private static List<Field> bookFields() {
        return asList(
                new Field("title", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("author", FieldType.nullable(new ArrowType.Struct()), authorFields()),
                new Field("isbn", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("subjects", FieldType.nullable(new ArrowType.List()), Collections.singletonList(new Field("subject", FieldType.nullable(new ArrowType.Utf8()), null)))
        );
    }

}
