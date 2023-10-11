package org.example.arrow.sample;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.commons.lang3.time.StopWatch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomDataGeneratorApp implements Vectorizer<Book> {
    private static final int CHUNK_SIZE = 20_000;
    static String[] firstNames={ "Alice", "Bob", "Charlie", "Eva", "David", "Mike", "John","Jian", "Jim", "Jake", "Jane", "Joseph", "Ken", "Kim", "Kate", "Kevin", "Kyle", "Theo", "Tim", "Todd", "Tony", "Tiffany"};
    static String[] lastNames = {"Smith", "Johnson", "Brown", "Davis", "Lee","Andrews", "Jones", "Miller", "Williams", "Davis","Ellis", "Garcia", "Rodriguez", "Martin", "Fernandez", "Martinez","Helena", "Johnson", "Miller", "Williams", "Davis","Ellis"};

    static String[] titles = {"Introduction to Java", "Programming in Python", "Data Structures and Algorithms"};

    static String[] subjects = {"Android", "Java", "C++", "C", "Python", "C#", "Ruby","Haskell"};

    static Author randomAuthor() {
        Random random = new Random();
        return new Author(
                firstNames[random.nextInt(firstNames.length)],
                lastNames[random.nextInt(lastNames.length)],
                random.nextInt(70) + 18);

    }

    static Book randomBook() {
        Random random = new Random();
        List<String> subjectList = new ArrayList<>();
        for (int i = 0; i < random.nextInt(subjects.length); i++) {
            subjectList.add(subjects[random.nextInt(subjects.length)]);
        }
        return new Book(
                titles[random.nextInt(titles.length)],
                randomAuthor(),
                "ISBN" + random.nextInt(10000),
                subjectList);
    }

    static Book[] createRandomBooks(int count) {
        Book[] books = new Book[count];
        for (int i = 0; i < count; i++) {
            books[i] = randomBook();
        }
        return books;
    }


    @Override
    public void vectorize(Book book, int index, VectorSchemaRoot schemaRoot) {
        // Using setSafe: it increases the buffer capacity if needed
        ((VarCharVector) schemaRoot.getVector("title")).setSafe(index, book.getTitle().getBytes());
        List<FieldVector> childrenFromFields = schemaRoot.getVector("author").getChildrenFromFields();
        Author author = book.getAuthor();
        ((VarCharVector) childrenFromFields.get(0)).setSafe(index, author.getFirstName().getBytes());
        ((VarCharVector) childrenFromFields.get(1)).setSafe(index, author.getLastName().getBytes());
        ((UInt4Vector) childrenFromFields.get(2)).setSafe(index, author.getAge());
        ((VarCharVector) schemaRoot.getVector("isbn")).setSafe(index, book.getIsbn().getBytes());
        // Assuming subjects is a ListVector of VarCharVector
        ListVector subjectsVector = (ListVector) schemaRoot.getVector("subjects");
        subjectsVector.setNotNull(index);
        VarCharVector valueVector = (VarCharVector) subjectsVector.getDataVector();
        for (String subject : book.getSubjects()) {
            valueVector.setSafe(subjectsVector.getOffsetBuffer().getInt(index * 4L), subject.getBytes());
            subjectsVector.getOffsetBuffer().setInt((index + 1) * 4L, subjectsVector.getOffsetBuffer().getInt(index * 4L) + 1);
        }
    }

    public void writeToArrowFile(Book[] books) throws IOException {
        new ChunkedWriter<>(CHUNK_SIZE, this).write(new File("books.arrow"), books);
    }

    //
    public static void main(String[] args) throws IOException {
        RandomDataGeneratorApp randomDataGenerator = new RandomDataGeneratorApp();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        int numberOfBooks = 1_000;
        System.out.printf("Generating %d book\n", numberOfBooks);
        Book[] books = createRandomBooks(numberOfBooks);
        stopWatch.split();
        System.out.println("Initiating writing");
        randomDataGenerator.writeToArrowFile(books);
        stopWatch.stop();
        System.out.printf("Timing: %s", stopWatch);
    }
}
