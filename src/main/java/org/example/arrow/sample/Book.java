package org.example.arrow.sample;

import java.util.List;

public class Book {
    private String title;
    private Author author;
    private String isbn;

    private List<String> subjects;

    public Book(String title, Author author, String isbn, List<String> subjects) {
        this.title = title;
        this.author = author;
        this.isbn = isbn;
        this.subjects = subjects;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Author getAuthor() {
        return author;
    }

    public void setAuthor(Author author) {
        this.author = author;
    }

    public String getIsbn() {
        return isbn;
    }

    public void setIsbn(String isbn) {
        this.isbn = isbn;
    }

    public List<String> getSubjects() {
        return subjects;
    }

    public void setSubjects(List<String> subjects) {
        this.subjects = subjects;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Book book = (Book) o;

        return new org.apache.commons.lang3.builder.EqualsBuilder().append(title, book.title).append(author, book.author).append(isbn, book.isbn).append(subjects, book.subjects).isEquals();
    }

    @Override
    public int hashCode() {
        return new org.apache.commons.lang3.builder.HashCodeBuilder(17, 37).append(title).append(author).append(isbn).append(subjects).toHashCode();
    }
}
