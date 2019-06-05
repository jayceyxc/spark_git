package com.linus.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

public class Person implements Serializable {
    String name;
    String sex;
    int age;

    public Person () {
    }

    public Person (String name, String sex, int age) {
        this.name = name;
        this.sex = sex;
        this.age = age;
    }

    public String getName () {
        return name;
    }

    public void setName (String name) {
        this.name = name;
    }

    public String getSex () {
        return sex;
    }

    public void setSex (String sex) {
        this.sex = sex;
    }

    public int getAge () {
        return age;
    }

    public void setAge (int age) {
        this.age = age;
    }

    @Override
    public boolean equals (Object o) {
        if (this == o) return true;
        if (o == null || getClass () != o.getClass ()) return false;
        Person person = (Person) o;
        return age == person.age &&
                Objects.equals (name, person.name) &&
                Objects.equals (sex, person.sex);
    }

    @Override
    public int hashCode () {

        return Objects.hash (name, sex, age);
    }

    @Override
    public String toString () {
        return "Person{" +
                "name='" + name + '\'' +
                ", sex='" + sex + '\'' +
                ", age=" + age +
                '}';
    }

    public static void main (String[] args) throws IOException {
        String line = "{\"name\": \"yu\", \"age\": 30, \"sex\": \"male\"}";
        ObjectMapper mapper = new ObjectMapper ();
        Person person = mapper.readValue(line, Person.class);
        System.out.println (person);
    }
}
