package com.cloudest.connect.elasticsearch;

import java.util.Objects;

public class Key {
    public final String index;
    public final String type;
    public final String id;

    public Key(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Key that = (Key) o;
        return Objects.equals(index, that.index)
                && Objects.equals(type, that.type)
                && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type, id);
    }

    @Override
    public String toString() {
        return String.format("Key{%s/%s/%s}", index, type, id);
    }
}
