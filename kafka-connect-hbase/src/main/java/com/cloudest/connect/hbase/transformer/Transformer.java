package com.cloudest.connect.hbase.transformer;

/**
 * Created by terry on 2017/6/8.
 */
public interface Transformer<T,D> {

    D transform(T oType);

}
