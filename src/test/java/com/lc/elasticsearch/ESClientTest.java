package com.lc.elasticsearch;

import org.junit.Test;

public class ESClientTest {

    @Test
    public void testCreateIndex() {
        ESClientHelper.getInstance().createIndex("mm-payment");
    }

    @Test
    public void testCreateDocument() {
        ESClientHelper.getInstance().addDocument("mm-payment", "1234567890");
    }

    @Test
    public void testQuery() {
        ESClientHelper.getInstance().queryDocument("mm-order", "TzebaG8B0canAUH5k1u5");
    }

    @Test
    public void testSearch() {
        ESClientHelper.getInstance().searchDocument("mm-order");
    }

    @Test
    public void testUpdate() {
        ESClientHelper.getInstance().updateDocument("mm-order", "UDekaG8B0canAUH53luW");
    }

    @Test
    public void testDeleteDocument() {
        ESClientHelper.getInstance().deleteDocument("mm-order", "UjdRaW8B0canAUH5Vlu6");
    }

    @Test
    public void testDeleteIndex() {
        ESClientHelper.getInstance().deleteIndex("mm-payment");
    }

    @Test
    public void scroll() {
        ESClientHelper.getInstance().scroll();
    }
}