package com.lc.elasticsearch;

import org.junit.Test;

public class ESClientTest {

    @Test
    public void testCreate() {
        // ESClientHelper.getInstance().createIndex("mm-payment");
        ESClientHelper.getInstance().addDocument("mm-order");
    }

    @Test
    public void testSearch() {
        ESClientHelper.getInstance().getDocument("mm-order", "TzebaG8B0canAUH5k1u5");
    }

    @Test
    public void testUpdate() {
        ESClientHelper.getInstance().updateDocument("mm-order", "UDekaG8B0canAUH53luW");
    }

    @Test
    public void testDelete() {
        ESClientHelper.getInstance().deleteDocument("mm-order", "UjdRaW8B0canAUH5Vlu6");
    }

    @Test
    public void scroll() {
        ESClientHelper.getInstance().scroll();
    }
}