package org.ldbcouncil.finbench.impls.common.interactive;

import com.google.common.collect.ImmutableList;
import org.ldbcouncil.finbench.impls.common.SnbTest;
import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkload;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.*;
import org.junit.Test;

import java.util.Date;

public abstract class InteractiveTest<D extends Db> extends SnbTest<D>
{

    public InteractiveTest( D db )
    {
        super( db, new LdbcSnbInteractiveWorkload() );
    }

    @Test
    public void testQuery1() throws Exception
    {
        run( db, new LdbcQuery1( 30786325579101L, "Ian", LIMIT ) );
    }
}

