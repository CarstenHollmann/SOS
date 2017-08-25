package org.n52.sw.suite.db.dao;

import org.hibernate.Session;

public interface Dao {
    
    /**
     * @return the daoFactory
     */
    DaoFactory getDaoFactory();

    /**
     * @return the session
     */
    Session getSession();

}
