package org.n52.sw.suite.db.dao;

import org.hibernate.Session;

public abstract class AbstractDao implements Dao {

    private final DaoFactory daoFactory;
    private final Session session;

    public AbstractDao(DaoFactory daoFactory, Session session) {
        this.daoFactory = daoFactory;
        this.session = session;
    }

    /**
     * @return the daoFactory
     */
    public DaoFactory getDaoFactory() {
        return daoFactory;
    }

    /**
     * @return the session
     */
    public Session getSession() {
        return session;
    }
    
}
