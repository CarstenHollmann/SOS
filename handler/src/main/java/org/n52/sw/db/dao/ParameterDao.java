package org.n52.sw.db.dao;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.hibernate.Session;
import org.n52.series.db.beans.HibernateRelations;
import org.n52.series.db.beans.UnitEntity;
import org.n52.series.db.beans.parameter.Parameter;
import org.n52.series.db.beans.parameter.ValuedParameter;
import org.n52.shetland.ogc.UoM;
import org.n52.shetland.ogc.om.NamedValue;
import org.n52.shetland.ogc.om.values.BooleanValue;
import org.n52.shetland.ogc.om.values.CategoryValue;
import org.n52.shetland.ogc.om.values.ComplexValue;
import org.n52.shetland.ogc.om.values.CountValue;
import org.n52.shetland.ogc.om.values.CvDiscretePointCoverage;
import org.n52.shetland.ogc.om.values.GeometryValue;
import org.n52.shetland.ogc.om.values.HrefAttributeValue;
import org.n52.shetland.ogc.om.values.MultiPointCoverage;
import org.n52.shetland.ogc.om.values.NilTemplateValue;
import org.n52.shetland.ogc.om.values.ProfileValue;
import org.n52.shetland.ogc.om.values.QuantityRangeValue;
import org.n52.shetland.ogc.om.values.QuantityValue;
import org.n52.shetland.ogc.om.values.RectifiedGridCoverage;
import org.n52.shetland.ogc.om.values.ReferenceValue;
import org.n52.shetland.ogc.om.values.SweDataArrayValue;
import org.n52.shetland.ogc.om.values.TLVTValue;
import org.n52.shetland.ogc.om.values.TVPValue;
import org.n52.shetland.ogc.om.values.TextValue;
import org.n52.shetland.ogc.om.values.TimeRangeValue;
import org.n52.shetland.ogc.om.values.UnknownValue;
import org.n52.shetland.ogc.om.values.Value;
import org.n52.shetland.ogc.om.values.XmlValue;
import org.n52.shetland.ogc.om.values.visitor.ValueVisitor;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.sos.Sos2Constants;
import org.n52.sw.db.beans.ParameterFactory;

import com.google.common.collect.Sets;

public class ParameterDao extends AbstractDao {

    public ParameterDao(DaoFactory daoFactory, Session session) {
        super(daoFactory, session);
    }
    
    public Set<Parameter<?>> insert(Collection<NamedValue<?>> parameter, long observationId, Map<UoM, UnitEntity> unitCache) throws OwsExceptionReport {
        Set<Parameter<?>> inserted = Sets.newHashSet();
        for (NamedValue<?> namedValue : parameter) {
            if (!Sos2Constants.HREF_PARAMETER_SPATIAL_FILTERING_PROFILE.equals(namedValue.getName().getHref())) {
                ParameterPersister persister = new ParameterPersister(
                        this,
                        namedValue,
                        observationId,
                        unitCache,
                        getSession()
                );
                inserted.add((Parameter<?>)namedValue.getValue().accept(persister));
            }
        }
        return inserted;
    }

    /**
     * If the local unit cache isn't null, use it when retrieving unit.
     *
     * @param unit
     *            UnitEntity
     * @param localCache
     *            Cache (possibly null)
     * @param session
     * @return UnitEntity
     */
    protected UnitEntity getUnit(String unit, Map<UoM, UnitEntity> localCache) {
        return getUnit(new UoM(unit), localCache);
    }

    /**
     * If the local unit cache isn't null, use it when retrieving unit.
     *
     * @param unit
     *            UnitEntity
     * @param localCache
     *            Cache (possibly null)
     * @param session
     * @return UnitEntity
     */
    protected UnitEntity getUnit(UoM unit, Map<UoM, UnitEntity> localCache) {
        if (localCache != null && localCache.containsKey(unit)) {
            return localCache.get(unit);
        } else {
            // query unit and set cache
            UnitEntity hUnitEntity = getDaoFactory().getUnitDao(getSession()).getOrInsert(unit);
            if (localCache != null) {
                localCache.put(unit, hUnitEntity);
            }
            return hUnitEntity;
        }
    }

    public ParameterFactory getParameterFactory() {
        return ParameterFactory.getInstance();
    }

    public static class ParameterPersister implements ValueVisitor<ValuedParameter<?,?>, OwsExceptionReport> {
        private final Caches caches;
        private final Session session;
        private final NamedValue<?> namedValue;
        private final DAOs daos;
        private final ParameterFactory parameterFactory;

        public ParameterPersister(ParameterDao parameterDAO, NamedValue<?> namedValue, long observationId, Map<UoM, UnitEntity> unitCache, Session session) {
            this(new DAOs(parameterDAO),
                    new Caches(unitCache),
                    namedValue,
                    observationId,
                    session);
        }

        public ParameterPersister(DAOs daos, Caches caches, NamedValue<?> namedValue, long observationId, Session session) {
            this.caches = caches;
            this.session = session;
            this.daos = daos;
            this.namedValue = namedValue;
            this.parameterFactory = daos.parameter.getParameterFactory();
        }

        private static class Caches {
            private final Map<UoM, UnitEntity> units;

            Caches(Map<UoM, UnitEntity> units) {
                this.units = units;
            }

            public Map<UoM, UnitEntity> units() {
                return units;
            }
        }

        private static class DAOs {
            private final ParameterDao parameter;

            DAOs(ParameterDao parameter) {
                this.parameter = parameter;
            }

            public ParameterDao parameter() {
                return this.parameter;
            }
        }

        private <V, T extends ValuedParameter<V, T>> T setUnitEntityAndPersist(T parameter, Value<V> value) throws OwsExceptionReport {
            if (parameter instanceof HibernateRelations.HasUnit) {
                ((HibernateRelations.HasUnit)parameter).setUnit(getUnit(value));
            }
            return persist(parameter, value.getValue());
        }

        private UnitEntity getUnit(Value<?> value) {
            return value.isSetUnit() ? daos.parameter().getUnit(value.getUnitObject(), caches.units()) : null;
        }

        private <V, T extends ValuedParameter<V, T>> T persist(T parameter, Value<V> value) throws OwsExceptionReport {
            return persist(parameter, value.getValue());
        }

        private <V, T extends ValuedParameter<V, T>> T persist(T parameter, V value) throws OwsExceptionReport {
            if (parameter instanceof HibernateRelations.HasUnit && !((HibernateRelations.HasUnit)parameter).isSetUnit()) {
                ((HibernateRelations.HasUnit)parameter).setUnit(getUnit(namedValue.getValue()));
            }
            parameter.setName(namedValue.getName().getHref());
            parameter.setValue(value);
            session.saveOrUpdate(parameter);
            session.flush();
            session.refresh(parameter);
            return parameter;
        }

        @Override
        public ValuedParameter<?, ?> visit(BooleanValue value) throws OwsExceptionReport {
            return persist(parameterFactory.truth(), value);
        }

        @Override
        public ValuedParameter<?, ?> visit(CategoryValue value) throws OwsExceptionReport {
            return setUnitEntityAndPersist(parameterFactory.category(), value);
        }

        @Override
        public ValuedParameter<?, ?> visit(ComplexValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(CountValue value) throws OwsExceptionReport {
            return persist(parameterFactory.count(), value);
        }

        @Override
        public ValuedParameter<?, ?> visit(GeometryValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(HrefAttributeValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(NilTemplateValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(QuantityValue value) throws OwsExceptionReport {
            return setUnitEntityAndPersist(parameterFactory.quantity(), value);
        }

        @Override
        public ValuedParameter<?, ?> visit(ReferenceValue value) throws OwsExceptionReport {
            return persist(parameterFactory.category(), value.getValue().getHref());
        }

        @Override
        public ValuedParameter<?, ?> visit(SweDataArrayValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(TVPValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(TextValue value) throws OwsExceptionReport {
            return persist(parameterFactory.text(), value);
        }

        @Override
        public ValuedParameter<?, ?> visit(TimeRangeValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(UnknownValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(TLVTValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(CvDiscretePointCoverage value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(MultiPointCoverage value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(RectifiedGridCoverage value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(ProfileValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(XmlValue<?> value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public ValuedParameter<?, ?> visit(QuantityRangeValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        private OwsExceptionReport notSupported(Value<?> value)
                throws OwsExceptionReport {
            throw new NoApplicableCodeException()
                    .withMessage("Unsupported om:parameter value %s", value
                                 .getClass().getCanonicalName());
        }
    }

}
