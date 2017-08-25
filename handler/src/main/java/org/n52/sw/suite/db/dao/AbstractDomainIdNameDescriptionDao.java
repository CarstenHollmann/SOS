package org.n52.sw.suite.db.dao;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.hibernate.Session;
import org.n52.iceland.i18n.I18NDAO;
import org.n52.iceland.i18n.I18NDAORepository;
import org.n52.iceland.i18n.metadata.I18NFeatureMetadata;
import org.n52.series.db.beans.CodespaceEntity;
import org.n52.series.db.beans.DescribableEntity;
import org.n52.series.db.beans.FeatureEntity;
import org.n52.shetland.ogc.OGCConstants;
import org.n52.shetland.ogc.gml.AbstractFeature;
import org.n52.shetland.ogc.gml.AbstractGML;
import org.n52.shetland.ogc.gml.CodeType;
import org.n52.shetland.ogc.gml.CodeWithAuthority;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;

public class AbstractDomainIdNameDescriptionDao extends AbstractDao implements TimeCreator {
    
    public AbstractDomainIdNameDescriptionDao(DaoFactory daoFactory, Session session) {
        super(daoFactory, session);
    }

    public void addIdentifierNameDescription(AbstractGML abstractFeature, DescribableEntity entity) {
        addIdentifier(abstractFeature, entity);
        addName(abstractFeature, entity);
        addDescription(abstractFeature, entity);
    }

    public void addIdentifier(AbstractGML abstractFeature, DescribableEntity entity) {
        addIdentifier(entity, abstractFeature.getIdentifierCodeWithAuthority());
    }

    public void addIdentifier(DescribableEntity entity, CodeWithAuthority identifier) {
        String value = identifier != null && identifier.isSetValue() ? identifier.getValue() : null;
        String codespace =
                identifier != null && identifier.isSetCodeSpace() ? identifier.getCodeSpace() : OGCConstants.UNKNOWN;
        entity.setIdentifier(value);
        entity.setCodespace(getDaoFactory().getCodespaceDao(getSession()).getOrInsert(codespace));
    }

    public void addName(AbstractGML abstractFeature, DescribableEntity entity) {
        addName(entity, abstractFeature.getFirstName());
    }

    public void addName(DescribableEntity entity, CodeType name) {
        String value = name != null && name.isSetValue() ? name.getValue() : null;
        String codespace =
                name != null && name.isSetCodeSpace() ? name.getCodeSpace().toString() : OGCConstants.UNKNOWN;
        entity.setName(value);
        entity.setCodespaceName(getDaoFactory().getCodespaceDao(getSession()).getOrInsert(codespace));
    }

    public void addDescription(AbstractGML abstractFeature, DescribableEntity entity) {
        addDescription(entity, abstractFeature.getDescription());
    }

    public void addDescription(DescribableEntity entity, String description) {
        if (description != null && !description.isEmpty()) {
            entity.setDescription(description);
        }
    }

    public void getAndAddIdentifierNameDescription(AbstractGML abstractFeature, DescribableEntity entity)
            throws OwsExceptionReport {
        abstractFeature.setIdentifier(getIdentifier(entity));
        abstractFeature.addName(getName(entity));
        abstractFeature.setDescription(getDescription(entity));
    }

    public CodeWithAuthority getIdentifier(DescribableEntity entity) {
        CodeWithAuthority identifier = new CodeWithAuthority(entity.getIdentifier());
        if (entity.isSetCodespace()) {
            identifier.setCodeSpace(entity.getCodespace().getCodespace());
        }
        return identifier;
    }

    public CodeType getName(DescribableEntity entity)
            throws OwsExceptionReport {
        if (entity.isSetName()) {
            CodeType name = new CodeType(entity.getName());
            if (entity.isSetCodespaceName()) {
                try {
                    name.setCodeSpace(new URI(entity.getCodespaceName().getCodespace()));
                } catch (URISyntaxException e) {
                    throw new NoApplicableCodeException().causedBy(e).withMessage("Error while creating URI from '{}'",
                            entity.getCodespaceName().getCodespace());
                }
            }
            return name;
        }
        return null;
    }

    public String getDescription(DescribableEntity entity) {
        if (entity.isSetDescription()) {
            return entity.getDescription();
        }
        return null;
    }

    public void insertNames(FeatureEntity feature, List<CodeType> name, I18NDAORepository i18nr) {
        CodespaceDao codespaceDAO = getDaoFactory().getCodespaceDao(getSession());
        I18NDAO<I18NFeatureMetadata> dao = i18nr.getDAO(I18NFeatureMetadata.class);
        for (CodeType codeType : name) {
            CodespaceEntity codespace = codespaceDAO.getOrInsert(codeType.getCodeSpace().toString());
            // i18ndao.insertI18N(feature, new I18NInsertionObject(codespace,
            // codeType.getValue()), session);
        }
    }

    public void insertNameAndDescription(DescribableEntity entity, AbstractFeature abstractFeature,
            Session session) {
        if (abstractFeature.isSetName()) {

        }
        // session.saveOrUpdate(
        //
        // AbstractI18NDAO<?, ?> i18ndao =
        // DaoFactory.getInstance().getI18NDAO(feature, session);
        // featureOfInterestDAO.addIdentifierNameDescription(samplingFeature,
        // feature, session);
    }
}
