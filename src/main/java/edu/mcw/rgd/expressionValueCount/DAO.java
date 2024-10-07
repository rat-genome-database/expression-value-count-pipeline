package edu.mcw.rgd.expressionValueCount;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.dao.impl.variants.VariantDAO;
import edu.mcw.rgd.dao.spring.variants.VariantSampleQuery;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionValueCount;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by llamers on 1/28/2020.
 */
public class DAO {

    private OntologyXDAO xdao = new OntologyXDAO();
    private GeneDAO geneDAO = new GeneDAO();
    private GeneExpressionDAO gedao = new GeneExpressionDAO();

    public String getConnection(){
        return geneDAO.getConnectionInfo();
    }

    public DataSource getVariantDataSource() throws Exception{
        return DataSourceFactory.getInstance().getCarpeNovoDataSource();
    }

    public List<String> getAllSlimTerms(String term, String source) throws Exception{
        return xdao.getAllSlimTerms(term,source);
    }

    public List<Gene> getActiveGenes(int speciesType) throws Exception{
        return geneDAO.getActiveGenes(speciesType);
    }

    public GeneExpressionValueCount getValueCountsByGeneRgdIdTermUnitAndLevel(int rgdId, String termAcc, String unit, String level)throws Exception{
        return gedao.getValueCountsByGeneRgdIdTermUnitAndLevel(rgdId, termAcc, unit, level);
    }

    public int getGeneExprRecordValuesCountForGeneBySlim(int rgdId, String termAcc, String unit, String level) throws Exception{
        return gedao.getGeneExprRecordValuesCountForGeneBySlim(termAcc,rgdId,unit,level);
    }

    public int getGeneExprRecordValuesCountForGene(int rgdId, String termAcc, String unit) throws Exception{
        return gedao.getGeneExpressionCountByTermRgdIdUnit(termAcc,rgdId,unit);
    }

    public int insertGeneExprRecValCnt(List<GeneExpressionValueCount> cnts) throws Exception{
        return gedao.insertGeneExpressionValueCountBatch(cnts);
    }

    public int updateGeneExprRecValCnts(List<GeneExpressionValueCount> cnts) throws Exception{
        return gedao.UpdateGeneExpressionValueCountBatch(cnts);
    }

    public int updateLastModified(List<GeneExpressionValueCount> cnts) throws Exception{
        return gedao.UpdateGeneExpressionValueLastModifiedBatch(cnts);
    }

}
