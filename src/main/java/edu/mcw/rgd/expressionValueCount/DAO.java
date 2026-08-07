package edu.mcw.rgd.expressionValueCount;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionValueCount;

import org.springframework.jdbc.object.BatchSqlUpdate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    /**
     * Removes rows whose (gene, term) pair no longer has any expression values. Keyed the same way
     * the batch update methods are, on (rgd id, term, unit, level), since the table has no surrogate
     * key of its own.
     */
    public int deleteValueCounts(List<GeneExpressionValueCount> obsolete) throws Exception {

        BatchSqlUpdate su = new BatchSqlUpdate(DataSourceFactory.getInstance().getDataSource(),
                "DELETE FROM gene_expression_value_counts WHERE expressed_object_rgd_id=? AND term_acc=? "
                        +"AND expression_unit=? AND expression_level=?",
                new int[]{Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR}, 10000);
        su.compile();
        for( GeneExpressionValueCount vc: obsolete ) {
            su.update(vc.getExpressedRgdId(), vc.getTermAcc(), vc.getUnit(), vc.getLevel());
        }
        su.flush();

        int rowsDeleted = 0;
        for( int rows: su.getRowsAffected() ) {
            rowsDeleted += rows;
        }
        return rowsDeleted;
    }

    /** key used to line up computed counts against the counts already stored */
    static String countKey(int rgdId, String termAcc) {
        return rgdId + "|" + termAcc;
    }

    /**
     * Counts every (gene, slim term) pair for one species in a single pass.
     * <p>
     * Replaces one query per gene per slim term -- roughly 29 million round trips for a full run --
     * with a single aggregate that lets the database do the counting.
     * <p>
     * The term hierarchy deliberately differs by level, to match the per-gene methods in
     * GeneExpressionDAO that this replaces: getGeneExpressionCountByTermRgdIdUnit (used for 'all')
     * counts the slim term itself as well as its descendants, while
     * getGeneExprRecordValuesCountForGeneBySlim (used for the individual levels) counts descendants
     * only. Neither filters on ont_rel_id or map_key, so neither does this.
     * <p>
     * The DISTINCT on the hierarchy matters: a descendant is reachable from a slim term by many
     * paths, and without it every count would be multiplied by the number of paths.
     *
     * @return computed counts keyed by {@link #countKey}
     */
    public Map<String,Integer> getComputedCounts(int speciesTypeKey, String ontId, String slimSource,
                                                 String unit, String level) throws Exception {

        boolean allLevels = "all".equals(level);
        List<Object> params = new ArrayList<>();

        StringBuilder sql = new StringBuilder("""
            WITH slim_map AS (
                SELECT DISTINCT CONNECT_BY_ROOT parent_term_acc AS slim_acc, child_term_acc AS descendant_acc
                FROM ont_dag
                START WITH parent_term_acc IN (SELECT term_acc FROM ont_slims WHERE ont_id=? AND source=?)
                CONNECT BY PRIOR child_term_acc = parent_term_acc
            """);
        params.add(ontId);
        params.add(slimSource);

        if( allLevels ) {
            sql.append("    UNION SELECT term_acc, term_acc FROM ont_slims WHERE ont_id=? AND source=?\n");
            params.add(ontId);
            params.add(slimSource);
        }

        sql.append("""
            )
            SELECT ge.expressed_object_rgd_id, m.slim_acc, COUNT(*) AS value_count
            FROM gene_expression_values ge
            JOIN gene_expression_exp_record gr ON ge.gene_expression_exp_record_id = gr.gene_expression_exp_record_id
            JOIN sample s    ON s.sample_id = gr.sample_id
            JOIN ont_terms t ON t.term_acc = s.tissue_ont_id
            JOIN slim_map m  ON m.descendant_acc = t.term_acc
            JOIN rgd_ids i   ON i.rgd_id = ge.expressed_object_rgd_id
            WHERE t.is_obsolete = 0
              AND i.object_key = 1 AND i.object_status = 'ACTIVE' AND i.species_type_key = ?
              AND ge.expression_unit = ?
            """);
        params.add(speciesTypeKey);
        params.add(unit);

        if( !allLevels ) {
            sql.append("  AND ge.expression_level = ?\n");
            params.add(level);
        }
        sql.append("GROUP BY ge.expressed_object_rgd_id, m.slim_acc");

        Map<String,Integer> counts = new HashMap<>();
        try( Connection conn = DataSourceFactory.getInstance().getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString()) ) {

            for( int i=0; i<params.size(); i++ ) {
                ps.setObject(i+1, params.get(i));
            }
            ps.setFetchSize(10000);
            try( ResultSet rs = ps.executeQuery() ) {
                while( rs.next() ) {
                    counts.put(countKey(rs.getInt(1), rs.getString(2)), rs.getInt(3));
                }
            }
        }
        return counts;
    }

    /** the counts already stored for one species, so the computed ones can be diffed against them */
    public Map<String,Integer> getStoredCounts(int speciesTypeKey, String unit, String level) throws Exception {

        String sql = """
            SELECT c.expressed_object_rgd_id, c.term_acc, c.value_count
            FROM gene_expression_value_counts c
            JOIN rgd_ids i ON i.rgd_id = c.expressed_object_rgd_id
            WHERE i.object_key = 1 AND i.object_status = 'ACTIVE' AND i.species_type_key = ?
              AND c.expression_unit = ? AND c.expression_level = ?
            """;

        Map<String,Integer> counts = new HashMap<>();
        try( Connection conn = DataSourceFactory.getInstance().getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql) ) {

            ps.setInt(1, speciesTypeKey);
            ps.setString(2, unit);
            ps.setString(3, level);
            ps.setFetchSize(10000);
            try( ResultSet rs = ps.executeQuery() ) {
                while( rs.next() ) {
                    counts.put(countKey(rs.getInt(1), rs.getString(2)), rs.getInt(3));
                }
            }
        }
        return counts;
    }

}
