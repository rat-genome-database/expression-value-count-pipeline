package edu.mcw.rgd.expressionValueCount;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.SpeciesType;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionValueCount;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Main {
    private String version;
    private Map<Integer,String> species;
    private List<String> expressionLevels;
    protected Logger logger = LogManager.getLogger("status");
    private DAO dao = new DAO();


    public static void main(String[] args) throws Exception {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        try {
            Main main = (Main) bf.getBean("main");
            main.run(args);
        }
        catch (Exception e) {
            Utils.printStackTrace(e, LogManager.getLogger("status"));
            throw e;
        }
    }

    void run(String[] args) throws Exception {
        int speciesTypeKey = 3;
        for (int i = 0; i < args.length; i++) {
            speciesTypeKey = switch (args[i]) {
                case "1" -> 1;
                case "2" -> 2;
                case "3" -> 3;
                case "6" -> 6;
                case "9" -> 9;
                case "13" -> 13;
                case "-runForAll" -> 0;
                default -> 3;
            };
        }
        if (speciesTypeKey == 0){
            for (int speciesType : species.keySet()){
                generateValueCounts(speciesType);
            } // end species for
        }
        else
            generateValueCounts(speciesTypeKey);
        return;
    }

    public void generateValueCounts(int speciesTypeKey) throws Exception {
        logger.info(getVersion());
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long pipeStart = System.currentTimeMillis();
        logger.info("\tPipeline started at "+sdt.format(new Date(pipeStart))+"\n");
        List<String> terms = dao.getAllSlimTerms("UBERON","AGR");
        logger.info("\t\tRunning for species "+species.get(speciesTypeKey)+"...");
        List<Gene> activeGenes = dao.getActiveGenes(speciesTypeKey);
        List<GeneExpressionValueCount> newValueCounts = new ArrayList<>();
        List<GeneExpressionValueCount> updateValueCounts = new ArrayList<>();
        List<GeneExpressionValueCount> updateLastModified = new ArrayList<>();
        // loop through terms and start getting counts and insert them
        for (Gene g : activeGenes){
            int geneRgdId = g.getRgdId();
            for (String term : terms){
                for (String level : expressionLevels){
                    // check if row exists, if yes, update value and last modified
                    // else create row and add to list
                    int cnt = 0;
                    switch (level) {
                        case "below cutoff", "low", "medium", "high" ->
                                cnt = dao.getGeneExprRecordValuesCountForGeneBySlim(geneRgdId, term, "TPM", level);
                        case "all" -> cnt = dao.getGeneExprRecordValuesCountForGene(geneRgdId, term, "TPM");
                    }
                    if (cnt==0)
                        continue;
                    GeneExpressionValueCount gvc = dao.getValueCountsByGeneRgdIdTermUnitAndLevel(geneRgdId,term,"TPM",level);
                    if (gvc == null){
                        gvc = new GeneExpressionValueCount();
                        gvc.setValueCnt(cnt);
                        gvc.setExpressedRgdId(geneRgdId);
                        gvc.setTermAcc(term);
                        gvc.setUnit("TPM");
                        gvc.setLevel(level);
                        newValueCounts.add(gvc);
                    }
                    else if (gvc.getValueCnt()!=cnt){
                        updateValueCounts.add(gvc);
                    }
                    else {
                        updateLastModified.add(gvc);
                    }
                }
            } // end terms for
        } // end gene for
        if (!newValueCounts.isEmpty()){
            logger.info("\t\tNew Counts for Expression Values: "+newValueCounts.size());
                dao.insertGeneExprRecValCnt(newValueCounts);
        }
        if (!updateValueCounts.isEmpty()){
            logger.info("\t\tValues being updated: "+updateValueCounts.size());
                dao.updateGeneExprRecValCnts(updateValueCounts);
        }
        if (!updateLastModified.isEmpty()){
            logger.info("\t\tCounts not changed: "+updateLastModified.size());
                dao.updateLastModified(updateLastModified);
        }
        // get genes for species
        logger.info("\tExpression Value Count pipeline for species "+species.get(speciesTypeKey)+" runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));
    }
    public void setVersion(String version) {
        this.version=version;
    }

    public String getVersion() {
        return version;
    }

    public void setSpecies(Map<Integer,String> species) {
        this.species = species;
    }

    public Map<Integer,String> getSpecies(){
        return species;
    }

    public void setExpressionLevels(List<String> expressionLevels) {
        this.expressionLevels = expressionLevels;
    }

    public List<String> getExpressionLevels() {
        return expressionLevels;
    }
}