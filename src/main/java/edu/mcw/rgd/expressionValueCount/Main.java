package edu.mcw.rgd.expressionValueCount;

import edu.mcw.rgd.datamodel.pheno.GeneExpressionValueCount;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.text.SimpleDateFormat;
import java.util.*;

public class Main {
    private String version;
    private Map<Integer,String> species;
    private List<String> expressionLevels;
    protected Logger logger = LogManager.getLogger("status");
    private final DAO dao = new DAO();

    /** rows buffered before they are written out, so memory does not grow with the species */
    private static final int FLUSH_THRESHOLD = 20000;

    /** the slim the counts are grouped by; must match what the report pages query */
    private static final String ONT_ID = "UBERON";
    private static final String SLIM_SOURCE = "AGR";


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
        logger.info("\t\tRunning for species "+species.get(speciesTypeKey)+"...");

        int totalNew = 0;
        int totalUpdated = 0;

        for (String level : expressionLevels) {

            // One aggregate for the whole species, rather than a count query per gene per slim
            // term. The old shape issued roughly 29 million round trips for a full run, which is
            // what made it take hours and what exhausted the connection pool; the retry loop that
            // used to sit here was treating that symptom and is no longer needed.
            long queryStart = System.currentTimeMillis();
            Map<String,Integer> computed = dao.getComputedCounts(speciesTypeKey, ONT_ID, SLIM_SOURCE, "TPM", level);
            logger.info("\t\t"+level+": counted "+computed.size()+" gene/term pairs in "
                    +Utils.formatElapsedTime(queryStart, System.currentTimeMillis()));

            Map<String,Integer> stored = dao.getStoredCounts(speciesTypeKey, "TPM", level);
            logger.info("\t\t"+level+": "+stored.size()+" rows already stored");

            List<GeneExpressionValueCount> newValueCounts = new ArrayList<>();
            List<GeneExpressionValueCount> updateValueCounts = new ArrayList<>();
            List<GeneExpressionValueCount> updateLastModified = new ArrayList<>();

            for (Map.Entry<String,Integer> entry : computed.entrySet()) {
                String key = entry.getKey();
                int cnt = entry.getValue();
                int sep = key.indexOf('|');

                GeneExpressionValueCount gvc = new GeneExpressionValueCount();
                gvc.setValueCnt(cnt);
                gvc.setExpressedRgdId(Integer.parseInt(key.substring(0, sep)));
                gvc.setTermAcc(key.substring(sep+1));
                gvc.setUnit("TPM");
                gvc.setLevel(level);

                // a pair the aggregate did not return has no values at all, and is skipped here just
                // as the old code skipped a count of zero
                Integer storedCnt = stored.get(key);
                if (storedCnt == null) {
                    newValueCounts.add(gvc);
                } else if (storedCnt != cnt) {
                    updateValueCounts.add(gvc);
                } else {
                    updateLastModified.add(gvc);
                }

                // all three lists count towards the flush: on a re-run almost every count is
                // unchanged, so leaving out updateLastModified would mean the flush never fired
                int total = newValueCounts.size()+updateValueCounts.size()+updateLastModified.size();
                if (total > FLUSH_THRESHOLD) {
                    totalNew = totalNew+newValueCounts.size();
                    totalUpdated = totalUpdated+updateLastModified.size()+updateValueCounts.size();
                    insertValues(newValueCounts, updateValueCounts, updateLastModified);
                }
            }
            totalNew = totalNew+newValueCounts.size();
            totalUpdated = totalUpdated+updateLastModified.size()+updateValueCounts.size();
            insertValues(newValueCounts, updateValueCounts, updateLastModified);
        }

        logger.info("\tTotal new values: " + totalNew);
        logger.info("\tTotal updated: " + totalUpdated);
        logger.info("\tExpression Value Count pipeline for species "+species.get(speciesTypeKey)+" runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));
    }

    void insertValues(List<GeneExpressionValueCount> newValueCounts, List<GeneExpressionValueCount> updateValueCounts, List<GeneExpressionValueCount> updateLastModified) throws Exception {
        if (!newValueCounts.isEmpty()){
            logger.debug("\t\tNew Counts for Expression Values: "+newValueCounts.size());
            dao.insertGeneExprRecValCnt(newValueCounts);
            newValueCounts.clear();
        }
        if (!updateValueCounts.isEmpty()){
            logger.debug("\t\tValues being updated: "+updateValueCounts.size());
            dao.updateGeneExprRecValCnts(updateValueCounts);
            updateValueCounts.clear();
        }
        if (!updateLastModified.isEmpty()){
            logger.debug("\t\tCounts not changed: "+updateLastModified.size());
            dao.updateLastModified(updateLastModified);
            updateLastModified.clear();
        }
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