/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.mcl.Sedna.Configuration;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

/**
 *
 * @author daidong
 */
public class Configuration {

    private Properties property;

    public Configuration(){
        this("conf/conf.properties");
    }
    public Configuration(String filePathWithComma){
        property = new Properties();
        try{
            String[] files = filePathWithComma.split(",");
            for (String file:files){
                FileInputStream fis = new FileInputStream(file);
                property.load(fis);
                fis.close();
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
    public String getValue(String key){
        if (property.containsKey(key)){
            return property.getProperty(key);
        } else
            return "";
    }
    public String getValue(String filePath, String key){
        String value = "";
        try{
            FileInputStream fis = new FileInputStream(filePath);
            property.load(fis);
            fis.close();
            if (property.containsKey(key)){
                value = property.getProperty(key);
            }
        } catch (Exception ex){
            ex.printStackTrace();
        }
        return value;

    }
    public void clear(){

    }
    public void setValue(String key, String value){

    }
    public void setValue(String filePath, String key, String value){

    }
    public void saveFile(String fileName, String description){
        try{
            FileOutputStream fos = new FileOutputStream(fileName);
            property.store(fos, description);
            fos.close();
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
