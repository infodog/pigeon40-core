package net.xinshi.pigeon50.client.distributedclient.fileclient;

import com.aliyun.oss.OSSClient;
import net.xinshi.pigeon.filesystem.IFileSystem;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

public class AliyunOssClient implements IFileSystem {

    String apiEndPoint;
    String internalUrlPrefix;
    String externalUrlPrefix;
    String accessKeyId;
    String accessKeySecret;
    String bucketName;
    String includeList;

    String[] includeFileExts;


    int[] minDates = new int[3];
    int minDatesLen;

    int[] maxDates = new int[3];
    int maxDatesLen;

    String minDatesToCloud;    //大于这个prefix才放到云中
    String maxDatesToCloud;    //小于这个prefix的才放到云中

    IFileSystem localFileSystem;

    OSSClient ossClient;

    public String getMinDatesToCloud() {
        return minDatesToCloud;
    }

    public void setMinDatesToCloud(String minDatesToCloud) {
        this.minDatesToCloud = minDatesToCloud;
        minDatesLen = 0;
        if(minDatesToCloud!=null){
            String[] parts = minDatesToCloud.split("/");
            for(int i=1 ; i<parts.length; i++){
                String part = parts[i];
                if(StringUtils.isNumeric(part)){
                    minDates[i-1] = Integer.parseInt(part);
                    minDatesLen = i;
                }
                if(i == 3){
                    break;
                }
            }
        }
    }

    public String getMaxDatesToCloud() {
        return maxDatesToCloud;
    }

    public void setMaxDatesToCloud(String maxDatesToCloud) {
        this.maxDatesToCloud = maxDatesToCloud;
        maxDatesLen = 0;
        if(maxDatesToCloud!=null){
            String[] parts = maxDatesToCloud.split("/");
            for(int i=1 ; i<parts.length; i++){
                String part = parts[i];
                if(StringUtils.isNumeric(part)){
                    maxDates[i-1] = Integer.parseInt(part);
                    maxDatesLen = i;
                }
                if(i == 3){
                    break;
                }

            }
        }
    }

    public String getIncludeList() {
        return includeList;
    }

    public void setIncludeList(String includeList) {
        this.includeList = includeList;
        if(StringUtils.isBlank(includeList)){
            this.includeFileExts = null;
        }
        else {
            this.includeFileExts = includeList.split(",");
        }
    }

    boolean isIncluded(String filePath){
        if(this.includeFileExts==null){
            return true;
        }
        String ext = getExtension(filePath);
        if (ext == null) {
            return false;
        } else {
            for (String inc : includeFileExts) {
                if (inc.equalsIgnoreCase(ext)) {
                    return true;
                }
            }
        }
        return false;
    }

    boolean isOnAliYun(String fileId){
        //判断一个图片是否已经在阿里云上
        int pos = fileId.indexOf("@");
        if(pos==-1) {
            return true;
        }
        if(!isIncluded(fileId)){
            return false;
        }
        String path = fileId.substring(pos + 1);
        String[] parts = path.split("/");
        if(parts.length<4){
            return false;
        }

        for(int i=0; i<minDatesLen; i++){
            int iPart = Integer.parseInt(parts[i+1]);
            if(iPart<minDates[i]){
                return false;
            }
        }

        for(int i=0; i<maxDatesLen; i++){
            int iPart = Integer.parseInt(parts[i+1]);
            if(iPart>maxDates[i]){
                return false;
            }
        }
        return true;
    }

    public IFileSystem getLocalFileSystem() {
        return localFileSystem;
    }

    public void setLocalFileSystem(IFileSystem localFileSystem) {
        this.localFileSystem = localFileSystem;
    }

    public String getApiEndPoint() {
        return apiEndPoint;
    }

    public void setApiEndPoint(String apiEndPoint) {
        this.apiEndPoint = apiEndPoint;
    }

    public String getInternalUrlPrefix() {
        return internalUrlPrefix;
    }

    public void setInternalUrlPrefix(String internalUrlPrefix) {
        if(internalUrlPrefix.endsWith("/")) {
            this.internalUrlPrefix = internalUrlPrefix;
        }
        else{
            this.internalUrlPrefix = "/" + internalUrlPrefix;
        }
    }

    public String getExternalUrlPrefix() {
        return externalUrlPrefix;
    }

    public void setExternalUrlPrefix(String externalUrlPrefix) {
        if(externalUrlPrefix.endsWith("/")) {
            this.externalUrlPrefix = externalUrlPrefix;
        }
        else {
            this.externalUrlPrefix = "/" + externalUrlPrefix;
        }
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public void setAccessKeySecret(String accessKeySecret) {
        this.accessKeySecret = accessKeySecret;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }


    static public String getAliYunObjectId(String fileId){
        //首先看看是否带有@,如果有，表明这是一个旧版本的文件id
        int pos = fileId.indexOf("@");
        if(pos>-1){
            String path = fileId.substring(pos+1);
            if(path.startsWith("/")){
                path = path.substring(1);
            }
            return "img/" + path;
        }
        else{
            return "pn/" + fileId;
        }
    }


    static String getFilePath() throws Exception {
        Calendar calendar = Calendar.getInstance();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        String fileId = "/" + year + "/" + month + "/" + day;
        String ranId = UUID.randomUUID().toString();
        String id = fileId + "/" + ranId;
        return id;
    }

    static String getExtension(File f) {
        String fullPath = f.getName();
        int rpos = fullPath.lastIndexOf(".");
        if (rpos < 0) {
            return "";
        } else {
            return fullPath.substring(rpos);
        }

    }

    static String getExtension(String fullPath) {
        int rpos = fullPath.lastIndexOf(".");
        if (rpos < 0) {
            return "";
        } else {
            return fullPath.substring(rpos);
        }
    }

    @Override
    public void delete(String fileid) throws Exception {
        String aliyunId = getAliYunObjectId(fileid);
        ossClient.deleteObject(bucketName,aliyunId);
    }

    @Override
    public String getUrl(String fileid) throws Exception {
        if(isOnAliYun(fileid)) {
            String aliyunId = getAliYunObjectId(fileid);
            return externalUrlPrefix + aliyunId;
        }
        else{
            return localFileSystem.getUrl(fileid);
        }
    }

    @Override
    public String getInternalUrl(String fileid) throws Exception {
        if(isOnAliYun(fileid)) {
            String aliyunId = getAliYunObjectId(fileid);
            return internalUrlPrefix + aliyunId;
        }
        else{
            return localFileSystem.getInternalUrl(fileid);
        }
    }

    @Override
    public OutputStream openOutputSystem(String fileId) throws Exception {
        return null;
    }

    @Override
    public String checkExists(File f) throws Exception, IOException {
        return null;
    }

    @Override
    public String addFile(File f, String name) throws Exception {
        if(isIncluded(f.getCanonicalPath())) {
            String filePath = getFilePath();
            String fileId = bucketName + filePath + getExtension(f);
            String aliyunId = getAliYunObjectId(fileId);
            ossClient.putObject(bucketName, aliyunId, f);
            return fileId;
        }
        else{
            return localFileSystem.addFile(f,name);
        }
    }

    @Override
    public String addBytes(byte[] bytes, String name) throws Exception {
        if(isIncluded(name)) {
            String filePath = getFilePath();
            String fileId = bucketName + filePath + getExtension(name);
            String aliyunId = getAliYunObjectId(fileId);
            System.out.println("addbytes to aliyun " + aliyunId);;
            ossClient.putObject(bucketName, aliyunId, new ByteArrayInputStream(bytes));
            return fileId;
        }
        else{
            return localFileSystem.addBytes(bytes,name);
        }
    }

    @Override
    public void init() {
        this.ossClient = new OSSClient(apiEndPoint, accessKeyId, accessKeySecret);

    }

    @Override
    public String getRelatedUrl(String fileId, String spec) {
        try {
            if (isOnAliYun(fileId)) {
                String externalUrl = getUrl(fileId);
                String lower = spec.toLowerCase();
                String[] w_h = lower.split("x");
                String w = w_h[0];
                String h = w_h[1];
                return externalUrl + "?x-oss-process=image/resize,w_" + w + ",h_" + h;
            }
            else{
                return localFileSystem.getRelatedUrl(fileId,spec);
            }

        }
        catch(Exception e){
            System.out.printf("fileId=" + fileId);
            e.printStackTrace();
            return "error:" + e.getMessage();
        }

    }

    @Override
    public void genRelatedFile(String fileId, String spec) throws Exception {

    }

    @Override
    public List<String> getUrls(String fileId) throws Exception {
        return null;
    }
}
