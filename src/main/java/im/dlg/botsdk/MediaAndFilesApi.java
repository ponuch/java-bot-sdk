package im.dlg.botsdk;

import dialog.MediaAndFilesGrpc;
import dialog.MediaAndFilesOuterClass.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MediaAndFilesApi {

    private static final Logger log = LoggerFactory.getLogger(MessagingApi.class);

    private static InternalBotApi privateBot;

    MediaAndFilesApi(InternalBotApi privateBot) {
        this.privateBot = privateBot;
    }

    /**
     * Uploads a file to server
     *
     * @param file     - file to upload
     * @param mimeType - file mime type
     * @return A CompletableFuture for FileLocation on server side (FileLocation for
     * Dialog internal api)
     */
    public static CompletableFuture<FileLocation> upLoadFile(@Nonnull File file, @Nonnull String mimeType) {

        if (!file.isFile()) {
            throw new IllegalArgumentException("Input isn't a file !");
        }

        RequestGetFileUploadUrl.Builder requestUploadURL = RequestGetFileUploadUrl.newBuilder()
                .setExpectedSize((int) file.length());

        // TODO check multi part Upload if needed
        return privateBot
                .withToken(MediaAndFilesGrpc.newFutureStub(privateBot.channel.getChannel()).withDeadlineAfter(2,
                        TimeUnit.MINUTES), stub -> stub.getFileUploadUrl(requestUploadURL.build()))
                .thenComposeAsync(uplkeyresp -> {
                    RequestGetFileUploadPartUrl requestUploadPartURL = RequestGetFileUploadPartUrl.newBuilder()
                            .setPartSize((int) file.length()).setPartNumber(0).setUploadKey(uplkeyresp.getUploadKey())
                            .build();
                    return privateBot
                            .withToken(
                                    MediaAndFilesGrpc.newFutureStub(privateBot.channel.getChannel())
                                            .withDeadlineAfter(2, TimeUnit.MINUTES),
                                    stub -> stub.getFileUploadPartUrl(requestUploadPartURL))
                            .thenComposeAsync(resp -> {
                                httpPutFile(file, resp.getUrl(), mimeType);
                                RequestCommitFileUpload commit = RequestCommitFileUpload.newBuilder()
                                        .setFileName(file.getName()).setUploadKey(uplkeyresp.getUploadKey()).build();
                                return privateBot.withToken(
                                        MediaAndFilesGrpc.newFutureStub(privateBot.channel.getChannel())
                                                .withDeadlineAfter(2, TimeUnit.MINUTES),
                                        stub -> stub.commitFileUpload(commit));
                            }, privateBot.executor.getExecutor());
                }, privateBot.executor.getExecutor()).thenApplyAsync(respCommit -> {
                    return respCommit.getUploadedFileLocation();
                }, privateBot.executor.getExecutor());
    }

    /**
     * Get Url to download a file from server
     *
     * @param fileLoc - file location (dialog internal API)
     * @return - A CompletableFuture for a String containing download url
     * @throws InterruptedException in concurrency issues
     * @throws ExecutionException   in concurrency issues
     */
    public CompletableFuture<String> getFileUrl(FileLocation fileLoc) throws InterruptedException, ExecutionException {
        return privateBot
                .withToken(
                        MediaAndFilesGrpc.newFutureStub(privateBot.channel.getChannel()).withDeadlineAfter(2,
                                TimeUnit.MINUTES),
                        stub -> stub.getFileUrls(RequestGetFileUrls.newBuilder().addFiles(fileLoc).build()))
                .thenApply(resp -> resp.getFileUrlsList().get(0).getUrl());
    }

    /**
     * Helper function to perform HTPP PUT request from a file to server
     *
     * @param file      - file to upload
     * @param upLoadUrl - url to execute PUT request on server
     * @param mimeType  - mime type of the file
     */
    private static void httpPutFile(File file, String upLoadUrl, String mimeType) {

        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpPut request = new HttpPut(upLoadUrl);
        FileEntity entity;
        if (mimeType.contains("text")) {
            entity = new FileEntity(file, ContentType.create(mimeType, "UTF-8"));
        } else {
            entity = new FileEntity(file, ContentType.create(mimeType));
        }
        request.setEntity(entity);
        log.debug("Executing request ->" + request.getRequestLine());
        CloseableHttpResponse response = null;
        try {
            response = httpclient.execute(request);
            log.debug("Response ->" + response.getStatusLine());
        } catch (IOException e) {
            log.error("Failed to send file", e);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                log.error("Failed to close response", e);
            }
        }

    }

}