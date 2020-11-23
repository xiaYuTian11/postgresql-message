package com.sunnyday.msg;

import cn.hutool.core.thread.ThreadUtil;
import com.google.common.util.concurrent.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 * @author TMW
 * @date 2020/8/28 11:57
 */
public class FuturecallbackTest {

    @Test
    public void test() throws ExecutionException, InterruptedException {
        final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
        final ListenableFuture<String> submit = executorService.submit(new Callable<String>() {

            @Override
            public String call() throws Exception {

                System.out.println(1/0);
                return "hello world";
            }
        });
        ThreadUtil.sleep(1000*10);
        final FutureCallback<String> futureCallback = new FutureCallback<String>() {

            @Override
            public void onSuccess(@Nullable String result) {
                System.out.println("FutureCallback onSuccess:" + result);
            }

            @Override
            public void onFailure(Throwable t) {
                System.out.println("-------------/");
                t.printStackTrace();
            }
        };
        Futures.addCallback(submit, futureCallback, executorService);

        final String s = submit.get();
        System.out.println(s);
    }
}
