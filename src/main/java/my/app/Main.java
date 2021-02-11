package my.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import ratpack.exec.Promise;
import ratpack.exec.util.ParallelBatch;
import ratpack.handling.Context;
import ratpack.handling.Handler;
import ratpack.http.client.HttpClient;
import ratpack.jackson.Jackson;
import ratpack.server.RatpackServer;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {
    public static void main(String... args) throws Exception {
        RatpackServer.start(server -> server
                .serverConfig(config -> config
                        .port(Integer.parseInt(System.getProperty("port")))
                        .threads(2)
                )
                .handlers(chain -> chain
                        .get("slow/:count", ctx -> slowReply(getCount(ctx))
                                .map(Jackson::json)
                                .then(ctx::render))
                        .get("parallelSlow/:count", ctx -> parallelSlowness(getCount(ctx))
                                .map(Jackson::json)
                                .then(ctx::render))
                        .get("remoteSlow/:count", ctx -> remoteSlowness(ctx, getCount(ctx))
                                .map(Jackson::json)
                                .then(ctx::render))
                        .get("remoteParallelSlow/:count", ctx -> remoteParallelSlowness(ctx, getCount(ctx))
                                .map(Jackson::json)
                                .then(ctx::render))
                )
        );
    }

    static Integer getCount(Context ctx) {
        return getPathInt(ctx, "count");
    }

    private static Integer getPathInt(Context ctx, String name) {
        try {
            return Integer.parseInt(ctx.getPathTokens().get(name));
        } catch (NumberFormatException e) {
            return 1;
        }
    }

    static Promise<Integer> slowReply(Integer i) {
        return Promise.value(i)
                .next(ignore -> TimeUnit.SECONDS.sleep(2));
    }

    static Promise<List<Integer>> parallelSlowness(Integer i) {
        List<Promise<Integer>> promises = IntStream.range(0, i)
                .boxed()
                .map(Main::slowReply)
                .collect(Collectors.toList());

        return ParallelBatch.of(promises).yield();
    }

    static Promise<List<Integer>> remoteParallelSlowness(Context ctx, Integer i) {
        List<Promise<Integer>> promises = IntStream.range(0, i)
                .boxed()
                .map(x-> remoteSlowness(ctx, x))
                .collect(Collectors.toList());

        return ParallelBatch.of(promises).yield();
    }

    static Promise<Integer> remoteSlowness(Context ctx, Integer i) {
        HttpClient client = ctx.get(HttpClient.class);
        ObjectMapper mapper = ctx.get(ObjectMapper.class);

        return client.get(URI.create("http://localhost:5051/slow/" + i))
                .map(receivedResponse -> mapper.readValue(receivedResponse.getBody().getBytes(), Integer.class));
    }
}