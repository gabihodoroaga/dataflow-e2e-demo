# dataflow-e2e-demo

This is a demo and a seed project to show how you can end-to-end test a Dataflow pipeline.
You can find more about this by following this blog post [Dataflow - e2e test for you pipeline](http://hodo.dev/posts/post-31-gcp-dataflow-e2e-tests/)

## How to use

Build

```bash
gradle build
```

Run unit tests 
```bash
gradle test
```

Run local

```bash
./run_local.sh
```

Run with Dataflow

```bash
./run_dataflow.sh
```

Run the e2e tests

```bash
./run_e2e.sh
```

## Author 

Gabriel Hodoroaga [hodo.dev](https://hodo.dev)
