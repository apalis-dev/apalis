# apalis-workflow

This crate provides a flexible and composable workflow engine for [apalis](https://github.com/apalis-dev/apalis). Can be used for building general workflows or advanced LLM workflows.

## Overview

The workflow engine allows you to define a sequence or DAG chain of steps in a workflow.
Workflows are built by composing steps/nodes, and can be executed using supported backends

## Features

- Extensible, durable and resumable workflows.
- Workflows are processed in a distributed manner.
- Parallel and concurrent execution of single steps.
- Full integration with `apalis` backends, workers and middleware.
- Macro free with compile-time guarantees.

## Example

Currently `apalis-workflow` supports sequential and directed acyclic graph based workflows

### Sequential Workflow

```rust,ignore
use apalis::prelude::*;
use apalis_workflow::*;
use apalis_file_storage::JsonStorage;;

#[tokio::main]
async fn main() {
   let workflow = Workflow::new("odd-numbers-workflow")
       .and_then(|a: usize| async move { Ok::<_, BoxDynError>((0..a).collect::<Vec<_>>()) })
       .filter_map(|x| async move { if x % 2 != 0 { Some(x) } else { None } })
       .and_then(|a: Vec<usize>| async move {
           println!("Sum: {}", a.iter().sum::<usize>());
           Ok::<_, BoxDynError>(())
        });

   let mut in_memory = JsonStorage::new_temp().unwrap();

   in_memory.push_start(10).await.unwrap();

   let worker = WorkerBuilder::new("rango-tango")
       .backend(in_memory)
       .on_event(|ctx, ev| {
           println!("On Event = {:?}", ev);
       })
       .build(workflow);
   worker.run().await.unwrap();
}
```

### Directed Acyclic Graph

```rust,ignore
use apalis::prelude::*;
use apalis_file_storage::JsonStorage;
use apalis_workflow::{DagFlow, WorkflowSink};
use serde_json::Value;

async fn get_name(user_id: u32) -> Result<String, BoxDynError> {
    Ok(user_id.to_string())
}

async fn get_age(user_id: u32) -> Result<usize, BoxDynError> {
    Ok(user_id as usize + 20)
}

async fn get_address(user_id: u32) -> Result<usize, BoxDynError> {
    Ok(user_id as usize + 100)
}

async fn collector(
    (name, age, address): (String, usize, usize),
    wrk: WorkerContext,
) -> Result<usize, BoxDynError> {
    let result = name.parse::<usize>()? + age + address;
    wrk.stop().unwrap();
    Ok(result)
}


#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    let mut backend = JsonStorage::new_temp().unwrap();

    backend
        .push_start(vec![42, 43, 44])
        .await
        .unwrap();

    let dag_flow = DagFlow::new("user-etl-workflow");
    let get_name = dag_flow.node(get_name);
    let get_age = dag_flow.node(get_age);
    let get_address = dag_flow.node(get_address);
    dag_flow
        .node(collector)
        .depends_on((&get_name, &get_age, &get_address)); // Order and types matters here

    dag_flow.validate()?; // Ensure DAG is valid

    info!("Executing workflow:\n{}", dag_flow); // Print the DAG structure in dot format

    WorkerBuilder::new("tasty-banana")
        .backend(backend)
        .enable_tracing()
        .on_event(|_c, e| info!("{e}"))
        .build(dag_flow)
        .run()
        .await?;
    Ok(())
}

```

## Observability

You can track your workflows using [apalis-board](https://github.com/apalis-dev/apalis-board).
![Task](https://github.com/apalis-dev/apalis-board/raw/main/screenshots/task.png)

## Backend Support

- [x] [JSONStorage](https://docs.rs/apalis-core/1.0.0-rc.3/apalis_core/backend/json/struct.JsonStorage.html)
- [x] [SqliteStorage](https://docs.rs/apalis-sqlite#workflow-example)
- [x] [RedisStorage](https://docs.rs/apalis-redis#workflow-example)
- [x] [PostgresStorage](https://docs.rs/apalis-postgres#workflow-example)
- [x] [MysqlStorage](https://docs.rs/apalis-mysql#workflow-example)
- [ ] RsMq

## Roadmap

- [x] AndThen: Sequential execution on success
- [x] Delay: Delay execution
- [x] FilterMap: MapReduce
- [x] Fold
- [-] Repeater
- [-] Subflow
- [x] DAG

## Inspirations:

- [Underway](https://github.com/maxcountryman/underway): Postgres-only `stepped` solution
- [dagx](https://github.com/swaits/dagx): blazing fast _in-memory_ `dag` solution

## License

Licensed under MIT or Apache-2.0.
