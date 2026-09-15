use apalis::prelude::*;
use apalis_workflow::{GraphFlow, WorkflowSink, in_memory::InMemoryWorkflow};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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
    wrk: WorkerContext, // Nodes are still apalis services and can inject deps
) -> Result<usize, BoxDynError> {
    let result = name.parse::<usize>()? + age + address;
    wrk.stop().unwrap();
    Ok(result)
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    use tracing_subscriber::EnvFilter;

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
    let filter_layer =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("debug"))?;
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let mut backend = InMemoryWorkflow::create();

    backend.start_fan_out(vec![42, 43, 44]).await.unwrap();

    let graph = GraphFlow::new("user-info-workflow");
    let get_name = graph.add_task(get_name);
    let get_age = graph.add_task(get_age);
    let get_address = graph.add_task(get_address);
    graph
        .add_task(collector)
        .depends_on((&get_name, &get_age, &get_address)); // Order and types matters here

    // This should print something like:
    // digraph {
    //     0 [ label="dag::get_name"]
    //     1 [ label="dag::get_age"]
    //     2 [ label="dag::get_address"]
    //     3 [ label="dag::collector"]
    //     0 -> 3 [ ]
    //     1 -> 3 [ ]
    //     2 -> 3 [ ]
    // }

    // You can visualize this using tools like Graphviz
    // https://dreampuf.github.io/GraphvizOnline/
    info!("Executing workflow:\n{}", graph); // Print the DAG structure in dot format

    WorkerBuilder::new("tasty-banana")
        .backend(backend)
        .parallelize(tokio::spawn)
        .enable_tracing()
        .on_event(|_c, e| info!("{e}"))
        .build(graph)
        .run()
        .await?;
    Ok(())
}
