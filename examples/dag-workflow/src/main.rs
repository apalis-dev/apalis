use apalis::prelude::*;
use apalis_file_storage::JsonStorage;
use apalis_workflow::{DagFlow, WorkflowSink};
use tracing::info;

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
    unsafe {
        std::env::set_var("RUST_LOG", "debug");
    };
    tracing_subscriber::fmt::init();
    let mut backend = JsonStorage::new_temp().unwrap();
    backend.push_start(vec![42, 43, 44]).await.unwrap();

    let dag_flow = DagFlow::new("user-info-workflow");
    let get_name = dag_flow.node(get_name);
    let get_age = dag_flow.node(get_age);
    let get_address = dag_flow.node(get_address);
    dag_flow
        .node(collector)
        .depends_on((&get_name, &get_age, &get_address)); // Order and types matters here

    dag_flow.validate()?; // Ensure DAG is valid

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
    //
    // You can visualize this using tools like Graphviz
    // https://dreampuf.github.io/GraphvizOnline/
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
