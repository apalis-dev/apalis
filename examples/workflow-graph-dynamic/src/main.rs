use apalis::prelude::*;

use apalis_workflow::{GraphFlow, WorkflowSink, in_memory::InMemoryWorkflow};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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

    let graph = GraphFlow::new("fan-out");
    let mut backend = InMemoryWorkflow::create();

    backend.push_start(()).await.unwrap();
    let source = graph.add_node("source", task_fn(|_: ()| async move { 1 }));

    let mut transformers = vec![];
    for i in 0..1000 {
        // Add 1000 nodes
        let node = graph
            .add_node(
                &format!("transform-{i}"),
                task_fn(|x: i32| async move { x + 1 }),
            )
            .depends_on(&source);
        transformers.push(node);
    }
    // Collect 1000 nodes results
    graph
        .add_node(
            "collector",
            task_fn(|res: Vec<i32>, w: WorkerContext| async move {
                w.stop().unwrap();
                let res = res.iter().sum::<i32>();

                println!("Res {res}");
            }),
        )
        .depends_on(transformers);

    graph.validate()?; // Ensure DAG is valid

    // info!("Executing workflow:\n{}", graph);

    WorkerBuilder::new("workflow-bench")
        .backend(backend)
        .enable_tracing()
        .build(graph)
        .run()
        .await?;
    Ok(())
}
