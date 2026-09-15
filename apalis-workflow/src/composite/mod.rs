// use std::str::FromStr;

// use apalis_core::{
//     backend::{Backend, BackendConfig, WireFormatBackend},
//     error::BoxDynError,
// };

// use crate::{
//     GraphFlow, SteppedFlow, SteppedService,
//     sequential::{Stack, Step, router::WorkflowRouter},
// };

// impl<Cur, B> Step<Cur, B> for GraphFlow<B>
// where
//     B: Backend + WireFormatBackend + BackendConfig,
//     B::Compact: Send + Sync + 'static,
// {
//     type Response = Res;

//     type Error = B::Error;

//     fn register(&mut self, router: &mut WorkflowRouter<B>) -> Result<(), BoxDynError> {
//         let svc = self.build()?;
//         let end_nodes = svc.end_nodes;
//         let svc = SteppedService::<B::Compact>::new(svc);
//         let count = router.steps.len();
//         router.steps.insert(count, svc);
//         Ok(())
//     }
// }
