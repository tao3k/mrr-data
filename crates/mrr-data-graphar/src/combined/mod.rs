//! Complete native capture of the authenticated combined content closure.
mod api;
mod capture;
pub use api::{
    CapturedCombinedGraphAr, CapturedGraphArRelation, CombinedGraphArLimits, CombinedGraphArParts,
};
pub use capture::capture_combined_graphar;
#[cfg(feature = "backend")]
mod backend;
#[cfg(feature = "backend")]
pub use backend::{CombinedGraphArRequest, prepare_combined_graphar};

#[cfg(feature = "backend")]
mod content;
#[cfg(feature = "backend")]
pub use content::{
    CombinedGraphArContentRequest, prepare_combined_graph_content,
    prepare_combined_graphar_from_content,
};

#[cfg(feature = "backend")]
mod restore;
#[cfg(feature = "backend")]
pub use restore::{CombinedGraphArRestoreRequest, restore_combined_graph_content};
