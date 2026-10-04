//! Catalog-declared string vertex properties; MRR retains semantic authority.
mod projection;
pub(crate) mod read;
mod write;

pub use projection::{GraphArEntityPropertyProjection, GraphArEntityPropertyTable};
pub use read::{CapturedGraphArEntityProperties, capture_graphar_entity_properties};
pub use write::write_graphar_entity_properties;

mod api;
pub use api::{
    GraphArEntityPropertyError, GraphArEntityPropertyLimits, GraphArEntityPropertyReceipt,
};

#[cfg(feature = "backend")]
mod backend;
#[cfg(feature = "backend")]
pub use backend::{GraphArEntityPropertiesRequest, prepare_graphar_entity_properties};

mod descriptor;
pub use descriptor::GraphArEntityPropertyBlock;

mod registered;
pub use registered::{
    RegisteredGraphArEntityProperties, capture_registered_graphar_entity_properties,
};

#[cfg(feature = "backend")]
mod registered_backend;
#[cfg(feature = "backend")]
pub use registered_backend::{
    RegisteredGraphArEntityPropertiesRequest, prepare_registered_graphar_entity_properties,
};
