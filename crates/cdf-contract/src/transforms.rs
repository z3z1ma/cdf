use crate::policy::TransformDescription;

pub(crate) trait TransformColumn {
    fn column_name(&self) -> Option<&str>;
}

impl TransformColumn for TransformDescription {
    fn column_name(&self) -> Option<&str> {
        match self {
            Self::Rename { from, .. } => Some(from.as_str()),
            Self::Cast { column, .. }
            | Self::Redact { column, .. }
            | Self::Derive { column, .. }
            | Self::ExpandNested { column, .. } => Some(column.as_str()),
            Self::Filter { .. } => None,
        }
    }
}
