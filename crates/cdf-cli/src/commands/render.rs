use super::{HelpReport, VersionReport};
use crate::render::RenderDocument;

pub(super) fn help_document(report: &HelpReport) -> RenderDocument {
    RenderDocument::text(report.help.clone())
}

pub(super) fn version_document(report: &VersionReport) -> RenderDocument {
    RenderDocument::text(format!("cdf {}", report.version))
}
