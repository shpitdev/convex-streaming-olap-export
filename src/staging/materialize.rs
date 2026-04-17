#[derive(Debug, Default)]
pub struct StagingMaterializer;

impl StagingMaterializer {
    pub fn milestone_note() -> &'static str {
        "Staging materialization belongs to milestone 02."
    }
}
