use std::process::ExitCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalStatus {
    Success,
    PartialFailure,
}

impl FinalStatus {
    pub fn exit_code(self) -> ExitCode {
        match self {
            Self::Success => ExitCode::from(0),
            Self::PartialFailure => ExitCode::from(2),
        }
    }
}
